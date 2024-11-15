#include "stats_fetcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/netlink/socket.h>

#include <util/datetime/cputimer.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/file.h>

#include <linux/genetlink.h>
#include <linux/taskstats.h>
#include <linux/cgroupstats.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/netlink.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcher final
    : public IStatsFetcher
{
private:
    const TString ComponentName;

    const ILoggingServicePtr Logging;
    const TString StatsFile;

    TLog Log;

    TFile CpuAcctWait;

    TDuration Last;

public:
    TCgroupStatsFetcher(
            TString componentName,
            ILoggingServicePtr logging,
            TString statsFile)
        : ComponentName(std::move(componentName))
        , Logging(std::move(logging))
        , StatsFile(std::move(statsFile))
    {
    }

    void Start() override
    {
        Log = Logging->CreateLog(ComponentName);

        try {
            CpuAcctWait = TFile(
                StatsFile,
                EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
        } catch (...) {
            STORAGE_ERROR(BuildErrorMessageFromException());
            return;
        }

        if (!CpuAcctWait.IsOpen()) {
            STORAGE_ERROR("Failed to open " << StatsFile);
            return;
        }

        if (auto [cpuWait, error] = GetCpuWait(); HasError(error)) {
            STORAGE_ERROR("Failed to get CpuWait stats: " << error);
        } else {
            Last = cpuWait;
        }
    }

    void Stop() override
    {
    }

    TResultOrError<TDuration> GetCpuWait() override
    {
        if (!CpuAcctWait.IsOpen()) {
            return MakeError(E_INVALID_STATE, "Failed to open " + StatsFile);
        }

        try {
            CpuAcctWait.Seek(0, SeekDir::sSet);

            constexpr i64 bufSize = 1024;

            if (CpuAcctWait.GetLength() >= bufSize - 1) {
                CpuAcctWait.Close();
                return MakeError(E_INVALID_STATE, StatsFile + " is too large");
            }

            char buf[bufSize];

            auto cnt = CpuAcctWait.Read(buf, bufSize - 1);
            if (buf[cnt - 1] == '\n') {
                --cnt;
            }
            buf[cnt] = '\0';
            auto value = TDuration::MicroSeconds(FromString<ui64>(buf) / 1000);

            if (value < Last) {
                auto errorMessage = ReportCpuWaitCounterReadError(
                    TStringBuilder() << StatsFile << " : new value " << value
                                     << " is less than previous " << Last);
                Last = value;
                return MakeError(E_INVALID_STATE, std::move(errorMessage));
            }
            auto retval = value - Last;
            Last = value;

            return retval;
        } catch (...) {
            auto errorMessage = BuildErrorMessageFromException();
            CpuAcctWait.Close();
            return MakeError(E_FAIL, std::move(errorMessage));
        }
    }

    TString BuildErrorMessageFromException()
    {
        auto msg = TStringBuilder() << "IO error for " << StatsFile;
        msg << " with exception " << CurrentExceptionMessage();
        return msg;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStatsFetcherStub final
    : public IStatsFetcher
{
    void Start() override
    {
    }

    void Stop() override
    {
    }

    TResultOrError<TDuration> GetCpuWait() override
    {
        return TDuration::Zero();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTaskStatsFetcher final: public IStatsFetcher
{
    using TNetlinkMessage = NCloud::NNetlink::TMessage;
    using TNetlinkSocket = NCloud::NNetlink::TSocket;
private:
    const TString ComponentName;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    int Pid;
    TLog Log;
    const TDuration NetlinkSocketTimeout = TDuration::Seconds(1);
    TDuration Last;

public:
    TTaskStatsFetcher(
            TString componentName,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            int pid)
        : ComponentName(std::move(componentName))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , Pid(pid)
    {
    }

    ~TTaskStatsFetcher() override {
        Stop();
    }

    void Start() override
    {
        Log = Logging->CreateLog(ComponentName);
    }

    void Stop() override
    {
    }

    TResultOrError<TDuration> GetCpuWait() override
    {
        try {
            TNetlinkSocket netlinkSocket(TASKSTATS_GENL_NAME);

            TNetlinkMessage message(
                netlinkSocket.GetFamily(),
                TASKSTATS_CMD_GET);
            message.Put(TASKSTATS_CMD_ATTR_PID, Pid);

            auto cpuDelay = NThreading::NewPromise<TResultOrError<TDuration>>();
            netlinkSocket.SetCallback(
                NL_CB_VALID,
                [this, &cpuDelay](nl_msg* nlmsg) {
                    auto delayNs = CpuDelayStatHandler(nlmsg);
                    if (delayNs < 0) {
                        cpuDelay.SetValue(MakeError(
                            E_INVALID_STATE,
                            "Failed to parse netlink message"));
                        return NL_STOP;
                    }
                    cpuDelay.SetValue(
                        TDuration::MilliSeconds(delayNs / 1000));
                    return NL_OK;
                });
            netlinkSocket.Send(message);
            auto cpuWait = cpuDelay.GetFuture().GetValue(NetlinkSocketTimeout);
            if (HasError(cpuWait.GetError())) {
                return cpuWait;
            }
            auto cpuLack = cpuWait.GetResult();
            auto retval = cpuLack - Last;
            Last = cpuLack;
            return retval;
        } catch (...) {
            auto errorMessage = BuildErrorMessageFromException();
            return MakeError(E_FAIL, errorMessage);
        }
    }

    TString BuildErrorMessageFromException()
    {
        auto msg = TStringBuilder() << "IO error";
        msg << " with exception " << CurrentExceptionMessage();
        return msg;
    }

    int CpuDelayStatHandler(nl_msg* nlmsg)
    {
        nlattr* nlattrs[TASKSTATS_TYPE_MAX + 1];
        nlattr* taskStatsAttrs[TASKSTATS_TYPE_MAX + 1];
        if (genlmsg_parse(
                nlmsg_hdr(nlmsg),
                0,
                nlattrs,
                TASKSTATS_TYPE_MAX,
                NULL) != 0)
        {
            return -1;
        }

        auto nlattr = nlattrs[TASKSTATS_TYPE_AGGR_PID];
        if (!nlattr) {
            return -1;
        }

        if (nla_parse_nested(
                taskStatsAttrs,
                TASKSTATS_TYPE_MAX,
                nlattr,
                nullptr) != 0)
        {
            return -1;
        }

        nlattr = taskStatsAttrs[TASKSTATS_TYPE_STATS];
        if (!nlattr) {
            return -1;
        }

        const auto* stats = static_cast<const taskstats*>(nla_data(nlattr));
        return stats->cpu_delay_total;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    TString statsFile)
{
    return std::make_shared<TCgroupStatsFetcher>(
        std::move(componentName),
        std::move(logging),
        std::move(statsFile));
}

IStatsFetcherPtr CreateTaskStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    int pid)
{
    return std::make_shared<TTaskStatsFetcher>(
        std::move(componentName),
        std::move(logging),
        std::move(monitoring),
        pid);
}

IStatsFetcherPtr CreateStatsFetcherStub()
{
    return std::make_shared<TStatsFetcherStub>();
}

TString BuildCpuWaitStatsFilename(const TString& serviceName)
{
    static constexpr auto CpuWaitStatsFilenameTemplate =
        "/sys/fs/cgroup/cpu/system.slice/%s.service/cpuacct.wait";
    if (!serviceName.empty()) {
        return Sprintf(CpuWaitStatsFilenameTemplate, serviceName.c_str());
    }
    return {};
}

NCloud::NStorage::IStatsFetcherPtr BuildStatsFetcher(
    NProto::EStatsFetcherType statsFetcherType,
    const TString& cpuWaitFilename,
    const TLog& log,
    ILoggingServicePtr logging)
{
    switch (statsFetcherType) {
        case NCloud::NProto::CGROUP: {
            if (cpuWaitFilename.empty()) {
                const auto& Log = log;
                STORAGE_INFO(
                    "CpuWaitFilename is empty, can't build "
                    "CgroupStatsFetcher");
                return CreateCgroupStatsFetcherStub();
            }

            return CreateCgroupStatsFetcher(
                "STORAGE_STATS",
                std::move(logging),
                std::move(cpuWaitFilename));
        }
        case NCloud::NProto::TASKSTATS:
            return CreateTaskStatsFetcher(
                "STORAGE_STATS",
                std::move(logging),
                getpid());
    }
}

}   // namespace NCloud::NStorage
