#include "cgroup_stats_fetcher.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/file.h>

#include <iostream>

#include <linux/genetlink.h>
#include <linux/taskstats.h>
#include <linux/cgroupstats.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/netlink.h>

namespace NCloud::NStorage {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcher final
    : public IStatsFetcher
{
private:
    const TString ComponentName;

    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const TString StatsFile;
    const TCgroupStatsFetcherMonitoringSettings MonitoringSettings;

    TLog Log;

    TFile CpuAcctWait;

    TDuration Last;

    TIntrusivePtr<NMonitoring::TCounterForPtr> FailCounter;

public:
    TCgroupStatsFetcher(
            TString componentName,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            TString statsFile,
            TCgroupStatsFetcherMonitoringSettings monitoringSettings)
        : ComponentName(std::move(componentName))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , StatsFile(std::move(statsFile))
        , MonitoringSettings(std::move(monitoringSettings))
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
            ReportCpuWaitFatalError();
            STORAGE_ERROR(BuildErrorMessageFromException());
            return;
        }

        if (!CpuAcctWait.IsOpen()) {
            ReportCpuWaitFatalError();
            STORAGE_ERROR("Failed to open " << StatsFile);
            return;
        }

        Last = GetCpuWait();
    }

    void Stop() override
    {
    }

    TDuration GetCpuWait() override
    {
        if (!CpuAcctWait.IsOpen()) {
            return {};
        }

        try {
            CpuAcctWait.Seek(0, SeekDir::sSet);

            constexpr i64 bufSize = 1024;

            if (CpuAcctWait.GetLength() >= bufSize - 1) {
                ReportCpuWaitFatalError();
                STORAGE_ERROR(StatsFile << " is too large");
                CpuAcctWait.Close();
                return {};
            }

            char buf[bufSize];

            auto cnt = CpuAcctWait.Read(buf, bufSize - 1);
            if (buf[cnt - 1] == '\n') {
                --cnt;
            }
            buf[cnt] = '\0';
            auto value = TDuration::MicroSeconds(FromString<ui64>(buf) / 1000);

            if (value < Last) {
                STORAGE_ERROR(
                    ReportCpuWaitCounterReadError(
                        TStringBuilder() << StatsFile <<
                        " : new value " << value <<
                        " is less than previous " << Last));
                Last = value;
                return {};
            }
            auto retval = value - Last;
            Last = value;

            return retval;
        } catch (...) {
            ReportCpuWaitFatalError();
            STORAGE_ERROR(BuildErrorMessageFromException())
            CpuAcctWait.Close();
            return {};
        }
    }

    TString BuildErrorMessageFromException()
    {
        auto msg = TStringBuilder() << "IO error for " << StatsFile;
        msg << " with exception " << CurrentExceptionMessage();
        return msg;
    }

    void ReportCpuWaitFatalError()
    {
        if (FailCounter) {
            return;
        }
        if (MonitoringSettings.ComponentGroupName.empty() ||
            MonitoringSettings.CountersGroupName.empty() ||
            MonitoringSettings.CounterName.empty())
        {
            return;
        }
        FailCounter = Monitoring->GetCounters()
            ->GetSubgroup("counters", MonitoringSettings.CountersGroupName)
            ->GetSubgroup("component", MonitoringSettings.ComponentGroupName)
            ->GetCounter(MonitoringSettings.CounterName, false);
        *FailCounter = 1;
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

    TDuration GetCpuWait() override
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////


class TNetlinkSocket
{
private:
    nl_sock* Socket;
    int Family;

public:
    TNetlinkSocket(const char* netlinkFamilyName)
    {
        Socket = nl_socket_alloc();

        if (Socket == nullptr) {
            throw TServiceError(E_FAIL) << "unable to allocate netlink socket";
        }

        if (int err = genl_connect(Socket)) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to connect to generic netlink socket: "
                << nl_geterror(err);
        }

        Family = genl_ctrl_resolve(Socket, netlinkFamilyName);

        if (Family < 0) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to resolve nbd netlink family: "
                << nl_geterror(Family);
        }
    }

    ~TNetlinkSocket()
    {
        nl_socket_free(Socket);
    }

    operator nl_sock*() const
    {
        return Socket;
    }

    int GetFamily() const
    {
        return Family;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNetlinkMessage
{
private:
    nl_msg* Message;

public:
    TNetlinkMessage(int family, int command)
    {
        Message = nlmsg_alloc();
        if (Message == nullptr) {
            throw TServiceError(E_FAIL) << "unable to allocate message";
        }
        genlmsg_put(
            Message,
            NL_AUTO_PID,
            NL_AUTO_SEQ,
            family,
            0,          // hdrlen
            NLM_F_REQUEST,
            command,
            TASKSTATS_VERSION);
    }

    ~TNetlinkMessage()
    {
        if (Message) {
            nlmsg_free(Message);
        }
    }

    template <typename T>
    void Put(int attribute, T data)
    {
        if (int err = nla_put(Message, attribute, sizeof(T), &data)) {
            throw TServiceError(E_FAIL) << "unable to put attribute "
                << attribute << ": " << nl_geterror(err);
        }
    }

    void Send(nl_sock* socket)
    {
        // send will free message even if it fails
        auto* message = Message;
        Message = nullptr;
        if (int err = nl_send_sync(socket, message)) {
            throw TServiceError(E_FAIL)
                << "unable to send message: " << nl_geterror(err);
        }
    }
};

class TCpuDelayReceiver
{
    private:
    nl_sock* Socket;
    int DelayNanoSeconds = 0;
public:
    explicit TCpuDelayReceiver(nl_sock* socket)
        : Socket(socket)
    {
        if (auto err = nl_socket_modify_cb(
                Socket,
                NL_CB_VALID,
                NL_CB_CUSTOM,
                TCpuDelayReceiver::CpuDelayCallbackMessage,
                &DelayNanoSeconds);
            err < 0)
        {
            throw TServiceError(E_FAIL) << "set cb error: " << nl_geterror(err);
        }
    }

    TDuration Wait()
    {
        if (auto err = nl_recvmsgs_default(Socket); err < 0) {
            throw TServiceError(E_FAIL)
                << "unable to receive message: " << nl_geterror(err);
        }

        return TDuration::MilliSeconds(DelayNanoSeconds / 1000);
    }

private:
    static int CpuDelayCallbackMessage(struct nl_msg* nlmsg, void* arg)
    {
        nlattr* nlattrs[TASKSTATS_TYPE_MAX + 1];
        if (auto rc = genlmsg_parse(
                nlmsg_hdr(nlmsg),
                0,
                nlattrs,
                TASKSTATS_TYPE_MAX,
                NULL);
            rc < 0)
        {
            std::cerr << "error parsing msg" << std::endl;
            return -1;
        }

        if (auto nlattr = nlattrs[TASKSTATS_TYPE_AGGR_PID]; nlattr != nullptr)
        {
            const auto pdata =
                reinterpret_cast<const struct nlattr*>(nla_data(nlattr));
            int rem = 0;
            auto stats =
                reinterpret_cast<taskstats*>(nla_data(nla_next(pdata, &rem)));
            if (arg) {
                *reinterpret_cast<ui32*>(arg) = stats->cpu_delay_total;
            }
            return 0;
        } else {
            std::cerr << "unknown attribute format received" << std::endl;
            return -1;
        }
        return 0;
    }
};

struct TKernelTaskDelayAcctStatsFetcher final: public IStatsFetcher
{
private:
    const TString ComponentName;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    TLog Log;
    std::unique_ptr<TNetlinkSocket> NetlinkSocket;

public:
    TKernelTaskDelayAcctStatsFetcher(
        TString componentName,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring)
        : ComponentName(std::move(componentName))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
    {
    }

    ~TKernelTaskDelayAcctStatsFetcher() override {
        Stop();
    }

    void Start() override
    {
        Log = Logging->CreateLog(ComponentName);
        NetlinkSocket = std::make_unique<TNetlinkSocket>(TASKSTATS_GENL_NAME);
    }

    void Stop() override
    {
        NetlinkSocket.reset();
    }

    TDuration GetCpuWait() override
    {
        if (!NetlinkSocket) {
            STORAGE_ERROR("Invalid netlink socket");
            return {};
        }
        try {
            int mypid = getpid();
            TNetlinkMessage message(
                NetlinkSocket->GetFamily(),
                TASKSTATS_CMD_GET);
            message.Put(TASKSTATS_CMD_ATTR_PID, mypid);
            TCpuDelayReceiver receiver(*NetlinkSocket);
            message.Send(*NetlinkSocket);
            return receiver.Wait();
        } catch (...) {
            STORAGE_ERROR(BuildErrorMessageFromException());
        }

        return {};
    }

    TString BuildErrorMessageFromException()
    {
        auto msg = TStringBuilder() << "IO error";
        msg << " with exception " << CurrentExceptionMessage();
        return msg;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString statsFile,
    TCgroupStatsFetcherMonitoringSettings settings)
{
    return std::make_shared<TCgroupStatsFetcher>(
        std::move(componentName),
        std::move(logging),
        std::move(monitoring),
        std::move(statsFile),
        std::move(settings));
}

IStatsFetcherPtr CreateKernelTaskDelayAcctStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring)
{
    return std::make_shared<TKernelTaskDelayAcctStatsFetcher>(
        std::move(componentName),
        std::move(logging),
        std::move(monitoring));
}

IStatsFetcherPtr CreateStatsFetcherStub()
{
    return std::make_shared<TStatsFetcherStub>();
}

TString BuildCpuWaitStatsFilename(const TString& serviceName)
{
    static constexpr auto CpuWaitStatsFilenameTemplate =
        "/sys/fs/cgroup/cpu/system.slice/%s.service/cpuacct.wait";
    if (!serviceName.Empty()) {
        return Sprintf(CpuWaitStatsFilenameTemplate, serviceName.c_str());
    }
    return {};
}

}   // namespace NCloud::NStorage
