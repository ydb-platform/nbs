#include "stats_fetcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/netlink/socket.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <linux/genetlink.h>
#include <linux/taskstats.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/netlink.h>

namespace NCloud::NStorage {

namespace {


////////////////////////////////////////////////////////////////////////////////

struct TTaskStatsFetcher final: public IStatsFetcher
{
    using TNetlinkMessage = NCloud::NNetlink::TMessage;
    using TNetlinkSocket = NCloud::NNetlink::TSocket;
private:
    const TString ComponentName;
    const ILoggingServicePtr Logging;
    int Pid;
    TLog Log;
    const TDuration NetlinkSocketTimeout = TDuration::Seconds(1);
    TDuration Last;

public:
    TTaskStatsFetcher(
            TString componentName,
            ILoggingServicePtr logging,
            int pid)
        : ComponentName(std::move(componentName))
        , Logging(std::move(logging))
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
                nullptr) != 0)
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

IStatsFetcherPtr CreateTaskStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    int pid)
{
    return std::make_shared<TTaskStatsFetcher>(
        std::move(componentName),
        std::move(logging),
        pid);
}

}   // namespace NCloud::NStorage
