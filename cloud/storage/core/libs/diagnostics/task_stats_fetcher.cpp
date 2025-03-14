#include "stats_fetcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/netlink/socket.h>

#include <util/generic/yexception.h>
#include <util/network/socket.h>
#include <util/string/builder.h>

#include <linux/genetlink.h>
#include <linux/taskstats.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void ValidateAttribute(const ::nlattr& attribute, ui16 expectedAttribute)
{
    if (attribute.nla_type != expectedAttribute) {
        throw yexception() << "Invalid attribute type: " << attribute.nla_type
                           << " Expected attribute type: " << expectedAttribute;
    }
}

////////////////////////////////////////////////////////////////////////////////

// Documentation:
// https://github.com/torvalds/linux/blob/master/Documentation/accounting/taskstats.rst

#pragma pack(push, NLMSG_ALIGNTO)
struct TTaskStatsRequest
{
    NNetlink::TNetlinkHeader Headers;
    ::nlattr TgidAttr;
    ui32 Tgid;

    TTaskStatsRequest(ui16 familyId, ui32 tgid)
        : Headers(sizeof(TTaskStatsRequest), familyId, TASKSTATS_CMD_GET)
        , TgidAttr{sizeof(Tgid) + NLA_HDRLEN, TASKSTATS_CMD_ATTR_TGID}
        , Tgid(tgid)
    {}
};

struct TTaskStatsResponse
{
    NNetlink::TNetlinkHeader Headers;
    ::nlattr AggrTgidAttr;
    ::nlattr TgidAttr;
    ui32 Tgid;
    ::nlattr TaskStatsAttr;
    ::taskstats TaskStats;

    void Validate()
    {
        ValidateAttribute(AggrTgidAttr, TASKSTATS_TYPE_AGGR_TGID);
        ValidateAttribute(TgidAttr, TASKSTATS_TYPE_TGID);
        ValidateAttribute(TaskStatsAttr, TASKSTATS_TYPE_STATS);
    }
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

struct TTaskStatsFetcher final: public IStatsFetcher
{
private:
    const TString ComponentName;
    const ILoggingServicePtr Logging;
    int Pid;
    TLog Log;
    const TDuration NetlinkSocketTimeout = TDuration::Seconds(1);
    TDuration Last;

    ui16 GetFamilyId()
    {
        static ui16 familyId = NNetlink::GetFamilyId(TASKSTATS_GENL_NAME);
        return familyId;
    }

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

    ~TTaskStatsFetcher() override
    {
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
            NNetlink::TNetlinkSocket socket;
            socket.Send(TTaskStatsRequest(GetFamilyId(), Pid));
            NNetlink::TNetlinkResponse<TTaskStatsResponse> response;
            socket.Receive(response);
            response.Msg.Validate();
            auto cpuLack = TDuration::MicroSeconds(
                response.Msg.TaskStats.cpu_delay_total / 1000);
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
