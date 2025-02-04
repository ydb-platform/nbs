#include "stats_fetcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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

struct TTaskStatsFamilyIdRequest
{
    ::nlmsghdr MessageHeader =
        {sizeof(TTaskStatsFamilyIdRequest), GENL_ID_CTRL, NLM_F_REQUEST, 0, 0};
    ::genlmsghdr GenericHeader = {CTRL_CMD_GETFAMILY, 1, 0};
    ::nlattr FamilyNameAttr = {
        sizeof(FamilyName) + NLA_HDRLEN,
        CTRL_ATTR_FAMILY_NAME};
    const char FamilyName[sizeof(TASKSTATS_GENL_NAME)] = TASKSTATS_GENL_NAME;
};

struct TTaskStatsFamilyIdResponse
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;
    ::nlattr FamilyNameAttr;
    char FamilyName[sizeof(TASKSTATS_GENL_NAME)];
    alignas(NLMSG_ALIGNTO)::nlattr FamilyIdAttr;
    ui16 FamilyId;

    void Validate()
    {
        ValidateAttribute(FamilyNameAttr, CTRL_ATTR_FAMILY_NAME);
        ValidateAttribute(FamilyIdAttr, CTRL_ATTR_FAMILY_ID);
    }
};

struct TTaskStatsRequest
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;
    ::nlattr PidAttr;
    ui32 Pid;

    TTaskStatsRequest(ui16 familyId, ui32 pid)
        : MessageHeader{sizeof(TTaskStatsRequest), familyId, NLM_F_REQUEST, 0, 0}
        , GenericHeader{TASKSTATS_CMD_GET, 1, 0}
        , PidAttr{sizeof(Pid) + NLA_HDRLEN, TASKSTATS_CMD_ATTR_PID}
        , Pid(pid)
    {}
};

struct TTaskStatsResponse
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;
    ::nlattr AggrPidAttr;
    ::nlattr PidAttr;
    ui32 Pid;
    ::nlattr TaskStatsAttr;
    ::taskstats TaskStats;

    void Validate()
    {
        ValidateAttribute(AggrPidAttr, TASKSTATS_TYPE_AGGR_PID);
        ValidateAttribute(PidAttr, TASKSTATS_TYPE_PID);
        ValidateAttribute(TaskStatsAttr, TASKSTATS_TYPE_STATS);
    }
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

template <typename T, size_t MaxMsgSize = 1024>
union TNetlinkResponse {
    T msg;
    ui8 buffer[MaxMsgSize];

    TNetlinkResponse() {
        static_assert(sizeof(T) < MaxMsgSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNetlinkSocket
{
    const static long kSocketTimeoutMs = 100;

private:
    TSocket socket;

public:
    TNetlinkSocket()
        : socket(::socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC))
    {
        if (socket < 0) {
            throw yexception() << "Failed to create netlink socket";
        }
        socket.SetSocketTimeout(0, kSocketTimeoutMs);
    }

    template <typename TNetlinkMessage>
    void send(const TNetlinkMessage& msg)
    {
        auto ret = socket.Send(&msg, sizeof(msg));
        if (ret == -1) {
            throw yexception()
                << "Failed to send netlink message: " << strerror(errno);
        }
    }

    template <typename T>
    void receive(TNetlinkResponse<T>& response)
    {
        auto ret = socket.Recv(&response, sizeof(response));
        if (ret < 0) {
            throw yexception()
                << "Failed to receive netlink message: " << strerror(errno);
        }

        if (response.msg.MessageHeader.nlmsg_type == NLMSG_ERROR) {
            throw yexception()
                << "Failed to receive netlink message: kernel returned error";
        }

        if (!NLMSG_OK(&response.msg.MessageHeader, ret)) {
            throw yexception()
                << "Failed to parse netlink message: incorrect format";
        }
        return;
    }
};

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
    ui16 FamilyId;

    ui16 GetFamilyId()
    {
        TNetlinkSocket socket;
        socket.send(TTaskStatsFamilyIdRequest());
        TNetlinkResponse<TTaskStatsFamilyIdResponse> response;
        socket.receive(response);
        response.msg.Validate();
        return response.msg.FamilyId;
    }

public:
    TTaskStatsFetcher(
            TString componentName,
            ILoggingServicePtr logging,
            int pid)
        : ComponentName(std::move(componentName))
        , Logging(std::move(logging))
        , Pid(pid)
        , FamilyId(0)
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
            if (FamilyId == 0) {
                FamilyId = GetFamilyId();
            }

            TNetlinkSocket socket;
            socket.send(TTaskStatsRequest(FamilyId, Pid));
            TNetlinkResponse<TTaskStatsResponse> response;
            socket.receive(response);
            response.msg.Validate();
            auto cpuLack = TDuration::MilliSeconds(
                response.msg.TaskStats.cpu_delay_total / 1000);
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
