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
    ::nlattr TgidAttr;
    ui32 Tgid;

    TTaskStatsRequest(ui16 familyId, ui32 tgid)
        : MessageHeader{sizeof(TTaskStatsRequest), familyId, NLM_F_REQUEST, 0, 0}
        , GenericHeader{TASKSTATS_CMD_GET, 1, 0}
        , TgidAttr{sizeof(Tgid) + NLA_HDRLEN, TASKSTATS_CMD_ATTR_TGID}
        , Tgid(tgid)
    {}
};

struct TTaskStatsResponse
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;
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

template <typename T, size_t MaxMsgSize = 1024>
union TNetlinkResponse {
    T Msg;
    ui8 Buffer[MaxMsgSize];

    TNetlinkResponse() {
        static_assert(sizeof(T) < MaxMsgSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNetlinkSocket
{
private:
    TSocket Socket;
    ui32 SocketTimeoutMs = 100;

public:
    TNetlinkSocket(ui32 socketTimeoutMs = 100)
        : Socket(::socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC))
        , SocketTimeoutMs(socketTimeoutMs)
    {
        if (Socket < 0) {
            throw yexception() << "Failed to create netlink socket";
        }
        Socket.SetSocketTimeout(0, SocketTimeoutMs);
    }

    template <typename TNetlinkMessage>
    void Send(const TNetlinkMessage& msg)
    {
        auto ret = Socket.Send(&msg, sizeof(msg));
        if (ret == -1) {
            throw yexception()
                << "Failed to send netlink message: " << strerror(errno);
        }
    }

    template <typename T>
    void Receive(TNetlinkResponse<T>& response)
    {
        auto ret = Socket.Recv(&response, sizeof(response));
        if (ret < 0) {
            throw yexception()
                << "Failed to receive netlink message: " << strerror(errno);
        }

        if (response.Msg.MessageHeader.nlmsg_type == NLMSG_ERROR) {
            throw yexception()
                << "Failed to receive netlink message: kernel returned error";
        }

        if (!NLMSG_OK(&response.Msg.MessageHeader, ret)) {
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
        socket.Send(TTaskStatsFamilyIdRequest());
        TNetlinkResponse<TTaskStatsFamilyIdResponse> response;
        socket.Receive(response);
        response.Msg.Validate();
        return response.Msg.FamilyId;
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
            socket.Send(TTaskStatsRequest(FamilyId, Pid));
            TNetlinkResponse<TTaskStatsResponse> response;
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
