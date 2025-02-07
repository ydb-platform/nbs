#include <cloud/storage/core/libs/common/error.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>

namespace NCloud::NNetlink {

namespace NLibnl {

////////////////////////////////////////////////////////////////////////////////

class TNestedAttribute
{
private:
    nl_msg* Message;
    nlattr* Attribute;

public:
    TNestedAttribute(nl_msg* message, int attribute);
    ~TNestedAttribute();
};

////////////////////////////////////////////////////////////////////////////////

class TMessage
{
private:
    nl_msg* Message;

public:
    TMessage(int family, int command);
    ~TMessage();

    operator nl_msg*() const
    {
        return Message;
    }

    TNestedAttribute Nest(int attribute);

    void Put(int attribute, void* data, size_t size);

    template <typename T>
    void Put(int attribute, T data)
    {
        Put(attribute, &data, sizeof(T));
    }
};

}  // namespace NLibnl

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, NLMSG_ALIGNTO)

struct TNetlinkError
{
    ::nlmsghdr MessageHeader;
    ::nlmsgerr MessageError;
};

struct TNetlinkHeader
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;

    TNetlinkHeader() = default;

    TNetlinkHeader(uint32_t len, uint16_t type, uint8_t cmd, bool ack)
        : MessageHeader{len, type, NLM_F_REQUEST, 0, 0}
        , GenericHeader{cmd, 1, 0}
    {
        if (ack) {
            MessageHeader.nlmsg_flags |= NLM_F_ACK;
        }
    }
};

struct TNetlinkMessage
{
    TNetlinkHeader Headers;

    void Validate()
    {}
};

template <typename TMessage = TNetlinkMessage, size_t MaxMsgSize = 1024>
union TNetlinkResponse {
    TMessage Msg;
    TNetlinkError NetlinkError;
    ui8 Buffer[MaxMsgSize];

    TNetlinkResponse()
    {
        static_assert(sizeof(TMessage) < MaxMsgSize);
    }
};

#pragma pack(pop)

}   // namespace NCloud::NNetlink
