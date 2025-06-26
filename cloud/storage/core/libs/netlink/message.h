#include <cloud/storage/core/libs/common/error.h>

#include <linux/genetlink.h>

namespace NCloud::NNetlink {

////////////////////////////////////////////////////////////////////////////////

void ValidateAttribute(const ::nlattr& attribute, ui16 expectedAttribute);

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

    TNetlinkHeader(ui32 len, ui16 type, ui8 cmd)
        : MessageHeader{len, type, NLM_F_REQUEST | NLM_F_ACK, 0, 0}
        , GenericHeader{cmd, 1, 0}
    {}
};

template <int attribute, typename P>
struct TNetlinkAttribute
{
    ::nlattr Attribute;
    P Payload;

    TNetlinkAttribute(P payload)
        : Attribute(NLA_HDRLEN + sizeof(P), attribute)
        , Payload(payload)
    {}
};

template <typename H, typename... T>
struct TNetlinkPayload
{
    H Head;
    TNetlinkPayload<T...> Tail;

    TNetlinkPayload(H head, T... tail)
        : Head(head)
        , Tail(tail...)
    {}
};

template <typename H>
struct TNetlinkPayload<H>
{
    H Head;

    TNetlinkPayload(H head)
        : Head(head)
    {}
};

template <typename... P>
struct TNetlinkRequest
{
    TNetlinkHeader Header;
    TNetlinkPayload<P...> Payload;

    TNetlinkRequest(ui16 familyId, ui8 command, P... payload)
        : Header{sizeof(TNetlinkHeader) + sizeof(TNetlinkPayload<P...>), familyId, command}
        , Payload(payload...)
    {}
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

template <size_t FamilyNameLength>
struct TNetlinkFamilyIdRequest
{
    TNetlinkHeader Headers;
    ::nlattr FamilyNameAttr;
    std::array<char, FamilyNameLength> FamilyName;

    TNetlinkFamilyIdRequest(const char (&familyName)[FamilyNameLength])
    {
        // Use memset to resolve the "uninitialized bytes" memory sanitizer
        // warning, as this structure is transmitted via a socket, and padding
        // may be present depending on the length of the family name.
        memset(this, 0, sizeof(TNetlinkFamilyIdRequest<FamilyNameLength>));
        Headers = {
            sizeof(TNetlinkFamilyIdRequest<FamilyNameLength>),
            GENL_ID_CTRL,
            CTRL_CMD_GETFAMILY};
        FamilyNameAttr = {
            sizeof(FamilyName) + NLA_HDRLEN,
            CTRL_ATTR_FAMILY_NAME};
        memcpy(&FamilyName[0], familyName, FamilyNameLength);
    }
};

template<size_t FamilyNameLength>
struct TNetlinkFamilyIdResponse
{
    TNetlinkHeader Headers;
    ::nlattr FamilyNameAttr;
    std::array<char, FamilyNameLength> FamilyName;
    alignas(NLMSG_ALIGNTO)::nlattr FamilyIdAttr;
    ui16 FamilyId;

    void Validate()
    {
        ValidateAttribute(FamilyNameAttr, CTRL_ATTR_FAMILY_NAME);
        ValidateAttribute(FamilyIdAttr, CTRL_ATTR_FAMILY_ID);
    }
};

#pragma pack(pop)

}   // namespace NCloud::NNetlink
