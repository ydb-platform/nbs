#include "message.h"

namespace NCloud::NNetlink {

namespace NLibnl {

TNestedAttribute::TNestedAttribute(nl_msg* message, int attribute)
    : Message(message)
{
    Attribute = nla_nest_start(message, attribute);
    if (!Attribute) {
        throw TServiceError(E_FAIL) << "unable to nest attribute";
    }
}

TNestedAttribute::~TNestedAttribute()
{
    nla_nest_end(Message, Attribute);
}

TMessage::TMessage(int family, int command)
{
    Message = nlmsg_alloc();
    if (Message == nullptr) {
        throw TServiceError(E_FAIL) << "unable to allocate message";
    }
    genlmsg_put(
        Message,
        NL_AUTO_PORT,
        NL_AUTO_SEQ,
        family,
        0,   // hdrlen
        0,   // flags
        command,
        1);  // version
}

TMessage::~TMessage()
{
    nlmsg_free(Message);
}

TNestedAttribute TMessage::Nest(int attribute)
{
    return TNestedAttribute(Message, attribute);
}

void TMessage::Put(int attribute, void* data, size_t size)
{
    if (int err = nla_put(Message, attribute, size, data)) {
        throw TServiceError(E_FAIL)
            << "unable to put attribute " << attribute << ": "
            << nl_geterror(err);
    }
}

}   // namespace NLibnl

////////////////////////////////////////////////////////////////////////////////

void ValidateAttribute(const ::nlattr& attribute, ui16 expectedAttribute)
{
    if (attribute.nla_type != expectedAttribute) {
        throw TIoException() << "Invalid attribute type: " << attribute.nla_type
                           << " Expected attribute type: " << expectedAttribute;
    }
}

}   // namespace NCloud::NNetlink
