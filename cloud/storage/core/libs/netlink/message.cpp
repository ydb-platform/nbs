#include "message.h"

namespace NCloud::NNetlink {

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

TMessage::TMessage(int family, int command, int flags, int version)
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
        flags,
        command,
        version);
}

TMessage::~TMessage()
{
    nlmsg_free(Message);
}

TNestedAttribute TMessage::Nest(int attribute)
{
    return TNestedAttribute(Message, attribute);
}

}   // namespace NCloud::NNetlink
