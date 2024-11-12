#include <cloud/storage/core/libs/common/error.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>

namespace NCloud::NNetlink {

////////////////////////////////////////////////////////////////////////////////

class TNestedAttribute
{
private:
    nl_msg* Message;
    nlattr* Attribute;

public:
    TNestedAttribute(nl_msg* message, int attribute)
        : Message(message)
    {
        Attribute = nla_nest_start(message, attribute);
        if (!Attribute) {
            throw TServiceError(E_FAIL) << "unable to nest attribute";
        }
    }

    ~TNestedAttribute()
    {
        nla_nest_end(Message, Attribute);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMessage
{
private:
    nl_msg* Message;

public:
    TMessage(int family, int command, int flags = 0, int version = 0)
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

    ~TMessage()
    {
        nlmsg_free(Message);
    }

    operator nl_msg*() const
    {
        return Message;
    }

    template <typename T>
    void Put(int attribute, T data)
    {
        if (int err = nla_put(Message, attribute, sizeof(T), &data)) {
            throw TServiceError(E_FAIL)
                << "unable to put attribute " << attribute << ": "
                << nl_geterror(err);
        }
    }

    TNestedAttribute Nest(int attribute)
    {
        return TNestedAttribute(Message, attribute);
    }
};

}   // namespace NCloud::NNetlink
