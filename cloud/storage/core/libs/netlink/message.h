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

    template <typename T>
    void Put(int attribute, T data)
    {
        if (int err = nla_put(Message, attribute, sizeof(T), &data)) {
            throw TServiceError(E_FAIL)
                << "unable to put attribute " << attribute << ": "
                << nl_geterror(err);
        }
    }
};

}   // namespace NCloud::NNetlink
