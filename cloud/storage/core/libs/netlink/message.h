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

    void Put(int attribute, void* data, size_t size);

    template <typename T>
    void Put(int attribute, T data)
    {
        Put(attribute, &data, sizeof(T));
    }
};

}   // namespace NCloud::NNetlink
