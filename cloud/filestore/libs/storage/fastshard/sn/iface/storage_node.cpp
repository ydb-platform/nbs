#include "storage_node.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageNodeStub: public IStorageNode
{
public:
#define SN_STUB_METHOD(name, ...)                                              \
    NCloud::NProto::T##name##Response name(                                    \
        NCloud::NProto::T##name##Request request) override                     \
    {                                                                          \
        Y_UNUSED(request);                                                     \
        NProto::T##name##Response response;                                    \
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);               \
        return response;                                                       \
    }                                                                          \
    // SN_STUB_METHOD

    SN_METHODS(SN_STUB_METHOD)

#undef SN_STUB_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateStorageNodeStub()
{
    return std::make_shared<TStorageNodeStub>();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
