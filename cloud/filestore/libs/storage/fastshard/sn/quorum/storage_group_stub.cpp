#include "storage_group.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageGroupStub: public IStorageGroup
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

IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TVector<IStorageNodePtr> nodes)
{
    Y_UNUSED(nodes);

    return std::make_shared<TStorageGroupStub>();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
