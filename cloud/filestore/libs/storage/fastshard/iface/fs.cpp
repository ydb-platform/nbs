#include "fs.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemShardStub: IFileSystemShard
{
    template <typename TResponse, typename TRequest>
    NThreading::TFuture<TResponse> NotImplemented(TRequest request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);
        return NThreading::MakeFuture(std::move(response));
    }

#define FAST_SHARD_DEFINE_METHOD(name, ns, ...)                                \
    NThreading::TFuture<ns::T##name##Response> name(                           \
        ns::T##name##Request request) override                                 \
    {                                                                          \
        return NotImplemented<ns::T##name##Response>(                          \
            std::move(request));                                               \
    }                                                                          \
// FAST_SHARD_DEFINE_METHOD

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_DEFINE_METHOD, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_DEFINE_METHOD, NProto)

#undef FAST_SHARD_DEFINE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateFileSystemShardStub()
{
    return std::make_shared<TFileSystemShardStub>();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
