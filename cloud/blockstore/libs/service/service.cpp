#include "service.h"

#include <cloud/blockstore/libs/service/service_method.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBlockStoreStub final
    : public TBlockStoreImpl<TBlockStoreStub, IBlockStore>
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture<typename TMethod::TResponse>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateBlockStoreStub()
{
    return std::make_shared<TBlockStoreStub>();
}

}   // namespace NCloud::NBlockStore
