#include "throttling.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/throttling/throttler.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlingService final: public IBlockStore
{
private:
    IBlockStorePtr Service;
    IThrottlerPtr Throttler;

public:
    TThrottlingService(IBlockStorePtr service, IThrottlerPtr throttler)
        : Service(std::move(service))
        , Throttler(std::move(throttler))
    {}

    void Start() override
    {
        Service->Start();
        Throttler->Start();
    }

    void Stop() override
    {
        Throttler->Stop();
        Service->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                      \
    TFuture<NProto::T##name##Response> name(                        \
        TCallContextPtr callContext,                                \
        std::shared_ptr<NProto::T##name##Request> request) override \
    {                                                               \
        return Throttler->name(                                     \
            Service,                                                \
            std::move(callContext),                                 \
            std::move(request));                                    \
    }                                                               \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateThrottlingService(
    IBlockStorePtr service,
    IThrottlerPtr throttler)
{
    return std::make_shared<TThrottlingService>(
        std::move(service),
        std::move(throttler));
}

}   // namespace NCloud::NBlockStore
