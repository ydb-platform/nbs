#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/service_method.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/generic/ptr.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// THotSwap stores an atomically refcounted object, while an endpoint is held
// by a shared_ptr, hence the wrapper.
struct TTarget: public TAtomicRefCount<TTarget>
{
    const IBlockStorePtr Endpoint;

    explicit TTarget(IBlockStorePtr endpoint)
        : Endpoint(std::move(endpoint))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointRouter final
    : public TBlockStoreImpl<TEndpointRouter, IEndpointRouter>
{
private:
    // read on every request and replaced rarely, so reads have to stay
    // wait-free instead of serializing on a lock
    THotSwap<TTarget> Target;

public:
    explicit TEndpointRouter(IBlockStorePtr target)
        : Target(MakeIntrusive<TTarget>(std::move(target)))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Target.AtomicLoad()->Endpoint->AllocateBuffer(bytesCount);
    }

    void SetTarget(IBlockStorePtr target) override
    {
        Target.AtomicStore(MakeIntrusive<TTarget>(std::move(target)));
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto target = Target.AtomicLoad();

        auto future = TMethod::Execute(
            target->Endpoint.get(),
            std::move(callContext),
            std::move(request));

        // the target must outlive the request it is serving, even if it gets
        // replaced in the meantime
        future.Subscribe([target = std::move(target)](const auto&) {});

        return future;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointRouterPtr CreateEndpointRouter(IBlockStorePtr target)
{
    return std::make_shared<TEndpointRouter>(std::move(target));
}

}   // namespace NCloud::NBlockStore::NCells
