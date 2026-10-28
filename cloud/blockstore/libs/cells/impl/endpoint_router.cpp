#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/generic/ptr.h>

#include <atomic>
#include <type_traits>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// THotSwap stores an atomically refcounted object, while an endpoint is held
// by a shared_ptr, hence the wrapper.
struct TTarget: public TAtomicRefCount<TTarget>
{
    const IBlockStorePtr Endpoint;
    std::atomic<ui64> ReadRequests = 0;
    std::atomic<ui64> ReadErrors = 0;
    std::atomic<ui64> ReadBytes = 0;
    std::atomic<ui64> WriteRequests = 0;
    std::atomic<ui64> WriteErrors = 0;
    std::atomic<ui64> WriteBytes = 0;
    std::atomic<ui64> ZeroRequests = 0;
    std::atomic<ui64> ZeroErrors = 0;

    explicit TTarget(IBlockStorePtr endpoint)
        : Endpoint(std::move(endpoint))
    {}

    TEndpointIoStats GetIoStats() const
    {
        return {
            .ReadRequests = ReadRequests.load(std::memory_order_relaxed),
            .ReadErrors = ReadErrors.load(std::memory_order_relaxed),
            .ReadBytes = ReadBytes.load(std::memory_order_relaxed),
            .WriteRequests = WriteRequests.load(std::memory_order_relaxed),
            .WriteErrors = WriteErrors.load(std::memory_order_relaxed),
            .WriteBytes = WriteBytes.load(std::memory_order_relaxed),
            .ZeroRequests = ZeroRequests.load(std::memory_order_relaxed),
            .ZeroErrors = ZeroErrors.load(std::memory_order_relaxed),
        };
    }
};

template <typename TRequest>
ui64 RequestBytes(const TRequest& request)
{
    if constexpr (std::is_same_v<TRequest, NProto::TReadBlocksLocalRequest> ||
                  std::is_same_v<TRequest, NProto::TWriteBlocksLocalRequest>)
    {
        return CalculateBytesCount(request, request.GetBlockSize());
    }
    return 0;
}

template <typename TRequest, typename TCompletion>
void NoteCompletedIo(
    TTarget& target,
    ui64 bytes,
    const TCompletion& completed)
{
    if constexpr (std::is_same_v<TRequest, NProto::TReadBlocksLocalRequest> ||
                  std::is_same_v<TRequest, NProto::TWriteBlocksLocalRequest> ||
                  std::is_same_v<TRequest, NProto::TZeroBlocksRequest>)
    {
        bool failed = true;
        try {
            failed = HasError(completed.GetValue());
        } catch (...) {
            // An exceptional completion is an I/O error as well.
        }
        if (failed) {
            if constexpr (
                std::is_same_v<TRequest, NProto::TReadBlocksLocalRequest>)
            {
                target.ReadErrors.fetch_add(1, std::memory_order_relaxed);
            } else if constexpr (
                std::is_same_v<TRequest, NProto::TWriteBlocksLocalRequest>)
            {
                target.WriteErrors.fetch_add(1, std::memory_order_relaxed);
            } else {
                target.ZeroErrors.fetch_add(1, std::memory_order_relaxed);
            }
        } else if constexpr (
            std::is_same_v<TRequest, NProto::TReadBlocksLocalRequest>)
        {
            target.ReadRequests.fetch_add(1, std::memory_order_relaxed);
            target.ReadBytes.fetch_add(bytes, std::memory_order_relaxed);
        } else if constexpr (
            std::is_same_v<TRequest, NProto::TWriteBlocksLocalRequest>)
        {
            target.WriteRequests.fetch_add(1, std::memory_order_relaxed);
            target.WriteBytes.fetch_add(bytes, std::memory_order_relaxed);
        } else {
            target.ZeroRequests.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

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

    TEndpointIoStats GetIoStats() const override
    {
        return Target.AtomicLoad()->GetIoStats();
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto target = Target.AtomicLoad();
        const ui64 bytes = RequestBytes(*request);

        auto future = TMethod::Execute(
            target->Endpoint.get(),
            std::move(callContext),
            std::move(request));

        // the target must outlive the request it is serving, even if it gets
        // replaced in the meantime
        future.Subscribe(
            [target = std::move(target), bytes](const auto& completed)
            {
                NoteCompletedIo<typename TMethod::TRequest>(
                    *target,
                    bytes,
                    completed);
            });

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
