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

struct TIoCounters
{
    std::atomic<ui64> Requests = 0;
    std::atomic<ui64> Errors = 0;
    std::atomic<ui64> Bytes = 0;

    TIoOperationStats Snapshot() const
    {
        return {
            .Requests = Requests.load(std::memory_order_relaxed),
            .Errors = Errors.load(std::memory_order_relaxed),
            .Bytes = Bytes.load(std::memory_order_relaxed),
        };
    }

    template <typename TResponse>
    void Record(const TFuture<TResponse>& completed, ui64 bytes)
    {
        bool failed = true;
        try {
            failed = HasError(completed.GetValue());
        } catch (...) {
            // An exceptional completion is an I/O error as well.
        }
        if (failed) {
            Errors.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        Requests.fetch_add(1, std::memory_order_relaxed);
        if constexpr (!std::is_same_v<TResponse, NProto::TZeroBlocksResponse>) {
            Bytes.fetch_add(bytes, std::memory_order_relaxed);
        }
    }
};

// THotSwap stores an atomically refcounted object, while an endpoint is held
// by a shared_ptr, hence the wrapper.
struct TTarget: public TAtomicRefCount<TTarget>
{
    const IBlockStorePtr Endpoint;
    TIoCounters Read;
    TIoCounters Write;
    TIoCounters Zero;

    explicit TTarget(IBlockStorePtr endpoint)
        : Endpoint(std::move(endpoint))
    {}

    TEndpointIoStats GetIoStats() const
    {
        return {
            .Read = Read.Snapshot(),
            .Write = Write.Snapshot(),
            .Zero = Zero.Snapshot(),
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
    if constexpr (std::is_same_v<TRequest, NProto::TReadBlocksLocalRequest>) {
        target.Read.Record(completed, bytes);
    } else if constexpr (std::is_same_v<TRequest, NProto::TWriteBlocksLocalRequest>) {
        target.Write.Record(completed, bytes);
    } else if constexpr (std::is_same_v<TRequest, NProto::TZeroBlocksRequest>) {
        target.Zero.Record(completed, bytes);
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
