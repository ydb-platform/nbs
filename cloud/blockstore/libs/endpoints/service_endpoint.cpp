#include "service_endpoint.h"

#include "endpoint_manager.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <type_traits>

namespace NCloud::NBlockStore::NServer {

using namespace NClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEndpointService final
    : public IBlockStore
{
private:
    const IBlockStorePtr Service;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IEndpointManagerPtr EndpointManager;

public:
    TEndpointService(
            IBlockStorePtr service,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            IEndpointManagerPtr endpointManager)
        : Service(std::move(service))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , EndpointManager(std::move(endpointManager))
    {}

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

#define STORAGE_IMPLEMENT_METHOD(name, ...)                                    \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        using TMethod = TBlockStore##name##Method;                             \
        return ExecuteStorageRequest<TMethod>(                                 \
            std::move(ctx),                                                    \
            std::move(request));                                               \
    }                                                                          \
// STORAGE_IMPLEMENT_METHOD

    BLOCKSTORE_STORAGE_SERVICE(STORAGE_IMPLEMENT_METHOD)
    BLOCKSTORE_LOCAL_NVME_SERVICE(STORAGE_IMPLEMENT_METHOD)

#undef STORAGE_IMPLEMENT_METHOD

#define ENDPOINT_IMPLEMENT_METHOD(name, ...)                                   \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> req) override                \
    {                                                                          \
        auto timeout = req->GetHeaders().GetRequestTimeout();                  \
        auto future = EndpointManager->name(std::move(ctx), std::move(req));   \
        return CreateTimeoutFuture(future, TDuration::MilliSeconds(timeout));  \
    }                                                                          \
// ENDPOINT_IMPLEMENT_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(ENDPOINT_IMPLEMENT_METHOD)

#undef ENDPOINT_IMPLEMENT_METHOD

private:
    template <typename TMethod>
    TFuture<typename TMethod::TResponse> ExecuteStorageRequest(
        TCallContextPtr ctx,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        if constexpr (std::is_same_v<TMethod, TBlockStoreResizeVolumeMethod>) {
            const auto diskId = request->GetDiskId();
            const bool shouldRefreshEndpoint = request->GetBlocksCount() != 0;

            auto future = TMethod::Execute(
                Service.get(),
                std::move(ctx),
                std::move(request));

            return future.Apply([
                endpointManager = EndpointManager,
                diskId,
                shouldRefreshEndpoint] (const auto& f)
            {
                auto response = f.GetValue();
                if (!shouldRefreshEndpoint ||
                    response.GetError().GetCode() != S_OK ||
                    !endpointManager)
                {
                    return MakeFuture(std::move(response));
                }

                return endpointManager
                    ->RefreshEndpointIfNeeded(diskId, "volume resize")
                    .Apply([response = std::move(response)] (const auto& f) mutable
                    {
                        Y_UNUSED(f.GetValue());
                        return response;
                    });
            });
        } else {
            return TMethod::Execute(
                Service.get(),
                std::move(ctx),
                std::move(request));
        }
    }

    template <typename T>
    TFuture<T> CreateTimeoutFuture(const TFuture<T>& future, TDuration timeout)
    {
        if (!timeout) {
            return future;
        }

        auto promise = NewPromise<T>();

        Scheduler->Schedule(Timer->Now() + timeout, [=] () mutable {
            promise.TrySetValue(TErrorResponse(E_TIMEOUT, "Timeout"));
        });

        future.Subscribe([=] (const auto& f) mutable {
            promise.TrySetValue(f.GetValue());
        });

        return promise;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateMultipleEndpointService(
    IBlockStorePtr service,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IEndpointManagerPtr endpointManager)
{
    return std::make_shared<TEndpointService>(
        std::move(service),
        std::move(timer),
        std::move(scheduler),
        std::move(endpointManager));
}

}   // namespace NCloud::NBlockStore::NServer
