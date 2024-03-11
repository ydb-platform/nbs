#include "service_endpoint.h"

#include "endpoint_manager.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/metric.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/keyring/endpoints.h>

#include <util/generic/guid.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NServer {

using namespace NClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEndpointClient final
    : public IBlockStore
{
private:
    IEndpointManagerPtr EndpointManager;

public:
    TEndpointClient(IEndpointManagerPtr endpointManager)
        : EndpointManager(std::move(endpointManager))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(ctx);                                                         \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED, "Unsupported request"));                        \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)
    BLOCKSTORE_IMPLEMENT_METHOD(KickEndpoint)
    BLOCKSTORE_IMPLEMENT_METHOD(ListKeyrings)

#undef BLOCKSTORE_IMPLEMENT_METHOD

#define ENDPOINT_IMPLEMENT_METHOD(name, ...)                                   \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> req) override                \
    {                                                                          \
        return EndpointManager->name(std::move(ctx), std::move(req));          \
    }                                                                          \
// ENDPOINT_IMPLEMENT_METHOD

    ENDPOINT_IMPLEMENT_METHOD(StartEndpoint)
    ENDPOINT_IMPLEMENT_METHOD(StopEndpoint)
    ENDPOINT_IMPLEMENT_METHOD(ListEndpoints)
    ENDPOINT_IMPLEMENT_METHOD(DescribeEndpoint)
    ENDPOINT_IMPLEMENT_METHOD(RefreshEndpoint)

#undef ENDPOINT_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointService final
    : public IEndpointService
    , public std::enable_shared_from_this<TEndpointService>
{
private:
    const IBlockStorePtr Service;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const TExecutorPtr Executor;
    const IEndpointStoragePtr EndpointStorage;
    const IEndpointManagerPtr EndpointManager;

    IMetricClientPtr RestoringClient;
    TSet<TString> RestoringEndpoints;

    TLog Log;

    enum {
        WaitingForRestoring = 0,
        ReadingStorage = 1,
        StartingEndpoints = 2,
        Completed = 3,
    };

    TAtomic RestoringStage = WaitingForRestoring;

public:
    TEndpointService(
            IBlockStorePtr service,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IRequestStatsPtr requestStats,
            IVolumeStatsPtr volumeStats,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            IEndpointStoragePtr endpointStorage,
            IEndpointManagerPtr endpointManager,
            NProto::TClientConfig clientConfig)
        : Service(std::move(service))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Executor(std::move(executor))
        , EndpointStorage(std::move(endpointStorage))
        , EndpointManager(std::move(endpointManager))
    {
        Log = logging->CreateLog("BLOCKSTORE_SERVER");

        IBlockStorePtr client = std::make_shared<TEndpointClient>(
            EndpointManager);

        NProto::TClientAppConfig config;
        *config.MutableClientConfig() = std::move(clientConfig);
        auto appConfig = std::make_shared<TClientAppConfig>(std::move(config));
        auto retryPolicy = CreateRetryPolicy(appConfig);

        client = CreateDurableClient(
            std::move(appConfig),
            std::move(client),
            std::move(retryPolicy),
            logging,
            Timer,
            Scheduler,
            std::move(requestStats),
            std::move(volumeStats));

        RestoringClient = CreateMetricClient(
            std::move(client),
            std::move(logging),
            std::move(serverStats));
    }

    void Start() override
    {
        Service->Start();
        RestoringClient->Start();
    }

    void Stop() override
    {
        RestoringClient->Stop();
        Service->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override
    {
        return RestoringClient->CollectRequests(collector);
    }

#define STORAGE_IMPLEMENT_METHOD(name, ...)                                    \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return Service->name(std::move(ctx), std::move(request));              \
    }                                                                          \
// STORAGE_IMPLEMENT_METHOD

    BLOCKSTORE_STORAGE_SERVICE(STORAGE_IMPLEMENT_METHOD)

#undef STORAGE_IMPLEMENT_METHOD

#define ENDPOINT_IMPLEMENT_METHOD(name, ...)                                   \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        auto timeout = request->GetHeaders().GetRequestTimeout();              \
        auto future = Executor->Execute([                                      \
            ctx = std::move(callContext),                                      \
            req = std::move(request),                                          \
            this] () mutable                                                   \
        {                                                                      \
            return Do##name(std::move(ctx), std::move(req));                   \
        });                                                                    \
        return CreateTimeoutFuture(future, TDuration::MilliSeconds(timeout));  \
    }                                                                          \
// ENDPOINT_IMPLEMENT_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(ENDPOINT_IMPLEMENT_METHOD)

#undef ENDPOINT_IMPLEMENT_METHOD

    TFuture<void> RestoreEndpoints() override
    {
        AtomicSet(RestoringStage, ReadingStorage);
        return Executor->Execute([this] () mutable {
            auto future = DoRestoreEndpoints();
            AtomicSet(RestoringStage, StartingEndpoints);
            Executor->WaitFor(future);
            AtomicSet(RestoringStage, Completed);
        });
    }

private:
    bool IsEndpointRestoring(const TString& socket)
    {
        switch (AtomicGet(RestoringStage)) {
            case WaitingForRestoring: return false;
            case ReadingStorage: return true;
            case StartingEndpoints: return RestoringEndpoints.contains(socket);
            case Completed: return false;
        }
        return false;
    }

    NProto::TStartEndpointResponse DoStartEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request)
    {
        if (IsEndpointRestoring(request->GetUnixSocketPath())) {
            return TErrorResponse(E_REJECTED, "endpoint is restoring now");
        }

        auto req = *request;
        auto future = EndpointManager->StartEndpoint(
            std::move(ctx),
            std::move(request));

        auto response = Executor->WaitFor(future);
        if (HasError(response)) {
            return response;
        }

        if (req.GetPersistent()) {
            AddEndpointToStorage(req);
        }

        return response;
    }

    NProto::TStopEndpointResponse DoStopEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request)
    {
        if (IsEndpointRestoring(request->GetUnixSocketPath())) {
            return TErrorResponse(E_REJECTED, "endpoint is restoring now");
        }

        auto socketPath = request->GetUnixSocketPath();
        auto future = EndpointManager->StopEndpoint(
            std::move(ctx),
            std::move(request));

        auto response = Executor->WaitFor(future);
        if (HasError(response)) {
            return response;
        }

        EndpointStorage->RemoveEndpoint(socketPath);
        return response;
    }

    NProto::TListEndpointsResponse DoListEndpoints(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListEndpointsRequest> request)
    {
        auto future = EndpointManager->ListEndpoints(
            std::move(ctx),
            std::move(request));

        auto response = Executor->WaitFor(future);
        response.SetEndpointsWereRestored(
            AtomicGet(RestoringStage) == Completed);
        return response;
    }

    NProto::TKickEndpointResponse DoKickEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TKickEndpointRequest> request)
    {
        auto [str, error] = EndpointStorage->GetEndpoint(
            ToString(request->GetKeyringId()));

        if (HasError(error)) {
            return TErrorResponse(error);
        }

        auto startReq = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);

        if (!startReq) {
            return TErrorResponse(E_INVALID_STATE, TStringBuilder()
                << "Failed to deserialize endpoint with key "
                << request->GetKeyringId());
        }

        startReq->MutableHeaders()->MergeFrom(request->GetHeaders());

        STORAGE_INFO("Kick StartEndpoint request: " << *startReq);
        auto response = DoStartEndpoint(
            std::move(ctx),
            std::move(startReq));

        return TErrorResponse(response.GetError());
    }

    NProto::TListKeyringsResponse DoListKeyrings(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListKeyringsRequest> request)
    {
        Y_UNUSED(ctx);
        Y_UNUSED(request);

        auto [storedIds, error] = EndpointStorage->GetEndpointIds();
        if (HasError(error)) {
            return TErrorResponse(error);
        }

        NProto::TListKeyringsResponse response;
        auto& endpoints = *response.MutableEndpoints();

        endpoints.Reserve(storedIds.size());

        for (auto keyringId: storedIds) {
            auto& endpoint = *endpoints.Add();
            endpoint.SetKeyringId(keyringId);

            auto [str, error] = EndpointStorage->GetEndpoint(keyringId);
            if (HasError(error)) {
                STORAGE_WARN("Failed to get endpoint from storage, ID: "
                    << keyringId << ", error: " << FormatError(error));
                continue;
            }

            auto req = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);
            if (!req) {
                STORAGE_WARN("Failed to deserialize endpoint from storage, ID: "
                    << keyringId);
                continue;
            }

            endpoint.MutableRequest()->CopyFrom(*req);
        }

        return response;
    }

    NProto::TDescribeEndpointResponse DoDescribeEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TDescribeEndpointRequest> request)
    {
        if (IsEndpointRestoring(request->GetUnixSocketPath())) {
            return TErrorResponse(E_REJECTED, "endpoint is restoring now");
        }

        auto future = EndpointManager->DescribeEndpoint(
            std::move(ctx),
            std::move(request));
        return Executor->WaitFor(future);
    }

    NProto::TRefreshEndpointResponse DoRefreshEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TRefreshEndpointRequest> request)
    {
        if (IsEndpointRestoring(request->GetUnixSocketPath())) {
            return TErrorResponse(E_REJECTED, "endpoint is restoring now");
        }

        auto future = EndpointManager->RefreshEndpoint(
            std::move(ctx),
            std::move(request));
        return Executor->WaitFor(future);
    }

    TFuture<void> DoRestoreEndpoints();

    NProto::TError AddEndpointToStorage(
        const NProto::TStartEndpointRequest& request)
    {
        auto [data, error] = SerializeEndpoint(request);
        if (HasError(error)) {
            return error;
        }

        error = EndpointStorage->AddEndpoint(request.GetUnixSocketPath(), data);
        if (HasError(error)) {
            return error;
        }

        return {};
    }

    void HandleRestoredEndpoint(
        const TString& socketPath,
        const NProto::TError& error)
    {
        if (HasError(error)) {
            STORAGE_ERROR("Failed to start endpoint " << socketPath.Quote()
                << ", error:" << FormatError(error));
        }

        Executor->Execute([socketPath, this] () mutable {
            RestoringEndpoints.erase(socketPath);
        });
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

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TEndpointService::DoRestoreEndpoints()
{
    auto [storedIds, error] = EndpointStorage->GetEndpointIds();
    if (HasError(error)) {
        STORAGE_ERROR("Failed to get endpoints from storage: "
            << FormatError(error));
        ReportEndpointRestoringError();
        return MakeFuture();
    }

    STORAGE_INFO("Found " << storedIds.size() << " endpoints in storage");

    TString clientId = CreateGuidAsString() + "_bootstrap";

    TVector<TFuture<void>> futures;

    for (auto keyringId: storedIds) {
        auto [str, error] = EndpointStorage->GetEndpoint(keyringId);
        if (HasError(error)) {
            // NBS-3678
            STORAGE_WARN("Failed to restore endpoint. ID: " << keyringId
                << ", error: " << FormatError(error));
            continue;
        }

        auto request = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);

        if (!request) {
            ReportEndpointRestoringError();
            STORAGE_ERROR("Failed to deserialize request. ID: " << keyringId);
            continue;
        }

        auto& headers = *request->MutableHeaders();

        if (!headers.GetClientId()) {
            headers.SetClientId(clientId);
        }

        auto requestId = headers.GetRequestId();
        if (!requestId) {
            headers.SetRequestId(CreateRequestId());
        }

        auto socketPath = request->GetUnixSocketPath();
        RestoringEndpoints.insert(socketPath);

        auto future = RestoringClient->StartEndpoint(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request));

        auto weakPtr = weak_from_this();
        future.Subscribe([weakPtr, socketPath] (const auto& f) {
            const auto& response = f.GetValue();
            if (HasError(response)) {
                ReportEndpointRestoringError();
            }

            if (auto ptr = weakPtr.lock()) {
                ptr->HandleRestoredEndpoint(socketPath, response.GetError());
            }
        });

        futures.push_back(future.IgnoreResult());
    }

    return WaitAll(futures);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointServicePtr CreateMultipleEndpointService(
    IBlockStorePtr service,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    IEndpointStoragePtr endpointStorage,
    IEndpointManagerPtr endpointManager,
    NProto::TClientConfig clientConfig)
{
    return std::make_shared<TEndpointService>(
        std::move(service),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(requestStats),
        std::move(volumeStats),
        std::move(serverStats),
        std::move(executor),
        std::move(endpointStorage),
        std::move(endpointManager),
        std::move(clientConfig));
}

}   // namespace NCloud::NBlockStore::NServer
