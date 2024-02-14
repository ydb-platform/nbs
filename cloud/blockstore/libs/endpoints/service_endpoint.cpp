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

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TService>
class TServiceWrapper
    : public TService
{
private:
    IBlockStorePtr Service;

public:
    TServiceWrapper(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    void Start() override
    {
        if (Service) {
            Service->Start();
        }
    }

    void Stop() override
    {
        if (Service) {
            Service->Stop();
        }
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return Service->name(std::move(ctx), std::move(request));              \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TServiceAdapter final
    : public TServiceWrapper<IBlockStore>
{
private:
    const IEndpointManagerPtr EndpointManager;

public:
    TServiceAdapter(
            IBlockStorePtr service,
            IEndpointManagerPtr endpointManager)
        : TServiceWrapper(std::move(service))
        , EndpointManager(std::move(endpointManager))
    {}

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
    : public TServiceWrapper<IEndpointService>
    , public std::enable_shared_from_this<TEndpointService>
{
private:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const TExecutorPtr Executor;
    const NClient::IMetricClientPtr RestoringClient;
    const IEndpointStoragePtr EndpointStorage;

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
            TExecutorPtr executor,
            IEndpointStoragePtr endpointStorage,
            NClient::IMetricClientPtr restoringClient)
        : TServiceWrapper(std::move(service))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Executor(std::move(executor))
        , RestoringClient(std::move(restoringClient))
        , EndpointStorage(std::move(endpointStorage))
    {
        Log = logging->CreateLog("BLOCKSTORE_SERVER");
    }

    void Start() override
    {
        RestoringClient->Start();
    }

    void Stop() override
    {
        RestoringClient->Stop();
    }

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override
    {
        return RestoringClient->CollectRequests(collector);
    }

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
        auto future = TServiceWrapper::StartEndpoint(
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

        auto req = *request;
        auto future = TServiceWrapper::StopEndpoint(
            std::move(ctx),
            std::move(request));

        auto response = Executor->WaitFor(future);
        if (HasError(response)) {
            return response;
        }

        EndpointStorage->RemoveEndpoint(req.GetUnixSocketPath());
        return response;
    }

    NProto::TListEndpointsResponse DoListEndpoints(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListEndpointsRequest> request)
    {
        auto future = TServiceWrapper::ListEndpoints(
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
        auto requestOrError = EndpointStorage->GetEndpoint(
            ToString(request->GetKeyringId()));

        if (HasError(requestOrError)) {
            return TErrorResponse(requestOrError.GetError());
        }

        auto startRequest = DeserializeEndpoint<NProto::TStartEndpointRequest>(
            requestOrError.GetResult());

        if (!startRequest) {
            return TErrorResponse(E_INVALID_STATE, TStringBuilder()
                << "Failed to deserialize endpoint with key "
                << request->GetKeyringId());
        }

        startRequest->MutableHeaders()->MergeFrom(request->GetHeaders());

        STORAGE_INFO("Kick StartEndpoint request: " << *startRequest);
        auto response = DoStartEndpoint(
            std::move(ctx),
            std::move(startRequest));

        return TErrorResponse(response.GetError());
    }

    NProto::TListKeyringsResponse DoListKeyrings(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListKeyringsRequest> request)
    {
        Y_UNUSED(ctx);
        Y_UNUSED(request);

        auto idsOrError = EndpointStorage->GetEndpointIds();
        if (HasError(idsOrError)) {
            return TErrorResponse(idsOrError.GetError());
        }

        NProto::TListKeyringsResponse response;
        auto& endpoints = *response.MutableEndpoints();

        const auto& storedIds = idsOrError.GetResult();
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

        auto future = TServiceWrapper::DescribeEndpoint(
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

        auto future = TServiceWrapper::RefreshEndpoint(
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
    auto idsOrError = EndpointStorage->GetEndpointIds();
    if (HasError(idsOrError)) {
        STORAGE_ERROR("Failed to get endpoints from storage: "
            << FormatError(idsOrError.GetError()));
        ReportEndpointRestoringError();
        return MakeFuture();
    }

    const auto& storedIds = idsOrError.GetResult();
    STORAGE_INFO("Found " << storedIds.size() << " endpoints in storage");

    TString clientId = CreateGuidAsString() + "_bootstrap";

    TVector<TFuture<void>> futures;

    for (auto keyringId: storedIds) {
        auto requestOrError = EndpointStorage->GetEndpoint(keyringId);
        if (HasError(requestOrError)) {
            // NBS-3678
            STORAGE_WARN("Failed to restore endpoint. ID: " << keyringId
                << ", error: " << FormatError(requestOrError.GetError()));
            continue;
        }

        auto request = DeserializeEndpoint<NProto::TStartEndpointRequest>(
            requestOrError.GetResult());

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
    auto endpointService = std::make_shared<TServiceAdapter>(
        std::move(service),
        std::move(endpointManager));

    NProto::TClientAppConfig clientAppConfig;
    *clientAppConfig.MutableClientConfig() = std::move(clientConfig);
    auto appConfig = std::make_shared<NClient::TClientAppConfig>(
        std::move(clientAppConfig));

    auto client = CreateDurableClient(
        appConfig,
        endpointService,
        CreateRetryPolicy(appConfig),
        logging,
        timer,
        scheduler,
        std::move(requestStats),
        std::move(volumeStats));

    auto restoringClient = NClient::CreateMetricClient(
        std::move(client),
        logging,
        std::move(serverStats));

    return std::make_shared<TEndpointService>(
        std::move(endpointService),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(executor),
        std::move(endpointStorage),
        std::move(restoringClient));
}

}   // namespace NCloud::NBlockStore::NServer
