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
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/keyring/endpoints.h>

#include <util/generic/guid.h>
#include <util/generic/map.h>
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

struct TLockState
{
    struct TStartingEndpointState
    {
        NProto::TStartEndpointRequest Request;
        TFuture<NProto::TStartEndpointResponse> Result;
    };

    TMutex ProcessingLock;
    THashMap<TString, TStartingEndpointState> StartingSockets;
    THashMap<TString, TFuture<NProto::TStopEndpointResponse>> StoppingSockets;
};

////////////////////////////////////////////////////////////////////////////////

class TLockService final
    : public TServiceWrapper<IBlockStore>
{
private:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    std::shared_ptr<TLockState> LockState;

public:
    TLockService(
            IBlockStorePtr service,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            std::shared_ptr<TLockState> lockState)
        : TServiceWrapper(std::move(service))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , LockState(std::move(lockState))
    {}

    TFuture<NProto::TStartEndpointResponse> StartEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request) override
    {
        auto timeout = request->GetHeaders().GetRequestTimeout();
        auto future = StartEndpointImpl(std::move(ctx), std::move(request));
        return CreateTimeoutFuture(future, TDuration::MilliSeconds(timeout));
    }

    TFuture<NProto::TStopEndpointResponse> StopEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request) override
    {
        auto timeout = request->GetHeaders().GetRequestTimeout();
        auto future = StopEndpointImpl(std::move(ctx), std::move(request));
        return CreateTimeoutFuture(future, TDuration::MilliSeconds(timeout));
    }

private:
    TFuture<NProto::TStartEndpointResponse> StartEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request);

    TFuture<NProto::TStopEndpointResponse> StopEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request);

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

    template <typename T>
    static void RemoveProcessingSocket(
        TMutex& lock,
        THashMap<TString, T>& socketMap,
        const TString& socket)
    {
        with_lock (lock) {
            auto it = socketMap.find(socket);
            STORAGE_VERIFY(
                it != socketMap.end(),
                TWellKnownEntityTypes::ENDPOINT,
                socket);
            socketMap.erase(it);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TStartEndpointResponse> TLockService::StartEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStartEndpointRequest> request)
{
    TFuture<NProto::TStartEndpointResponse> result;

    auto socketPath = request->GetUnixSocketPath();
    auto& l = *LockState;

    with_lock (l.ProcessingLock) {
        if (l.StoppingSockets.find(socketPath) != l.StoppingSockets.end()) {
            auto response = TErrorResponse(
                E_REJECTED,
                TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " is stopping now");
            return MakeFuture<NProto::TStartEndpointResponse>(std::move(response));
        }

        auto it = l.StartingSockets.find(socketPath);
        if (it != l.StartingSockets.end()) {
            const auto& state = it->second;
            if (IsSameStartEndpointRequests(*request, state.Request)) {
                return state.Result;
            }

            auto response = TErrorResponse(
                E_REJECTED,
                TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " is starting now with other args");
            return MakeFuture<NProto::TStartEndpointResponse>(std::move(response));
        }

        TLockState::TStartingEndpointState state;
        state.Request = *request;
        state.Result = TServiceWrapper::StartEndpoint(
            std::move(ctx),
            std::move(request));

        result = state.Result;
        l.StartingSockets.emplace(socketPath, std::move(state));
    }

    return result.Apply([lockState = LockState, socketPath] (const auto& f) {
        RemoveProcessingSocket(
            lockState->ProcessingLock,
            lockState->StartingSockets,
            socketPath);
        return f.GetValue();
    });
}

TFuture<NProto::TStopEndpointResponse> TLockService::StopEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStopEndpointRequest> request)
{
    TFuture<NProto::TStopEndpointResponse> result;

    auto socketPath = request->GetUnixSocketPath();
    auto& l = *LockState;

    with_lock (l.ProcessingLock) {
        if (l.StartingSockets.find(socketPath) != l.StartingSockets.end()) {
            auto response = TErrorResponse(
                E_REJECTED,
                TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " is starting now");
            return MakeFuture<NProto::TStopEndpointResponse>(std::move(response));
        }

        auto it = l.StoppingSockets.find(socketPath);
        if (it != l.StoppingSockets.end()) {
            return it->second;
        }

        result = TServiceWrapper::StopEndpoint(
            std::move(ctx),
            std::move(request));

        l.StoppingSockets.emplace(socketPath, result);
    }

    return result.Apply([lockState = LockState, socketPath] (const auto& f) {
        RemoveProcessingSocket(
            lockState->ProcessingLock,
            lockState->StoppingSockets,
            socketPath);
        return f.GetValue();
    });
}

////////////////////////////////////////////////////////////////////////////////

class TRestoringService final
    : public TServiceWrapper<IEndpointService>
    , public std::enable_shared_from_this<TRestoringService>
{
private:
    const NClient::IMetricClientPtr RestoringClient;
    const IEndpointStoragePtr EndpointStorage;

    TLog Log;

    TAtomic Restored = 0;

public:
    TRestoringService(
            IBlockStorePtr service,
            ILoggingServicePtr logging,
            IEndpointStoragePtr endpointStorage,
            NClient::IMetricClientPtr restoringClient)
        : TServiceWrapper(std::move(service))
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

    TFuture<NProto::TStartEndpointResponse> StartEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request) override
    {
        auto req = *request;
        auto future = TServiceWrapper::StartEndpoint(
            std::move(ctx),
            std::move(request));

        auto weakPtr = weak_from_this();
        return future.Apply([weakPtr, req] (const auto& f) {
            auto response = f.GetValue();
            if (HasError(response)) {
                return response;
            }

            if (auto ptr = weakPtr.lock()) {
                if (req.GetPersistent()) {
                    ptr->AddEndpointToStorage(req);
                }
            }
            return response;
        });
    }

    TFuture<NProto::TStopEndpointResponse> StopEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request) override
    {
        auto req = *request;
        auto future = TServiceWrapper::StopEndpoint(
            std::move(ctx),
            std::move(request));

        auto weakPtr = weak_from_this();
        return future.Apply([weakPtr, req] (const auto& f) {
            auto response = f.GetValue();
            if (HasError(response)) {
                return response;
            }

            if (auto ptr = weakPtr.lock()) {
                ptr->RemovePersistentEndpointFromStorage(req);
            }
            return response;
        });
    }

    TFuture<NProto::TListEndpointsResponse> ListEndpoints(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListEndpointsRequest> request) override
    {
        auto future = TServiceWrapper::ListEndpoints(
            std::move(ctx),
            std::move(request));

        bool restored = AtomicGet(Restored);

        return future.Apply([restored] (const auto& f) {
            auto response = f.GetValue();
            response.SetEndpointsWereRestored(restored);
            return response;
        });
    }

    TFuture<NProto::TKickEndpointResponse> KickEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TKickEndpointRequest> request) override
    {
        return KickEndpointImpl(
            std::move(ctx),
            std::move(request));
    }

    TFuture<NProto::TListKeyringsResponse> ListKeyrings(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListKeyringsRequest> request) override
    {
        Y_UNUSED(ctx);
        Y_UNUSED(request);

        auto response = ListKeyringsImpl();
        return MakeFuture(std::move(response));
    }

    TFuture<void> RestoreEndpoints() override
    {
        auto weakPtr = weak_from_this();
        return RestoreEndpointsImpl().Apply([weakPtr] (const auto& future) {
            if (auto ptr = weakPtr.lock()) {
                AtomicSet(ptr->Restored, 1);
            }
            return future.GetValue();
        });
    }

private:
    TFuture<NProto::TKickEndpointResponse> KickEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TKickEndpointRequest> request)
    {
        auto requestOrError = EndpointStorage->GetEndpoint(
            request->GetKeyringId());

        if (HasError(requestOrError)) {
            return MakeFuture<NProto::TKickEndpointResponse>(
                TErrorResponse(requestOrError.GetError()));
        }

        auto startRequest = DeserializeEndpoint<NProto::TStartEndpointRequest>(
            requestOrError.GetResult());

        if (!startRequest) {
            NProto::TKickEndpointResponse response;
            *response.MutableError() = MakeError(E_INVALID_STATE, TStringBuilder()
                << "Failed to deserialize endpoint with key "
                << request->GetKeyringId());
            return MakeFuture(std::move(response));
        }

        startRequest->MutableHeaders()->MergeFrom(request->GetHeaders());

        STORAGE_INFO("Kick StartEndpoint request: " << *startRequest);
        auto future = StartEndpoint(
            std::move(ctx),
            std::move(startRequest));

        return future.Apply([] (const auto& f) {
            const auto& startResponse = f.GetValue();

            NProto::TKickEndpointResponse response;
            response.MutableError()->CopyFrom(startResponse.GetError());
            return response;
        });
    }

    NProto::TListKeyringsResponse ListKeyringsImpl();

    TFuture<void> RestoreEndpointsImpl();

    NProto::TError AddEndpointToStorage(
        const NProto::TStartEndpointRequest& request)
    {
        auto [data, error] = SerializeEndpoint(request);
        if (HasError(error)) {
            return error;
        }

        error = EndpointStorage->AddEndpoint(data).GetError();
        if (HasError(error)) {
            return error;
        }

        return {};
    }

    NProto::TError RemovePersistentEndpointFromStorage(
        const NProto::TStopEndpointRequest& request)
    {
        const auto& socketPath = request.GetUnixSocketPath();

        auto [ids, error] = EndpointStorage->GetEndpointIds();
        if (HasError(error)) {
            return error;
        }

        for (auto id: ids) {
            auto [data, error] = EndpointStorage->GetEndpoint(id);
            if (HasError(error)) {
                continue;
            }

            auto req = DeserializeEndpoint<NProto::TStartEndpointRequest>(data);
            if (req && req->GetUnixSocketPath() == socketPath) {
                if (req->GetPersistent()) {
                    return EndpointStorage->RemoveEndpoint(id);
                }
                break;
            }
        }

        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "Couldn't find endpoint " << socketPath);
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TListKeyringsResponse TRestoringService::ListKeyringsImpl()
{
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

        auto requestOrError = EndpointStorage->GetEndpoint(keyringId);
        if (HasError(requestOrError)) {
            continue;
        }

        auto request = DeserializeEndpoint<NProto::TStartEndpointRequest>(
            requestOrError.GetResult());

        if (!request) {
            continue;
        }

        endpoint.MutableRequest()->CopyFrom(*request);
    }

    return response;
}

TFuture<void> TRestoringService::RestoreEndpointsImpl()
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

        auto future = RestoringClient->StartEndpoint(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request));

        future.Subscribe([=] (const auto& f) {
            const auto& response = f.GetValue();
            if (HasError(response)) {
                ReportEndpointRestoringError();
                STORAGE_ERROR("Failed to start endpoint " << socketPath.Quote()
                    << ", error:" << FormatError(response.GetError()));
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
    IEndpointStoragePtr endpointStorage,
    IEndpointManagerPtr endpointManager,
    NProto::TClientConfig clientConfig)
{
    auto endpointService = std::make_shared<TServiceAdapter>(
        std::move(service),
        std::move(endpointManager));

    auto lockState = std::make_shared<TLockState>();

    auto safeEndpointService = std::make_shared<TLockService>(
        endpointService,
        timer,
        scheduler,
        lockState);

    NProto::TClientAppConfig clientAppConfig;
    *clientAppConfig.MutableClientConfig() = std::move(clientConfig);
    auto appConfig = std::make_shared<NClient::TClientAppConfig>(
        std::move(clientAppConfig));

    auto client = CreateDurableClient(
        appConfig,
        std::move(endpointService),
        CreateRetryPolicy(appConfig),
        logging,
        timer,
        scheduler,
        std::move(requestStats),
        std::move(volumeStats));

    auto safeClient = std::make_shared<TLockService>(
        std::move(client),
        std::move(timer),
        std::move(scheduler),
        std::move(lockState));

    auto restoringClient = NClient::CreateMetricClient(
        std::move(safeClient),
        logging,
        std::move(serverStats));

    return std::make_shared<TRestoringService>(
        std::move(safeEndpointService),
        std::move(logging),
        std::move(endpointStorage),
        std::move(restoringClient));
}

}   // namespace NCloud::NBlockStore::NServer
