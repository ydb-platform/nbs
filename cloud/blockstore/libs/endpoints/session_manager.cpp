#include "session_manager.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client/switchable_client.h>
#include <cloud/blockstore/libs/client/switchable_session.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_error_transform.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/storage/model/volume_label.h>
#include <cloud/blockstore/libs/validation/validation.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <utility>

namespace NCloud::NBlockStore::NServer {

using namespace NClient;
using namespace NThreading;

namespace {

auto Logging = CreateLoggingService("console");
auto Log = Logging->CreateLog(">>>>>>");

////////////////////////////////////////////////////////////////////////////////

class TEndpoint
{
private:
    TExecutor& Executor;
    ISwitchableSessionPtr SwitchableSession;
    const ISessionPtr Session;
    const IBlockStorePtr DataClient;
    const ISwitchableBlockStorePtr SwitchableDataClient;
    const IThrottlerProviderPtr ThrottlerProvider;
    const TString ClientId;
    const TString DiskId;
    const NProto::TStartEndpointRequest StartRequest;

    std::weak_ptr<TSessionSwitchingGuard> SwitchingGuard;
    TString SessionId;

public:
    TEndpoint(
            TExecutor& executor,
            ISwitchableSessionPtr switchableSession,
            ISessionPtr session,
            IBlockStorePtr dataClient,
            ISwitchableBlockStorePtr switchableDataClient,
            IThrottlerProviderPtr throttlerProvider,
            TString clientId,
            TString diskId,
            NProto::TStartEndpointRequest startRequest)
        : Executor(executor)
        , SwitchableSession(std::move(switchableSession))
        , Session(std::move(session))
        , DataClient(std::move(dataClient))
        , SwitchableDataClient(std::move(switchableDataClient))
        , ThrottlerProvider(std::move(throttlerProvider))
        , ClientId(std::move(clientId))
        , DiskId(std::move(diskId))
        , StartRequest(std::move(startRequest))
    {}

    NProto::TError Start(TCallContextPtr callContext, NProto::THeaders headers)
    {
        STORAGE_ERROR(">>> Start Endpoint");
        DataClient->Start();

        headers.SetClientId(ClientId);
        auto future = Session->MountVolume(std::move(callContext), headers);
        const auto& response = Executor.WaitFor(future);

        if (HasError(response)) {
            DataClient->Stop();
        } else {
            SessionId = response.GetSessionId();
        }

        return response.GetError();
    }

    NProto::TError Stop(TCallContextPtr callContext, NProto::THeaders headers)
    {
        STORAGE_ERROR(">>> Stop Endpoint");
        headers.SetClientId(ClientId);
        auto future = Session->UnmountVolume(std::move(callContext), headers);
        const auto& response = Executor.WaitFor(future);

        DataClient->Stop();
        return response.GetError();
    }

    NProto::TError Alter(
        TCallContextPtr callContext,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        NProto::THeaders headers)
    {
        STORAGE_ERROR(">>> Alter Endpoint");
        headers.SetClientId(ClientId);
        auto future = Session->MountVolume(
            accessMode,
            mountMode,
            mountSeqNumber,
            std::move(callContext),
            headers);
        const auto& response = Executor.WaitFor(future);
        return response.GetError();
    }

    ISessionPtr GetSession() const
    {
        // For consumers, we return the top-level session, which is the
        // SwitchableSession.
        return SwitchableSession;
    }

    TString GetDiskId() const
    {
        return DiskId;
    }

    NProto::TClientPerformanceProfile GetPerformanceProfile() const
    {
        return ThrottlerProvider->GetPerformanceProfile(ClientId);
    }

    NProto::TStartEndpointRequest GetStartRequest() const
    {
        return StartRequest;
    }

    TFuture<void> SwitchSession(TEndpoint& oldEndpoint)
    {
        SwitchableSession = std::move(oldEndpoint.SwitchableSession);

        return SwitchableSession->SwitchSession(
            GetDiskId(),
            SessionId,
            Session,
            SwitchableDataClient);
    }

    // The acquire of the session switching guard must be performed under
    // EndpointLock.
    // If nullptr is returned, then the session switch is already in progress.
    // The caller must keep the guard at all times while the session is being
    // switched.
    [[nodiscard]] TSessionSwitchingGuardPtr AcquireSwitchingGuard()
    {
        if (SwitchingGuard.lock()) {
            // Session switch is already in progress.
            return {};
        }
        auto result = CreateSessionSwitchingGuard(SwitchableDataClient);
        SwitchingGuard = result;
        return result;
    }
};

using TEndpointPtr = std::shared_ptr<TEndpoint>;

////////////////////////////////////////////////////////////////////////////////

class TClientBase: public TBlockStoreImpl<TClientBase, IBlockStore>
{
public:
    TClientBase() = default;

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeFuture<typename TMethod::TResponse>(TErrorResponse(
            E_NOT_IMPLEMENTED,
            TStringBuilder() << "TClientBase: unsupported request "
                             << TMethod::GetName().Quote()));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStorageDataClient final
    : public TClientBase
    , public std::enable_shared_from_this<TStorageDataClient>
{
private:
    const IStoragePtr Storage;
    const IBlockStorePtr Service;
    const IServerStatsPtr ServerStats;
    const TString ClientId;
    const TDuration RequestTimeout;
    const ui32 BlockSize;

public:
    TStorageDataClient(
            IStoragePtr storage,
            IBlockStorePtr service,
            IServerStatsPtr serverStats,
            TString clientId,
            TDuration requestTimeout,
            ui32 blockSize)
        : Storage(std::move(storage))
        , Service(std::move(service))
        , ServerStats(std::move(serverStats))
        , ClientId(std::move(clientId))
        , RequestTimeout(requestTimeout)
        , BlockSize(blockSize)
    {
        STORAGE_ERROR(">>> StorageDataClient Create");
    }

    ~TStorageDataClient()
    {
        STORAGE_ERROR(">>> StorageDataClient Destroy");
    }

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Storage->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, receiver)                            \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        SetBlockSize(*request);                                                \
        PrepareRequestHeaders(*request->MutableHeaders(), *callContext);       \
        return receiver->name(std::move(callContext), std::move(request));     \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_IMPLEMENT_METHOD(ReadBlocksLocal, Storage)
    BLOCKSTORE_IMPLEMENT_METHOD(WriteBlocksLocal, Storage)
    BLOCKSTORE_IMPLEMENT_METHOD(ZeroBlocks, Storage)
    BLOCKSTORE_IMPLEMENT_METHOD(ExecuteAction, Service)

#undef BLOCKSTORE_IMPLEMENT_METHOD

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        STORAGE_ERROR(">>> StorageDataClient MountVolume");
        const TString instanceId = request->GetInstanceId();
        PrepareRequestHeaders(*request->MutableHeaders(), *callContext);
        auto future = Service->MountVolume(
            std::move(callContext),
            std::move(request));

        return future.Apply(
            [=, weakSelf = weak_from_this()](const auto& f)
            {
                auto self = weakSelf.lock();
                if (!self) {
                    return f;
                }

                const auto& response = f.GetValue();

                if (!HasError(response) && response.HasVolume()) {
                    self->ServerStats->MountVolume(
                        response.GetVolume(),
                        self->ClientId,
                        instanceId);
                }
                return f;
            });
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override
    {
        STORAGE_ERROR(">>> StorageDataClient UnmountVolume");
        auto diskId = request->GetDiskId();

        PrepareRequestHeaders(*request->MutableHeaders(), *callContext);
        auto future = Service->UnmountVolume(
            std::move(callContext),
            std::move(request));

        return future.Apply(
            [=, weakSelf = weak_from_this()](const auto& f)
            {
                auto self = weakSelf.lock();
                if (!self) {
                    return f;
                }

                const auto& response = f.GetValue();

                if (!HasError(response)) {
                    self->ServerStats->UnmountVolume(
                        diskId,
                        self->ClientId);
                }
                return f;
            });
    }

private:
    void PrepareRequestHeaders(
        NProto::THeaders& headers,
        const TCallContext& callContext)
    {
        headers.SetClientId(ClientId);

        if (!headers.GetRequestTimeout()) {
            headers.SetRequestTimeout(RequestTimeout.MilliSeconds());
        }

        if (!headers.GetTimestamp()) {
            headers.SetTimestamp(TInstant::Now().MicroSeconds());
        }

        if (!headers.GetRequestId()) {
            headers.SetRequestId(callContext.RequestId);
        }
    }

    template <typename TRequest>
    void SetBlockSize(TRequest& request)
    {
        if constexpr (HasBlockSize<TRequest>) {
            request.BlockSize = BlockSize;
        }
        if constexpr (HasProtoBlockSize<TRequest>) {
            request.SetBlockSize(BlockSize);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSessionManager final
    : public ISessionManager
    , public ISessionSwitcher
    , public std::enable_shared_from_this<TSessionManager>
{
private:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const IRequestStatsPtr RequestStats;
    const IVolumeStatsPtr VolumeStats;
    const IServerStatsPtr ServerStats;
    const IBlockStorePtr Service;
    const NCells::ICellManagerPtr CellManager;
    const IStorageProviderPtr StorageProvider;
    const IThrottlerProviderPtr ThrottlerProvider;
    const IEncryptionClientFactoryPtr EncryptionClientFactory;
    const TExecutorPtr Executor;
    const TSessionManagerOptions Options;

    TLog Log;

    TMutex EndpointLock;
    THashMap<TString, TEndpointPtr> Endpoints;

public:
    TSessionManager(
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            IRequestStatsPtr requestStats,
            IVolumeStatsPtr volumeStats,
            IServerStatsPtr serverStats,
            IBlockStorePtr service,
            NCells::ICellManagerPtr cellManager,
            IStorageProviderPtr storageProvider,
            IThrottlerProviderPtr throttlerProvider,
            IEncryptionClientFactoryPtr encryptionClientFactory,
            TExecutorPtr executor,
            TSessionManagerOptions options)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , RequestStats(std::move(requestStats))
        , VolumeStats(std::move(volumeStats))
        , ServerStats(std::move(serverStats))
        , Service(std::move(service))
        , CellManager(std::move(cellManager))
        , StorageProvider(std::move(storageProvider))
        , ThrottlerProvider(std::move(throttlerProvider))
        , EncryptionClientFactory(std::move(encryptionClientFactory))
        , Executor(std::move(executor))
        , Options(std::move(options))
    {
        Log = Logging->CreateLog("BLOCKSTORE_SERVER");
    }

    // implementation ISessionManager
    TFuture<TSessionOrError> CreateSession(
        TCallContextPtr callContext,
        const NProto::TStartEndpointRequest& request) override;

    TFuture<NProto::TError> RemoveSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers) override;

    TFuture<NProto::TError> AlterSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        const NProto::THeaders& headers) override;

    TFuture<TSessionOrError> GetSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers) override;

    TResultOrError<NProto::TClientPerformanceProfile> GetProfile(
        const TString& socketPath) override;

    // implementation ISessionSwitcher
    void SwitchSession(
        const TString& diskId,
        const TString& newDiskId) override;

private:
    TSessionOrError CreateSessionImpl(
        TCallContextPtr callContext,
        NProto::TStartEndpointRequest request);

    NProto::TError RemoveSessionImpl(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers);

    NProto::TError AlterSessionImpl(
        TCallContextPtr callContext,
        const TString& socketPath,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        const NProto::THeaders& headers);

    TSessionOrError GetSessionImpl(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers);

    NProto::TDescribeVolumeResponse DescribeVolume(
        TCallContextPtr callContext,
        const TString& diskId,
        const NProto::THeaders& headers);

    TResultOrError<TEndpointPtr> CreateEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        const TString& cellId);

    TClientAppConfigPtr CreateClientConfig(
        const NProto::TStartEndpointRequest& request) const;

    TClientAppConfigPtr CreateClientConfig(
        const NProto::TStartEndpointRequest& request,
        TString host,
        ui32 port) const;

    TResultOrError<IBlockStorePtr> CreateStorageDataClient(
        const TString& cellId,
        const TClientAppConfigPtr& clientConfig,
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) const;

    static TSessionConfig CreateSessionConfig(
        const NProto::TStartEndpointRequest& request);

    void SwitchSessionForEndpoint(
        const TString& socketPath,
        TEndpointPtr endpoint,
        const TString& diskId,
        const TString& newDiskId);
    void StopEndpoint(TCallContextPtr callContext, TEndpointPtr oldEndpoint);
    void StopEndpointImpl(
        TCallContextPtr callContext,
        TEndpointPtr oldEndpoint);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<TSessionManager::TSessionOrError> TSessionManager::CreateSession(
    TCallContextPtr callContext,
    const NProto::TStartEndpointRequest& request)
{
    return Executor->Execute(
        [callContext = std::move(callContext), request, this] () mutable
        {
            return CreateSessionImpl(std::move(callContext), request);
        }
    );
}

TSessionManager::TSessionOrError TSessionManager::CreateSessionImpl(
    TCallContextPtr callContext,
    NProto::TStartEndpointRequest request)
{
    STORAGE_ERROR(">>> SessionManager CreateSession " << request.GetDiskId());
    auto describeResponse = DescribeVolume(
        callContext,
        request.GetDiskId(),
        request.GetHeaders());
    if (HasError(describeResponse)) {
        return TErrorResponse(describeResponse.GetError());
    }
    const auto& volume = describeResponse.GetVolume();
    const auto& cellId = describeResponse.GetCellId();

    if (volume.GetDiskId() != request.GetDiskId()) {
        // The original volume no longer exists. Use principal volume instead.
        request.SetDiskId(volume.GetDiskId());
    } else if (volume.GetPrincipalDiskId()) {
        // The original volume has lost leadership. Use principal volume instead.
        request.SetDiskId(volume.GetPrincipalDiskId());
        return CreateSessionImpl(std::move(callContext), std::move(request));
    }

    auto result = CreateEndpoint(request, volume, cellId);
    if (HasError(result)) {
        return TErrorResponse(result.GetError());
    }
    const auto& endpoint = result.GetResult();

    auto error = endpoint->Start(std::move(callContext), request.GetHeaders());
    if (HasError(error)) {
        return TErrorResponse(error);
    }

    with_lock (EndpointLock) {
        auto [it, inserted] = Endpoints.emplace(
            request.GetUnixSocketPath(),
            endpoint);
        STORAGE_VERIFY(
            inserted,
            TWellKnownEntityTypes::ENDPOINT,
            request.GetUnixSocketPath());
    }

    return TSessionInfo {
        .Volume = volume,
        .Session = endpoint->GetSession()
    };
}

NProto::TDescribeVolumeResponse TSessionManager::DescribeVolume(
    TCallContextPtr callContext,
    const TString& diskId,
    const NProto::THeaders& headers)
{
    STORAGE_ERROR(">>> SessionManager DescribeVolume " << diskId);
    auto cellDescribeFuture = CellManager->DescribeVolume(
        std::move(callContext),
        diskId,
        headers,
        Service,
        Options.DefaultClientConfig);

    return Executor->WaitFor(cellDescribeFuture);
}

TFuture<NProto::TError> TSessionManager::RemoveSession(
    TCallContextPtr callContext,
    const TString& socketPath,
    const NProto::THeaders& headers)
{
    return Executor->Execute(
        [this, callContext = std::move(callContext), socketPath, headers] () mutable
        {
            return RemoveSessionImpl(std::move(callContext), socketPath, headers);
        }
    );
}

NProto::TError TSessionManager::RemoveSessionImpl(
    TCallContextPtr callContext,
    const TString& socketPath,
    const NProto::THeaders& headers)
{
    TEndpointPtr endpoint;

    with_lock (EndpointLock) {
        auto it = Endpoints.find(socketPath);
        STORAGE_VERIFY(
            it != Endpoints.end(),
            TWellKnownEntityTypes::ENDPOINT,
            socketPath);
        endpoint = std::move(it->second);
        STORAGE_ERROR(
            ">>> SessionManager RemoveSession " << endpoint->GetDiskId());
        Endpoints.erase(it);
    }
    auto error = endpoint->Stop(std::move(callContext), headers);

    endpoint.reset();
    ThrottlerProvider->Clean();

    return error;
}

TFuture<NProto::TError> TSessionManager::AlterSession(
    TCallContextPtr callContext,
    const TString& socketPath,
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    ui64 mountSeqNumber,
    const NProto::THeaders& headers)
{
    return Executor->Execute(
        [this,
         callContext = std::move(callContext),
         socketPath,
         accessMode,
         mountMode,
         mountSeqNumber,
         headers] () mutable
        {
            return AlterSessionImpl(
                std::move(callContext),
                socketPath,
                accessMode,
                mountMode,
                mountSeqNumber,
                headers);
        }
    );
}

NProto::TError TSessionManager::AlterSessionImpl(
    TCallContextPtr callContext,
    const TString& socketPath,
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    ui64 mountSeqNumber,
    const NProto::THeaders& headers)
{
    TEndpointPtr endpoint;

    with_lock (EndpointLock) {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return TErrorResponse(
                E_INVALID_STATE,
                TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " hasn't been started");
        }
        endpoint = it->second;
    }
    STORAGE_ERROR(">>> SessionManager AlterSession " << endpoint->GetDiskId());
    return endpoint->Alter(
        std::move(callContext),
        accessMode,
        mountMode,
        mountSeqNumber,
        headers);
}

TFuture<TSessionManager::TSessionOrError> TSessionManager::GetSession(
    TCallContextPtr callContext,
    const TString& socketPath,
    const NProto::THeaders& headers)
{
    return Executor->Execute(
        [this, callContext = std::move(callContext), socketPath, headers] () mutable
        {
            return GetSessionImpl(
                std::move(callContext),
                socketPath,
                headers);
        }
    );
}

TSessionManager::TSessionOrError TSessionManager::GetSessionImpl(
    TCallContextPtr callContext,
    const TString& socketPath,
    const NProto::THeaders& headers)
{
    TEndpointPtr endpoint;

    with_lock (EndpointLock) {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return TErrorResponse(
                E_INVALID_STATE,
                TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " hasn't been started");
        }
        endpoint = it->second;
    }

    auto describeResponse = DescribeVolume(
        std::move(callContext),
        endpoint->GetDiskId(),
        headers);
    if (HasError(describeResponse)) {
        return TErrorResponse(describeResponse.GetError());
    }

    return TSessionInfo {
        .Volume = describeResponse.GetVolume(),
        .Session = endpoint->GetSession()
    };
}

TResultOrError<NProto::TClientPerformanceProfile> TSessionManager::GetProfile(
    const TString& socketPath)
{
    TEndpointPtr endpoint;

    with_lock (EndpointLock) {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return TErrorResponse(
                E_INVALID_STATE,
                TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " hasn't been started");
        }
        endpoint = it->second;
    }

    return endpoint->GetPerformanceProfile();
}

void TSessionManager::SwitchSession(
    const TString& diskId,
    const TString& newDiskId)
{
    STORAGE_ERROR(
        ">>> SessionManager SwitchSession " << diskId << " -> " << newDiskId);
    bool endpointFound = false;
    with_lock (EndpointLock) {
        for (const auto& [socketPath, endpoint]: Endpoints) {
            if (endpoint->GetDiskId() != diskId) {
                continue;
            }
            endpointFound = true;

            auto switchingGuard = endpoint->AcquireSwitchingGuard();
            if (!switchingGuard) {
                STORAGE_INFO(
                    "Session swithing in progress: " << diskId.Quote() << " -> "
                                                     << newDiskId.Quote());
                continue;
            }

            Executor->Execute(
                [socketPath = socketPath,
                 endpoint = endpoint,
                 switchingGuard = std::move(switchingGuard),
                 diskId = diskId,
                 newDiskId = newDiskId,
                 weakSelf = weak_from_this()]() mutable
                {
                    if (auto self = weakSelf.lock()) {
                        self->SwitchSessionForEndpoint(
                            socketPath,
                            std::move(endpoint),
                            diskId,
                            newDiskId);
                    }
                    switchingGuard.reset();
                });
        }
    }

    if (!endpointFound) {
        STORAGE_WARN(
            "Session for " << diskId.Quote() << " not found. Can't switch to "
                           << newDiskId.Quote());
    }
}

TResultOrError<IBlockStorePtr> TSessionManager::CreateStorageDataClient(
    const TString& cellId,
    const TClientAppConfigPtr& clientConfig,
    const NProto::TVolume& volume,
    const TString& clientId,
    NProto::EVolumeAccessMode accessMode) const
{
    auto service = Service;
    IStoragePtr storage;

    if (!cellId.empty()) {
        auto result = CellManager->GetCellEndpoint(
            cellId,
            clientConfig);
        if (HasError(result)) {
            return result.GetError();
        }

        service = result.GetResult().GetService();
        storage = result.GetResult().GetStorage();
    } else {
        auto future =
            StorageProvider->CreateStorage(volume, clientId, accessMode);

        storage = Executor->ResultOrError(future).GetResult();
    }

    return {
        std::make_shared<TStorageDataClient>(
            std::move(storage),
            std::move(service),
            ServerStats,
            clientId,
            clientConfig->GetRequestTimeout(),
            volume.GetBlockSize())
    };
}

TResultOrError<TEndpointPtr> TSessionManager::CreateEndpoint(
    const NProto::TStartEndpointRequest& request,
    const NProto::TVolume& volume,
    const TString& cellId)
{
    STORAGE_ERROR(">>> SessionManager CreateEndpoint " << volume.GetDiskId());

    const auto& clientId = request.GetClientId();
    auto accessMode = request.GetVolumeAccessMode();

    auto clientConfig = CreateClientConfig(request);

    auto [client, error] = CreateStorageDataClient(
        cellId,
        clientConfig,
        volume,
        clientId,
        accessMode);

    if (HasError(error)) {
        return error;
    }

    auto switchableClient = CreateSwitchableClient(
        Logging,
        weak_from_this(),
        volume.GetDiskId(),
        std::move(client));
    client = switchableClient;

    if (Options.TemporaryServer) {
        client = CreateErrorTransformService(
            std::move(client),
            {{ EErrorKind::ErrorFatal, E_REJECTED }});
    }

    if (Options.EnableDataIntegrityClient ||
        FindPtr(
            Options.MediaKindsToValidateDataIntegrity,
            volume.GetStorageMediaKind()) != nullptr)
    {
        client = CreateDataIntegrityClient(
            Logging,
            Monitoring,
            std::move(client),
            volume);
    }

    auto retryPolicy =
        CreateRetryPolicy(clientConfig, volume.GetStorageMediaKind());

    if (!Options.DisableDurableClient
            && (volume.GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD_LOCAL
                || !clientConfig->GetLocalNonreplDisableDurableClient()))
    {
        client = CreateDurableClient(
            clientConfig,
            std::move(client),
            std::move(retryPolicy),
            Logging,
            Timer,
            Scheduler,
            RequestStats,
            VolumeStats);
    } else {
        STORAGE_WARN("disable durable client for " << volume.GetDiskId().Quote());
    }

    auto encryptionFuture = EncryptionClientFactory->CreateEncryptionClient(
        std::move(client),
        request.GetEncryptionSpec(),
        NStorage::GetLogicalDiskId(request.GetDiskId()));

    const auto& clientOrError = Executor->WaitFor(encryptionFuture);
    if (HasError(clientOrError)) {
        return clientOrError.GetError();
    }
    client = clientOrError.GetResult();

    if (!Options.DisableClientThrottler) {
        auto throttler = ThrottlerProvider->GetThrottler(
            clientConfig->GetClientConfig(),
            request.GetClientProfile(),
            request.GetClientPerformanceProfile());

        if (throttler) {
            client =
                CreateThrottlingClient(std::move(client), std::move(throttler));
        }
    }

    if (Options.StrictContractValidation &&
        // switching fast path to slow path during migration might lead to
        // validation false positives
        !volume.GetTags().contains(UseFastPathTagName))
    {
        client = CreateValidationClient(
            Logging,
            Monitoring,
            std::move(client),
            CreateCrcDigestCalculator());
    }

    STORAGE_ERROR(
        ">>> SessionManager CreateEndpoint > CreateSession "
        << volume.GetDiskId());
    auto session = NClient::CreateSession(
        Timer,
        Scheduler,
        Logging,
        RequestStats,
        VolumeStats,
        client,
        std::move(clientConfig),
        CreateSessionConfig(request));

    auto switchableSession = CreateSwitchableSession(
        Logging,
        Scheduler,
        volume.GetDiskId(),
        session,
        switchableClient);

    return std::make_shared<TEndpoint>(
        *Executor,
        std::move(switchableSession),
        std::move(session),
        std::move(client),
        std::move(switchableClient),
        ThrottlerProvider,
        clientId,
        volume.GetDiskId(),
        request);
}

TClientAppConfigPtr TSessionManager::CreateClientConfig(
    const NProto::TStartEndpointRequest& request,
    TString host,
    ui32 port) const
{
    NProto::TClientAppConfig clientAppConfig;
    auto& config = *clientAppConfig.MutableClientConfig();

    config = Options.DefaultClientConfig;
    config.SetClientId(request.GetClientId());
    config.SetInstanceId(request.GetInstanceId());
    config.SetHost(host);
    config.SetPort(port);

    return std::make_shared<TClientAppConfig>(std::move(clientAppConfig));
}

TClientAppConfigPtr TSessionManager::CreateClientConfig(
    const NProto::TStartEndpointRequest& request) const
{
    NProto::TClientAppConfig clientAppConfig;
    auto& config = *clientAppConfig.MutableClientConfig();

    config = Options.DefaultClientConfig;
    config.SetClientId(request.GetClientId());
    config.SetInstanceId(request.GetInstanceId());

    return std::make_shared<TClientAppConfig>(std::move(clientAppConfig));
}

// static
TSessionConfig TSessionManager::CreateSessionConfig(
    const NProto::TStartEndpointRequest& request)
{
    TSessionConfig config;
    config.DiskId = request.GetDiskId();
    config.InstanceId = request.GetInstanceId();
    config.AccessMode = request.GetVolumeAccessMode();
    config.MountMode = request.GetVolumeMountMode();
    config.MountFlags = request.GetMountFlags();
    config.IpcType = request.GetIpcType();
    config.ClientVersionInfo = request.GetClientVersionInfo();
    config.MountSeqNumber = request.GetMountSeqNumber();
    return config;
}

void TSessionManager::SwitchSessionForEndpoint(
    const TString& socketPath,
    TEndpointPtr endpoint,
    const TString& diskId,
    const TString& newDiskId)
{
    STORAGE_INFO(
        "Start session switching: " << diskId.Quote() << " -> "
                                    << newDiskId.Quote());

    auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());

    // Prepare request for new endpoint
    auto newStartRequest = endpoint->GetStartRequest();
    newStartRequest.SetDiskId(newDiskId);

    // Describe new disk
    auto describeResponse =
        DescribeVolume(callContext, newDiskId, newStartRequest.GetHeaders());
    if (HasError(describeResponse)) {
        STORAGE_WARN(
            "Describe volume " << newDiskId.Quote() << " failed: "
                               << describeResponse.GetError().GetMessage());
        return;
    }

    // Start new endpoint
    auto result = CreateEndpoint(
        newStartRequest,
        describeResponse.GetVolume(),
        describeResponse.GetCellId());

    if (HasError(result)) {
        STORAGE_WARN(
            "Can't create new Session for " << newDiskId.Quote() << " "
                                            << FormatError(result.GetError()));
        return;
    }

    auto newEndpoint = std::move(result.ExtractResult());
    auto error = newEndpoint->Start(
        callContext,
        newEndpoint->GetStartRequest().GetHeaders());

    if (HasError(error)) {
        STORAGE_WARN(
            "Can't start new endpoint for " << newEndpoint->GetDiskId().Quote()
                                            << " " << FormatError(error));
        return;
    }

    // Switch to new endpoint
    with_lock (EndpointLock) {
        if (!Endpoints.contains(socketPath)) {
            STORAGE_WARN("Endpoint " << socketPath.Quote() << " not found");
            return;
        }

        if (Endpoints[socketPath] != endpoint) {
            STORAGE_WARN(
                "Endpoint on " << socketPath.Quote()
                               << " is not the same as before");
            return;
        }

        Endpoints[socketPath] = newEndpoint;
        auto future = newEndpoint->SwitchSession(*endpoint);
        future.Subscribe(
            [callContext = std::move(callContext),
             oldEndpoint = std::move(endpoint),
             weakSelf = weak_from_this()](const TFuture<void>& future) mutable
            {
                Y_UNUSED(future);
                // Stop old endpoint when switch finished
                if (auto self = weakSelf.lock()) {
                    self->StopEndpoint(
                        std::move(callContext),
                        std::move(oldEndpoint));
                }
            });
    }

    ThrottlerProvider->Clean();
}

void TSessionManager::StopEndpoint(
    TCallContextPtr callContext,
    TEndpointPtr oldEndpoint)
{
    Executor->Execute(
        [callContext = std::move(callContext),
         oldEndpoint = std::move(oldEndpoint),
         weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->StopEndpointImpl(callContext, oldEndpoint);
            }
        });
}

void TSessionManager::StopEndpointImpl(
    TCallContextPtr callContext,
    TEndpointPtr oldEndpoint)
{
    STORAGE_ERROR(
        ">>> SessionManager StopEndpoint " << oldEndpoint->GetDiskId());

    auto error = oldEndpoint->Stop(
        std::move(callContext),
        oldEndpoint->GetStartRequest().GetHeaders());

    if (HasError(error)) {
        STORAGE_WARN(
            "Stop old endpoint for " << oldEndpoint->GetDiskId().Quote()
                                     << "error :" << FormatError(error));
    } else {
        STORAGE_INFO(
            "Stop old endpoint for " << oldEndpoint->GetDiskId().Quote()
                                     << " success");
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISessionManagerPtr CreateSessionManager(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    NCells::ICellManagerPtr cellManager,
    IStorageProviderPtr storageProvider,
    IEncryptionClientFactoryPtr encryptionClientFactory,
    TExecutorPtr executor,
    TSessionManagerOptions options)
{
    auto throttlerProvider = CreateThrottlerProvider(
        options.HostProfile,
        logging,
        timer,
        scheduler,
        monitoring->GetCounters()->GetSubgroup("counters", "blockstore"),
        requestStats,
        volumeStats);

    return std::make_shared<TSessionManager>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(requestStats),
        std::move(volumeStats),
        std::move(serverStats),
        std::move(service),
        std::move(cellManager),
        std::move(storageProvider),
        std::move(throttlerProvider),
        std::move(encryptionClientFactory),
        std::move(executor),
        std::move(options));
}

}   // namespace NCloud::NBlockStore::NServer
