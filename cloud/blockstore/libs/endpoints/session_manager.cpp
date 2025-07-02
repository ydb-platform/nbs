#include "session_manager.h"

#include <cloud/blockstore/libs/cells/iface/cells.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_error_transform.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/validation/validation.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <util/generic/hash.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NServer {

using namespace NClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEndpoint
{
private:
    TExecutor& Executor;
    const ISessionPtr Session;
    const IBlockStorePtr DataClient;
    const IThrottlerProviderPtr ThrottlerProvider;
    const TString ClientId;
    const TString DiskId;

public:
    TEndpoint(
            TExecutor& executor,
            ISessionPtr session,
            IBlockStorePtr dataClient,
            IThrottlerProviderPtr throttlerProvider,
            TString clientId,
            TString diskId)
        : Executor(executor)
        , Session(std::move(session))
        , DataClient(std::move(dataClient))
        , ThrottlerProvider(std::move(throttlerProvider))
        , ClientId(std::move(clientId))
        , DiskId(std::move(diskId))
    {}

    NProto::TError Start(TCallContextPtr callContext, NProto::THeaders headers)
    {
        DataClient->Start();

        headers.SetClientId(ClientId);
        auto future = Session->MountVolume(std::move(callContext), headers);
        const auto& response = Executor.WaitFor(future);

        if (HasError(response)) {
            DataClient->Stop();
        }

        return response.GetError();
    }

    NProto::TError Stop(TCallContextPtr callContext, NProto::THeaders headers)
    {
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

    ISessionPtr GetSession()
    {
        return Session;
    }

    TString GetDiskId()
    {
        return DiskId;
    }

    NProto::TClientPerformanceProfile GetPerformanceProfile()
    {
        return ThrottlerProvider->GetPerformanceProfile(ClientId);
    }
};

using TEndpointPtr = std::shared_ptr<TEndpoint>;

////////////////////////////////////////////////////////////////////////////////

class TClientBase
    : public IBlockStore
{
public:
    TClientBase() = default;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        const auto& type = GetBlockStoreRequestName(EBlockStoreRequest::name); \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED,                                                 \
            TStringBuilder() << "Unsupported request " << type.Quote()));      \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
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
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Storage->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        SetupBlockSize(*request);                                              \
        PrepareRequestHeaders(*request->MutableHeaders(), *callContext);       \
        return Storage->name(std::move(callContext), std::move(request));      \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_IMPLEMENT_METHOD(ReadBlocksLocal)
    BLOCKSTORE_IMPLEMENT_METHOD(WriteBlocksLocal)
    BLOCKSTORE_IMPLEMENT_METHOD(ZeroBlocks)

#undef BLOCKSTORE_IMPLEMENT_METHOD

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
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
    void SetupBlockSize(TRequest& request)
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
    const NCells::ICellsManagerPtr CellsManager;
    const IStorageProviderPtr StorageProvider;
    const NRdma::IClientPtr RdmaClient;
    const IThrottlerProviderPtr ThrottlerProvider;
    const IEncryptionClientFactoryPtr EncryptionClientFactory;
    const TExecutorPtr Executor;
    const TSessionManagerOptions Options;
    const TServerAppConfigPtr Config;

    TLog Log;

    TMutex EndpointLock;
    THashMap<TString, TEndpointPtr> Endpoints;

    IBlockStorePtr ClientEndpoint;

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
            NCells::ICellsManagerPtr cellsManager,
            IStorageProviderPtr storageProvider,
            NRdma::IClientPtr rdmaClient,
            IThrottlerProviderPtr throttlerProvider,
            IEncryptionClientFactoryPtr encryptionClientFactory,
            TExecutorPtr executor,
            TSessionManagerOptions options,
            TServerAppConfigPtr config)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , RequestStats(std::move(requestStats))
        , VolumeStats(std::move(volumeStats))
        , ServerStats(std::move(serverStats))
        , Service(std::move(service))
        , CellsManager(std::move(cellsManager))
        , StorageProvider(std::move(storageProvider))
        , RdmaClient(std::move(rdmaClient))
        , ThrottlerProvider(std::move(throttlerProvider))
        , EncryptionClientFactory(std::move(encryptionClientFactory))
        , Executor(std::move(executor))
        , Options(std::move(options))
        , Config(std::move(config))
    {
        Log = Logging->CreateLog("BLOCKSTORE_SERVER");
    }

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

private:
    TSessionOrError CreateSessionImpl(
        TCallContextPtr callContext,
        const NProto::TStartEndpointRequest& request);

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
        const TString& suId);

    TClientAppConfigPtr CreateClientConfig(
        const NProto::TStartEndpointRequest& request);

    TClientAppConfigPtr CreateClientConfig(
        const NProto::TStartEndpointRequest& request,
        TString host,
        ui32 port);

    static TSessionConfig CreateSessionConfig(
        const NProto::TStartEndpointRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<TSessionManager::TSessionOrError> TSessionManager::CreateSession(
    TCallContextPtr callContext,
    const NProto::TStartEndpointRequest& request)
{
    return Executor->Execute([=] () mutable {
        return CreateSessionImpl(std::move(callContext), request);
    });
}

TSessionManager::TSessionOrError TSessionManager::CreateSessionImpl(
    TCallContextPtr callContext,
    const NProto::TStartEndpointRequest& request)
{
    auto describeResponse = DescribeVolume(
        callContext,
        request.GetDiskId(),
        request.GetHeaders());
    if (HasError(describeResponse)) {
        return TErrorResponse(describeResponse.GetError());
    }
    const auto& volume = describeResponse.GetVolume();
    const auto& suId = describeResponse.GetCellId();

    auto result = CreateEndpoint(request, volume, suId);
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
    auto multiShardFuture = CellsManager->DescribeVolume(
        diskId,
        headers,
        Service,
        Options.DefaultClientConfig);

    if (multiShardFuture.has_value()) {
        return Executor->WaitFor(multiShardFuture.value());
    }

    auto describeRequest = std::make_shared<NProto::TDescribeVolumeRequest>();
    describeRequest->MutableHeaders()->CopyFrom(headers);
    describeRequest->SetDiskId(diskId);

    auto future = Service->DescribeVolume(
        std::move(callContext),
        std::move(describeRequest));

    return Executor->WaitFor(future);
}

TFuture<NProto::TError> TSessionManager::RemoveSession(
    TCallContextPtr callContext,
    const TString& socketPath,
    const NProto::THeaders& headers)
{
    return Executor->Execute([=] () mutable {
        return RemoveSessionImpl(std::move(callContext), socketPath, headers);
    });
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
    return Executor->Execute([=] () mutable {
        return AlterSessionImpl(
            std::move(callContext),
            socketPath,
            accessMode,
            mountMode,
            mountSeqNumber,
            headers);
    });
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
    return Executor->Execute([=] () mutable {
        return GetSessionImpl(
            std::move(callContext),
            socketPath,
            headers);
    });
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

TResultOrError<TEndpointPtr> TSessionManager::CreateEndpoint(
    const NProto::TStartEndpointRequest& request,
    const NProto::TVolume& volume,
    const TString& shardId)
{
    const auto clientId = request.GetClientId();
    auto accessMode = request.GetVolumeAccessMode();

    auto service = Service;
    IStoragePtr storage;

    auto clientConfig = CreateClientConfig(request);

    if (!shardId.empty()) {
        auto result = CellsManager->GetCellEndpoint(
            shardId,
            clientConfig);
        if (HasError(result.GetError())) {
            return result.GetError();
        }

        service = result.GetResult().GetService();
        storage = result.GetResult().GetStorage();
    } else {
        auto future = StorageProvider->CreateStorage(volume, clientId, accessMode)
            .Apply([] (const auto& f) {
                auto storage = f.GetValue();
                // TODO: StorageProvider should return TResultOrError<IStoragePtr>
                return TResultOrError(std::move(storage));
            });

        const auto& storageResult = Executor->WaitFor(future);
        storage = storageResult.GetResult();

        clientConfig = CreateClientConfig(request);

        service = Service;
    }

    IBlockStorePtr client = std::make_shared<TStorageDataClient>(
        std::move(storage),
        std::move(service),
        ServerStats,
        clientId,
        clientConfig->GetRequestTimeout(),
        volume.GetBlockSize());

    if (Options.TemporaryServer) {
        client = CreateErrorTransformService(
            std::move(client),
            {{ EErrorKind::ErrorFatal, E_REJECTED }});
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
        request.GetDiskId());

    const auto& clientOrError = Executor->WaitFor(encryptionFuture);
    if (HasError(clientOrError)) {
        return clientOrError.GetError();
    }
    client = clientOrError.GetResult();

    auto throttler = ThrottlerProvider->GetThrottler(
        clientConfig->GetClientConfig(),
        request.GetClientProfile(),
        request.GetClientPerformanceProfile());

    if (throttler) {
        client = CreateThrottlingClient(
            std::move(client),
            std::move(throttler));
    }

    if (Options.StrictContractValidation &&
        !volume.GetIsFastPathEnabled()   // switching fast path to slow path
                                         // during migration might lead to
                                         // validation false positives
    )
    {
        client = CreateValidationClient(
            Logging,
            Monitoring,
            std::move(client),
            CreateCrcDigestCalculator());
    }

    auto session = NClient::CreateSession(
        Timer,
        Scheduler,
        Logging,
        RequestStats,
        VolumeStats,
        client,
        std::move(clientConfig),
        CreateSessionConfig(request));

    return std::make_shared<TEndpoint>(
        *Executor,
        std::move(session),
        std::move(client),
        ThrottlerProvider,
        clientId,
        volume.GetDiskId());
}

TClientAppConfigPtr TSessionManager::CreateClientConfig(
    const NProto::TStartEndpointRequest& request,
    TString host,
    ui32 port)
{
    NProto::TClientAppConfig clientAppConfig;
    auto& config = *clientAppConfig.MutableClientConfig();

    config = Options.DefaultClientConfig;
    config.SetClientId(request.GetClientId());
    config.SetInstanceId(request.GetInstanceId());
    config.SetHost(host);
    config.SetPort(port);

    if (request.GetRequestTimeout()) {
        config.SetRequestTimeout(request.GetRequestTimeout());
    }
    if (request.GetRetryTimeout()) {
        config.SetRetryTimeout(request.GetRetryTimeout());
    }
    if (request.GetRetryTimeoutIncrement()) {
        config.SetRetryTimeoutIncrement(request.GetRetryTimeoutIncrement());
    }

    return std::make_shared<TClientAppConfig>(std::move(clientAppConfig));
}


TClientAppConfigPtr TSessionManager::CreateClientConfig(
    const NProto::TStartEndpointRequest& request)
{
    NProto::TClientAppConfig clientAppConfig;
    auto& config = *clientAppConfig.MutableClientConfig();

    config = Options.DefaultClientConfig;
    config.SetClientId(request.GetClientId());
    config.SetInstanceId(request.GetInstanceId());

    if (request.GetRequestTimeout()) {
        config.SetRequestTimeout(request.GetRequestTimeout());
    }
    if (request.GetRetryTimeout()) {
        config.SetRetryTimeout(request.GetRetryTimeout());
    }
    if (request.GetRetryTimeoutIncrement()) {
        config.SetRetryTimeoutIncrement(request.GetRetryTimeoutIncrement());
    }

    return std::make_shared<TClientAppConfig>(std::move(clientAppConfig));
}

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
    NCells::ICellsManagerPtr cellsManager,
    IStorageProviderPtr storageProvider,
    NRdma::IClientPtr rdmaClient,
    IEncryptionClientFactoryPtr encryptionClientFactory,
    TExecutorPtr executor,
    TSessionManagerOptions options,
    TServerAppConfigPtr config)
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
        std::move(cellsManager),
        std::move(storageProvider),
        std::move(rdmaClient),
        std::move(throttlerProvider),
        std::move(encryptionClientFactory),
        std::move(executor),
        std::move(options),
        std::move(config));
}

}   // namespace NCloud::NBlockStore::NServer
