#include "session_manager.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>
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

#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NServer {

using namespace NClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEndpointSession final
    : public IEndpointSession
    , public std::enable_shared_from_this<TEndpointSession>
{
private:
    ISessionPtr Session;
    IBlockStorePtr DataClient;
    const IThrottlerProviderPtr ThrottlerProvider;
    const TString ClientId;
    const TString DiskId;

public:
    TEndpointSession(
            ISessionPtr session,
            IBlockStorePtr dataClient,
            IThrottlerProviderPtr throttlerProvider,
            TString clientId,
            TString diskId)
        : Session(std::move(session))
        , DataClient(std::move(dataClient))
        , ThrottlerProvider(std::move(throttlerProvider))
        , ClientId(std::move(clientId))
        , DiskId(std::move(diskId))
    {}

    TFuture<NProto::TError> Start(
        TCallContextPtr callContext,
        NProto::THeaders headers)
    {
        DataClient->Start();

        headers.SetClientId(ClientId);
        auto future = Session->MountVolume(std::move(callContext), headers);
        return future.Apply([weakPtr = weak_from_this()] (const auto& f) {
            if (auto p = weakPtr.lock()) {
                if (HasError(f.GetValue())) {
                    p->DataClient->Stop();
                }
            }
            return f.GetValue().GetError();
        });
    }

    TFuture<NProto::TError> Remove(
        TCallContextPtr callContext,
        NProto::THeaders headers) override
    {
        headers.SetClientId(ClientId);
        auto future = Session->UnmountVolume(std::move(callContext), headers);

        return future.Apply([weakPtr = weak_from_this()] (const auto& f) {
            if (auto p = weakPtr.lock()) {
                p->DataClient->Stop();

                p->Session.reset();
                p->DataClient.reset();
                p->ThrottlerProvider->Clean();
            };

            return f.GetValue().GetError();
        });
    }

    TFuture<NProto::TMountVolumeResponse> Alter(
        TCallContextPtr callContext,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        NProto::THeaders headers) override
    {
        headers.SetClientId(ClientId);
        return Session->MountVolume(
            accessMode,
            mountMode,
            mountSeqNumber,
            std::move(callContext),
            headers);
    }

    TFuture<NProto::TMountVolumeResponse> Describe(
        TCallContextPtr callContext,
        NProto::THeaders headers) const override
    {
        headers.SetClientId(ClientId);
        return Session->MountVolume(std::move(callContext), headers);
    }

    ISessionPtr GetSession() const override
    {
        return Session;
    }

    NProto::TClientPerformanceProfile GetProfile() const override
    {
        return ThrottlerProvider->GetPerformanceProfile(ClientId);
    }
};

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
{
private:
    const IStoragePtr Storage;
    const IBlockStorePtr Service;
    const IServerStatsPtr ServerStats;
    const TString ClientId;
    const TDuration RequestTimeout;

public:
    TStorageDataClient(
            IStoragePtr storage,
            IBlockStorePtr service,
            IServerStatsPtr serverStats,
            TString clientId,
            TDuration requestTimeout)
        : Storage(std::move(storage))
        , Service(std::move(service))
        , ServerStats(std::move(serverStats))
        , ClientId(std::move(clientId))
        , RequestTimeout(requestTimeout)
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
        PrepareRequestHeaders(*request->MutableHeaders(), *callContext);       \
        return Storage->name(std::move(callContext), std::move(request));      \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_IMPLEMENT_METHOD(ZeroBlocks)
    BLOCKSTORE_IMPLEMENT_METHOD(ReadBlocksLocal)
    BLOCKSTORE_IMPLEMENT_METHOD(WriteBlocksLocal)

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

        const auto& serverStats = ServerStats;
        return future.Apply([=] (const auto& f) {
            const auto& response = f.GetValue();

            if (!HasError(response) && response.HasVolume()) {
                serverStats->MountVolume(
                    response.GetVolume(),
                    ClientId,
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

        const auto& serverStats = ServerStats;
        return future.Apply([=, diskId = std::move(diskId)] (const auto& f) {
            const auto& response = f.GetValue();

            if (!HasError(response)) {
                serverStats->UnmountVolume(diskId, ClientId);
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
};

////////////////////////////////////////////////////////////////////////////////

class TSessionFactory final
    : public ISessionFactory
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
    const IStorageProviderPtr StorageProvider;
    const IThrottlerProviderPtr ThrottlerProvider;
    const IEncryptionClientFactoryPtr EncryptionClientFactory;
    const TExecutorPtr Executor;
    const TSessionFactoryOptions Options;

    TLog Log;

public:
    TSessionFactory(
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            IRequestStatsPtr requestStats,
            IVolumeStatsPtr volumeStats,
            IServerStatsPtr serverStats,
            IBlockStorePtr service,
            IStorageProviderPtr storageProvider,
            IThrottlerProviderPtr throttlerProvider,
            IEncryptionClientFactoryPtr encryptionClientFactory,
            TExecutorPtr executor,
            TSessionFactoryOptions options)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , RequestStats(std::move(requestStats))
        , VolumeStats(std::move(volumeStats))
        , ServerStats(std::move(serverStats))
        , Service(std::move(service))
        , StorageProvider(std::move(storageProvider))
        , ThrottlerProvider(std::move(throttlerProvider))
        , EncryptionClientFactory(std::move(encryptionClientFactory))
        , Executor(std::move(executor))
        , Options(std::move(options))
    {
        Log = Logging->CreateLog("BLOCKSTORE_SERVER");
    }

    TFuture<TSessionOrError> CreateSession(
        TCallContextPtr callContext,
        const NProto::TStartEndpointRequest& request,
        NProto::TVolume& volume) override;

private:
    TSessionOrError CreateSessionImpl(
        TCallContextPtr callContext,
        const NProto::TStartEndpointRequest& request,
        NProto::TVolume& volume);

    TClientAppConfigPtr CreateClientConfig(
        const NProto::TStartEndpointRequest& request);

    static TSessionConfig CreateSessionConfig(
        const NProto::TStartEndpointRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<TSessionFactory::TSessionOrError> TSessionFactory::CreateSession(
    TCallContextPtr callContext,
    const NProto::TStartEndpointRequest& request,
    NProto::TVolume& volume)
{
    return Executor->Execute([=, &volume] () mutable {
        return CreateSessionImpl(std::move(callContext), request, volume);
    });
}

TSessionFactory::TSessionOrError TSessionFactory::CreateSessionImpl(
    TCallContextPtr callContext,
    const NProto::TStartEndpointRequest& request,
    NProto::TVolume& volume)
{
    auto describeRequest = std::make_shared<NProto::TDescribeVolumeRequest>();
    describeRequest->MutableHeaders()->CopyFrom(request.GetHeaders());
    describeRequest->SetDiskId(request.GetDiskId());

    auto describeFuture = Service->DescribeVolume(
        callContext,
        std::move(describeRequest));

    auto describeResponse = Executor->WaitFor(describeFuture);
    if (HasError(describeResponse)) {
        return TErrorResponse(describeResponse.GetError());
    }

    volume = describeResponse.GetVolume();
    const auto clientId = request.GetClientId();
    auto accessMode = request.GetVolumeAccessMode();

    auto future = StorageProvider->CreateStorage(volume, clientId, accessMode)
        .Apply([] (const auto& f) {
            auto storage = f.GetValue();
            // TODO: StorageProvider should return TResultOrError<IStoragePtr>
            return TResultOrError(std::move(storage));
        });

    const auto& storageResult = Executor->WaitFor(future);
    auto storage = storageResult.GetResult();

    auto clientConfig = CreateClientConfig(request);

    IBlockStorePtr client = std::make_shared<TStorageDataClient>(
        std::move(storage),
        Service,
        ServerStats,
        clientId,
        clientConfig->GetRequestTimeout());

    if (Options.TemporaryServer) {
        client = CreateErrorTransformService(
            std::move(client),
            {{ EErrorKind::ErrorFatal, E_REJECTED }});
    }

    auto retryPolicy = CreateRetryPolicy(clientConfig);

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

    auto endpoint = std::make_shared<TEndpointSession>(
        std::move(session),
        std::move(client),
        ThrottlerProvider,
        clientId,
        volume.GetDiskId());

    auto startFuture = endpoint->Start(
        std::move(callContext),
        request.GetHeaders());
    auto error = Executor->WaitFor(startFuture);
    if (HasError(error)) {
        return error;
    }

    return TSessionOrError(endpoint);
}

TClientAppConfigPtr TSessionFactory::CreateClientConfig(
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

TSessionConfig TSessionFactory::CreateSessionConfig(
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

ISessionFactoryPtr CreateSessionFactory(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    IStorageProviderPtr storageProvider,
    IEncryptionClientFactoryPtr encryptionClientFactory,
    TExecutorPtr executor,
    TSessionFactoryOptions options)
{
    auto throttlerProvider = CreateThrottlerProvider(
        options.HostProfile,
        logging,
        timer,
        scheduler,
        monitoring->GetCounters()->GetSubgroup("counters", "blockstore"),
        requestStats,
        volumeStats);

    return std::make_shared<TSessionFactory>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(requestStats),
        std::move(volumeStats),
        std::move(serverStats),
        std::move(service),
        std::move(storageProvider),
        std::move(throttlerProvider),
        std::move(encryptionClientFactory),
        std::move(executor),
        std::move(options));
}

}   // namespace NCloud::NBlockStore::NServer
