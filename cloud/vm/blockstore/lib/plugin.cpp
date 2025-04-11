#include "plugin.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/nbd/client_handler.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/threading/future/future.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NPlugin {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

TSgList BuildSgList(const BlockPlugin_IOVector* iov)
{
    TSgList sglist;
    sglist.reserve(iov->niov);

    for (size_t i = 0; i < iov->niov; ++i) {
        BlockPlugin_Buffer* buf = &iov->iov[i];
        sglist.emplace_back((const char*)buf->iov_base, buf->iov_len);
    }

    return sglist;
}

////////////////////////////////////////////////////////////////////////////////

struct TVolume
{
    const TString DiskId;
    const ISessionPtr Session;
    const IBlockStorePtr ClientEndpoint;

    TVolume(TString diskId, ISessionPtr session, IBlockStorePtr clientEndpoint)
        : DiskId(std::move(diskId))
        , Session(std::move(session))
        , ClientEndpoint(std::move(clientEndpoint))
    {}
};

////////////////////////////////////////////////////////////////////////////////

void UpdateVolume(
    BlockPlugin_Volume* volume,
    const NProto::TVolume& vol,
    ui32 maxTransfer)
{
    volume->block_size = vol.GetBlockSize();
    volume->blocks_count = vol.GetBlocksCount();
    volume->max_transfer = maxTransfer;
    volume->opt_transfer = 0;
}

void DestroyVolume(BlockPlugin_Volume* volume)
{
    auto* state = static_cast<TVolume*>(volume->state);
    state->ClientEndpoint->Stop();

    delete state;
    volume->state = nullptr;
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestHandler
    : public TIntrusiveListItem<TRequestHandler>
{
    TMetricRequest MetricRequest;
    TCallContextPtr CallContext;

    TRequestHandler(EBlockStoreRequest requestType)
        : MetricRequest(requestType)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TPlugin final
    : public IPlugin
{
private:
    BlockPluginHost* const Host;

    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const IRequestStatsPtr RequestStats;
    const IVolumeStatsPtr VolumeStats;
    const IServerStatsPtr ClientStats;
    const IThrottlerPtr Throttler;
    const IClientPtr Client;
    const NBD::IClientPtr NbdClient;
    const TClientAppConfigPtr Config;
    IEncryptionClientFactoryPtr EncryptionClientFactory;

    TLog Log;

    TIntrusiveList<TRequestHandler> RequestsInFlight;
    TAdaptiveLock Lock;

public:
    TPlugin(
        BlockPluginHost* host,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        IServerStatsPtr clientStats,
        IThrottlerPtr throttler,
        IClientPtr сlient,
        NBD::IClientPtr nbdClient,
        TClientAppConfigPtr config);

    int MountVolume(
        const NProto::TPluginMountConfig& mountConfig,
        const TSessionConfig& sessionConfig,
        BlockPlugin_Volume* volume) override;

    int MountVolumeAsync(
        const NProto::TPluginMountConfig& mountConfig,
        const TSessionConfig& sessionConfig,
        BlockPlugin_Volume* volume,
        BlockPlugin_Completion* comp) override;

    int UnmountVolume(BlockPlugin_Volume* volume) override;

    int UnmountVolumeAsync(
        BlockPlugin_Volume* volume,
        BlockPlugin_Completion* comp) override;

    int SubmitRequest(
        BlockPlugin_Volume* volume,
        BlockPlugin_Request* req,
        BlockPlugin_Completion* comp) override;

    TString GetCountersJson() const override;

   size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override;

private:
    void HandleReadBlocks(
        BlockPlugin_Volume* volume,
        BlockPlugin_Request* req,
        BlockPlugin_Completion* comp);

    void HandleWriteBlocks(
        BlockPlugin_Volume* volume,
        BlockPlugin_Request* req,
        BlockPlugin_Completion* comp);

    void HandleZeroBlocks(
        BlockPlugin_Volume* volume,
        BlockPlugin_Request* req,
        BlockPlugin_Completion* comp);

    void CompleteRequest(BlockPlugin_Completion* comp)
    {
        Host->complete_request(Host, comp);
    }

    ISessionPtr CreateSessionIfNeeded(
        const NProto::TPluginMountConfig& mountConfig,
        const TSessionConfig& sessionConfig,
        BlockPlugin_Volume* volume);

    IBlockStorePtr CreateClientEndpoint(
        const NProto::TPluginMountConfig& mountConfig,
        const TString& diskId);

    std::shared_ptr<TRequestHandler> RegisterRequest(
        const EBlockStoreRequest requestType,
        const TString& diskId,
        const ui64 requestId,
        const ui64 startIndex,
        const ui32 blocksCount);

    void UnregisterRequest(
        TRequestHandler& requestHandler,
        const NProto::TError& error);

    void SetupHeaders(NProto::THeaders& headers, ui64 requestId) const;
};

////////////////////////////////////////////////////////////////////////////////

TPlugin::TPlugin(
        BlockPluginHost* host,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        IServerStatsPtr clientStats,
        IThrottlerPtr throttler,
        IClientPtr client,
        NBD::IClientPtr nbdClient,
        TClientAppConfigPtr config)
    : Host(host)
    , Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
    , Logging(std::move(logging))
    , Monitoring(std::move(monitoring))
    , RequestStats(std::move(requestStats))
    , VolumeStats(std::move(volumeStats))
    , ClientStats(std::move(clientStats))
    , Throttler(std::move(throttler))
    , Client(std::move(client))
    , NbdClient(std::move(nbdClient))
    , Config(std::move(config))
    , Log(Logging->CreateLog("BLOCKSTORE_PLUGIN"))
{
    EncryptionClientFactory = CreateEncryptionClientFactory(
        Logging,
        CreateDefaultEncryptionKeyProvider());
}

////////////////////////////////////////////////////////////////////////////////

int TPlugin::MountVolume(
    const NProto::TPluginMountConfig& mountConfig,
    const TSessionConfig& sessionConfig,
    BlockPlugin_Volume* volume)
{
    auto requestId = CreateRequestId();

    auto requestHandler = RegisterRequest(
        EBlockStoreRequest::MountVolume,
        sessionConfig.DiskId,
        requestId,
        0ull,   // startIndex
        0u      // blocksCount
    );

    auto session = CreateSessionIfNeeded(
        mountConfig,
        sessionConfig,
        volume);
    auto maxTransfer = session->GetMaxTransfer();

    NProto::THeaders headers;
    SetupHeaders(headers, requestId);

    STORAGE_DEBUG(
        TRequestInfo(EBlockStoreRequest::MountVolume, requestId, sessionConfig.DiskId)
        << " submit request");

    auto future = session->MountVolume(
        sessionConfig.AccessMode,
        sessionConfig.MountMode,
        sessionConfig.MountSeqNumber,
        requestHandler->CallContext,
        std::move(headers));

    ClientStats->RequestSent(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    const auto& response = future.GetValueSync();

    ClientStats->ResponseReceived(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    int result = 0;

    if (HasError(response)) {
        const auto& error = response.GetError();
        STORAGE_ERROR(
            TRequestInfo(EBlockStoreRequest::MountVolume, requestId, sessionConfig.DiskId)
            << " request failed: " << FormatError(error));
        result = BLOCK_PLUGIN_E_FAIL;
    } else {
        STORAGE_DEBUG(
            TRequestInfo(EBlockStoreRequest::MountVolume, requestId, sessionConfig.DiskId)
            << " request completed");

        UpdateVolume(volume, response.GetVolume(), maxTransfer);
        result = BLOCK_PLUGIN_E_OK;
    }

    UnregisterRequest(*requestHandler, response.GetError());
    return result;
}

int TPlugin::MountVolumeAsync(
    const NProto::TPluginMountConfig& mountConfig,
    const TSessionConfig& sessionConfig,
    BlockPlugin_Volume* volume,
    BlockPlugin_Completion* comp)
{
    comp->id = CreateRequestId();
    comp->status = BP_COMPLETION_INPROGRESS;

    auto requestHandler = RegisterRequest(
        EBlockStoreRequest::MountVolume,
        sessionConfig.DiskId,
        comp->id,
        0ull,   // startIndex
        0u      // blocksCount
    );

    auto session = CreateSessionIfNeeded(
        mountConfig,
        sessionConfig,
        volume);
    auto maxTransfer = session->GetMaxTransfer();

    STORAGE_DEBUG(
        TRequestInfo(EBlockStoreRequest::MountVolume, comp->id, sessionConfig.DiskId)
        << " submit request");

    NProto::THeaders headers;
    SetupHeaders(headers, comp->id);

    auto future = session->MountVolume(
        sessionConfig.AccessMode,
        sessionConfig.MountMode,
        sessionConfig.MountSeqNumber,
        requestHandler->CallContext,
        std::move(headers));

    ClientStats->RequestSent(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    future.Subscribe(
        [=,
        diskId = sessionConfig.DiskId,
        requestHandler = std::move(requestHandler)] (const auto& future)
        {
            ClientStats->ResponseReceived(
                requestHandler->MetricRequest,
                *requestHandler->CallContext);

            const auto& response = future.GetValue();
            if (HasError(response)) {
                const auto& error = response.GetError();
                STORAGE_ERROR(
                    TRequestInfo(EBlockStoreRequest::MountVolume, comp->id, diskId)
                    << " request failed: " << FormatError(error));
                comp->status = BP_COMPLETION_ERROR;
            } else {
                STORAGE_DEBUG(
                    TRequestInfo(EBlockStoreRequest::MountVolume, comp->id, diskId)
                    << " request completed");
                comp->status = BP_COMPLETION_MOUNT_FINISHED;
                UpdateVolume(volume, response.GetVolume(), maxTransfer);
            }

            UnregisterRequest(*requestHandler, response.GetError());
            CompleteRequest(comp);
        });

    return BLOCK_PLUGIN_E_OK;
}

////////////////////////////////////////////////////////////////////////////////

int TPlugin::UnmountVolume(BlockPlugin_Volume* volume)
{
    auto* state = static_cast<TVolume*>(volume->state);

    auto requestId = CreateRequestId();
    auto callContext = MakeIntrusive<TCallContext>(requestId);

    auto requestHandler = RegisterRequest(
        EBlockStoreRequest::UnmountVolume,
        state->DiskId,
        requestId,
        0ull,   // startIndex
        0u      // blocksCount
    );

    STORAGE_DEBUG(
        TRequestInfo(EBlockStoreRequest::UnmountVolume, requestId, state->DiskId)
        << " submit request");

    NProto::THeaders headers;
    SetupHeaders(headers, requestId);

    auto future = state->Session->UnmountVolume(
        requestHandler->CallContext,
        std::move(headers));

    ClientStats->RequestSent(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    const auto& response = future.GetValueSync();

    ClientStats->ResponseReceived(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    int result = 0;

    if (HasError(response)) {
        const auto& error = response.GetError();
        STORAGE_ERROR(
            TRequestInfo(EBlockStoreRequest::UnmountVolume, requestId, state->DiskId)
            << " request failed: " << FormatError(error));
        result = BLOCK_PLUGIN_E_FAIL;
    } else {
        STORAGE_DEBUG(
            TRequestInfo(EBlockStoreRequest::UnmountVolume, requestId, state->DiskId)
            << " request completed");

        DestroyVolume(volume);
        result = BLOCK_PLUGIN_E_OK;
    }

    UnregisterRequest(*requestHandler, response.GetError());
    return result;
}

int TPlugin::UnmountVolumeAsync(
    BlockPlugin_Volume* volume,
    BlockPlugin_Completion* comp)
{
    auto* state = static_cast<TVolume*>(volume->state);

    comp->id = CreateRequestId();
    comp->status = BP_COMPLETION_INPROGRESS;

    auto callContext = MakeIntrusive<TCallContext>(comp->id);

    auto requestHandler = RegisterRequest(
        EBlockStoreRequest::UnmountVolume,
        state->DiskId,
        comp->id,
        0ull,   // startIndex
        0u      // blocksCount
    );

    STORAGE_DEBUG(
        TRequestInfo(EBlockStoreRequest::UnmountVolume, comp->id, state->DiskId)
        << " submit request");

    NProto::THeaders headers;
    SetupHeaders(headers, comp->id);

    auto future = state->Session->UnmountVolume(
        requestHandler->CallContext,
        std::move(headers));

    ClientStats->RequestSent(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    future.Subscribe(
        [=,
        requestHandler = std::move(requestHandler)] (const auto& future)
        {
            ClientStats->ResponseReceived(
                requestHandler->MetricRequest,
                *requestHandler->CallContext);

            const auto& response = future.GetValue();
            if (HasError(response)) {
                const auto& error = response.GetError();
                STORAGE_ERROR(
                    TRequestInfo(EBlockStoreRequest::UnmountVolume, comp->id, state->DiskId)
                    << " request failed: " << FormatError(error));
                comp->status = BP_COMPLETION_ERROR;
            } else {
                STORAGE_DEBUG(
                    TRequestInfo(EBlockStoreRequest::UnmountVolume, comp->id, state->DiskId)
                    << " request completed");
                comp->status = BP_COMPLETION_UNMOUNT_FINISHED;
                DestroyVolume(volume);
            }

            UnregisterRequest(*requestHandler, response.GetError());
            CompleteRequest(comp);
        });

    return BLOCK_PLUGIN_E_OK;
}

////////////////////////////////////////////////////////////////////////////////

int TPlugin::SubmitRequest(
    BlockPlugin_Volume* volume,
    BlockPlugin_Request* req,
    BlockPlugin_Completion* comp)
{
    comp->id = CreateRequestId();
    comp->status = BP_COMPLETION_INPROGRESS;

    switch (req->type) {
        case BLOCK_PLUGIN_READ_BLOCKS: {
            auto* r = reinterpret_cast<BlockPlugin_ReadBlocks*>(req);
            if (req->size == sizeof(*r)) {
                HandleReadBlocks(volume, req, comp);
                return BLOCK_PLUGIN_E_OK;
            }
            break;
        }
        case BLOCK_PLUGIN_WRITE_BLOCKS: {
            auto* r = reinterpret_cast<BlockPlugin_WriteBlocks*>(req);
            if (req->size == sizeof(*r)) {
                HandleWriteBlocks(volume, req, comp);
                return BLOCK_PLUGIN_E_OK;
            }
            break;
        }
        case BLOCK_PLUGIN_ZERO_BLOCKS: {
            auto* r = reinterpret_cast<BlockPlugin_ZeroBlocks*>(req);
            if (req->size == sizeof(*r)) {
                HandleZeroBlocks(volume, req, comp);
                return BLOCK_PLUGIN_E_OK;
            }
            break;
        }
    }

    comp->status = BP_COMPLETION_ERROR;
    return BLOCK_PLUGIN_E_ARGUMENT;
}

void TPlugin::HandleReadBlocks(
    BlockPlugin_Volume* volume,
    BlockPlugin_Request* req,
    BlockPlugin_Completion* comp)
{
    auto* state = static_cast<TVolume*>(volume->state);
    auto* r = reinterpret_cast<BlockPlugin_ReadBlocks*>(req);

    auto callContext = MakeIntrusive<TCallContext>(comp->id);

    auto requestHandler = RegisterRequest(
        EBlockStoreRequest::ReadBlocks,
        state->DiskId,
        comp->id,
        r->start_index,
        r->blocks_count);

    TGuardedSgList guardedSgList(BuildSgList(r->bp_iov));

    STORAGE_TRACE(
        TRequestInfo(EBlockStoreRequest::ReadBlocks, comp->id, state->DiskId)
        << " submit request");

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(r->start_index);
    request->SetBlocksCount(r->blocks_count);
    request->Sglist = guardedSgList;
    request->BlockSize = volume->block_size;

    SetupHeaders(*request->MutableHeaders(), comp->id);

    auto future = state->Session->ReadBlocksLocal(
        requestHandler->CallContext,
        std::move(request));

    ClientStats->RequestSent(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    future.Subscribe(
        [=,
        sglist = std::move(guardedSgList),
        requestHandler = std::move(requestHandler)] (const auto& f) mutable
        {
            ClientStats->ResponseReceived(
                requestHandler->MetricRequest,
                *requestHandler->CallContext);

            const auto& response = f.GetValue();
            if (HasError(response)) {
                const auto& error = response.GetError();
                STORAGE_ERROR(
                    TRequestInfo(EBlockStoreRequest::ReadBlocks, comp->id, state->DiskId)
                    << " request failed: " << FormatError(error));
                comp->status = BP_COMPLETION_ERROR;
            } else {
                STORAGE_TRACE(
                    TRequestInfo(EBlockStoreRequest::ReadBlocks, comp->id, state->DiskId)
                    << " request completed");
                comp->status = BP_COMPLETION_READ_FINISHED;
            }

            sglist.Close();
            UnregisterRequest(*requestHandler, response.GetError());
            CompleteRequest(comp);
        });
}

void TPlugin::HandleWriteBlocks(
    BlockPlugin_Volume* volume,
    BlockPlugin_Request* req,
    BlockPlugin_Completion* comp)
{
    auto* state = static_cast<TVolume*>(volume->state);
    auto* r = reinterpret_cast<BlockPlugin_WriteBlocks*>(req);

    auto callContext = MakeIntrusive<TCallContext>(comp->id);

    auto requestHandler = RegisterRequest(
        EBlockStoreRequest::WriteBlocks,
        state->DiskId,
        comp->id,
        r->start_index,
        r->blocks_count);

    TGuardedSgList guardedSgList(BuildSgList(r->bp_iov));

    STORAGE_TRACE(
        TRequestInfo(EBlockStoreRequest::WriteBlocks, comp->id, state->DiskId)
        << " submit request");

    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(r->start_index);
    request->Sglist = guardedSgList;
    request->BlocksCount = r->blocks_count;
    request->BlockSize = volume->block_size;

    SetupHeaders(*request->MutableHeaders(), comp->id);

    auto future = state->Session->WriteBlocksLocal(
        requestHandler->CallContext,
        std::move(request));

    ClientStats->RequestSent(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    future.Subscribe(
        [=,
        sglist = std::move(guardedSgList),
        requestHandler = std::move(requestHandler)] (const auto& f) mutable
        {
            ClientStats->ResponseReceived(
                requestHandler->MetricRequest,
                *requestHandler->CallContext);

            const auto& response = f.GetValue();
            if (HasError(response)) {
                const auto& error = response.GetError();
                STORAGE_ERROR(
                    TRequestInfo(EBlockStoreRequest::WriteBlocks, comp->id, state->DiskId)
                    << " request failed: " << FormatError(error));
                comp->status = BP_COMPLETION_ERROR;
            } else {
                STORAGE_TRACE(
                    TRequestInfo(EBlockStoreRequest::WriteBlocks, comp->id, state->DiskId)
                    << " request completed");
                comp->status = BP_COMPLETION_WRITE_FINISHED;
            }

            sglist.Close();
            UnregisterRequest(*requestHandler, response.GetError());
            CompleteRequest(comp);
        });
}

void TPlugin::HandleZeroBlocks(
    BlockPlugin_Volume* volume,
    BlockPlugin_Request* req,
    BlockPlugin_Completion* comp)
{
    auto* state = static_cast<TVolume*>(volume->state);
    auto* r = reinterpret_cast<BlockPlugin_ZeroBlocks*>(req);

    auto callContext = MakeIntrusive<TCallContext>(comp->id);

    auto requestHandler = RegisterRequest(
        EBlockStoreRequest::ZeroBlocks,
        state->DiskId,
        comp->id,
        r->start_index,
        r->blocks_count);

    STORAGE_TRACE(
        TRequestInfo(EBlockStoreRequest::ZeroBlocks, comp->id, state->DiskId)
        << " submit request");

    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetStartIndex(r->start_index);
    request->SetBlocksCount(r->blocks_count);

    SetupHeaders(*request->MutableHeaders(), comp->id);

    auto future = state->Session->ZeroBlocks(
        requestHandler->CallContext,
        std::move(request));

    ClientStats->RequestSent(
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    future.Subscribe(
        [=, requestHandler = std::move(requestHandler)] (const auto& f) {
            ClientStats->ResponseReceived(
                requestHandler->MetricRequest,
                *requestHandler->CallContext);

            const auto& response = f.GetValue();
            if (HasError(response)) {
                const auto& error = response.GetError();
                STORAGE_ERROR(
                    TRequestInfo(EBlockStoreRequest::ZeroBlocks, comp->id, state->DiskId)
                    << " request failed: " << FormatError(error));
                comp->status = BP_COMPLETION_ERROR;
            } else {
                STORAGE_TRACE(
                    TRequestInfo(EBlockStoreRequest::ZeroBlocks, comp->id, state->DiskId)
                    << " request completed");
                comp->status = BP_COMPLETION_ZERO_FINISHED;
            }

            UnregisterRequest(*requestHandler, response.GetError());
            CompleteRequest(comp);
        });
}

ISessionPtr TPlugin::CreateSessionIfNeeded(
    const NProto::TPluginMountConfig& mountConfig,
    const TSessionConfig& sessionConfig,
    BlockPlugin_Volume* volume)
{
    auto* volumeState = static_cast<TVolume*>(volume->state);
    ISessionPtr session;

    if (volumeState) {
        session = volumeState->Session;
    } else {
        auto clientEndpoint = CreateClientEndpoint(
            mountConfig,
            sessionConfig.DiskId);
        clientEndpoint->Start();

        session = CreateSession(
            Timer,
            Scheduler,
            Logging,
            RequestStats,
            VolumeStats,
            clientEndpoint,
            Config,
            sessionConfig);
        volume->state = new TVolume(
            sessionConfig.DiskId,
            session,
            std::move(clientEndpoint));
    }
    return session;
}

IBlockStorePtr TPlugin::CreateClientEndpoint(
    const NProto::TPluginMountConfig& mountConfig,
    const TString& diskId)
{
    IBlockStorePtr clientEndpoint;
    const auto& socketPath = mountConfig.GetUnixSocketPath();

    if (socketPath) {
        switch (Config->GetIpcType()) {
            case NProto::IPC_GRPC: {
                STORAGE_INFO("Creating grpc/uds connection: " << socketPath);
                clientEndpoint = Client->CreateDataEndpoint(socketPath);
                break;
            }
            case NProto::IPC_NBD: {
                auto nbdSocketPath = socketPath + Config->GetNbdSocketSuffix();
                STORAGE_INFO("Creating nbd connection: " << nbdSocketPath);

                auto clientHandler = NBD::CreateClientHandler(
                    Logging,
                    Config->GetNbdStructuredReply(),
                    Config->GetNbdUseNbsErrors());

                auto grpcClientEndpoint = Client->CreateDataEndpoint(socketPath);

                Y_ENSURE(NbdClient, "NBD client is not initialized");
                clientEndpoint = NbdClient->CreateEndpoint(
                    TNetworkAddress(TUnixSocketPath(nbdSocketPath)),
                    std::move(clientHandler),
                    std::move(grpcClientEndpoint));
                break;
            }
            case NProto::IPC_VHOST:
            default:
                ythrow yexception()
                    << "Unsupported ipc type: "
                    << static_cast<ui32>(Config->GetIpcType());
        }
    } else {
        STORAGE_INFO("Creating grpc/tcp connection");
        clientEndpoint = Client->CreateDataEndpoint();
    }

    auto retryPolicy =
        CreateRetryPolicy(Config, VolumeStats->GetStorageMediaKind(diskId));

    clientEndpoint = CreateDurableClient(
        Config,
        std::move(clientEndpoint),
        std::move(retryPolicy),
        Logging,
        Timer,
        Scheduler,
        RequestStats,
        VolumeStats);

    if (socketPath.empty() // unix-socket requests are handled on server side
        && mountConfig.HasEncryptionSpec())
    {
        auto future = EncryptionClientFactory->CreateEncryptionClient(
            std::move(clientEndpoint),
            mountConfig.GetEncryptionSpec(),
            diskId);

        const auto& clientOrError = future.GetValue();
        if (HasError(clientOrError)) {
            const auto& error = clientOrError.GetError();
            ythrow TServiceError(error.GetCode()) << error.GetMessage();
        }

        clientEndpoint = clientOrError.GetResult();
    }

    if (Throttler) {
        clientEndpoint = CreateThrottlingClient(
            std::move(clientEndpoint),
            Throttler);
    }

    return clientEndpoint;
}

std::shared_ptr<TRequestHandler> TPlugin::RegisterRequest(
    const EBlockStoreRequest requestType,
    const TString& diskId,
    const ui64 requestId,
    const ui64 startIndex,
    const ui32 blocksCount)
{
    auto requestHandler = std::make_shared<TRequestHandler>(requestType);

    requestHandler->CallContext = MakeIntrusive<TCallContext>(requestId);

    ClientStats->PrepareMetricRequest(
        requestHandler->MetricRequest,
        Config->GetClientId(),
        diskId,
        startIndex,
        ClientStats->GetBlockSize(diskId) * blocksCount,
        false // unaligned
    );

    ClientStats->RequestStarted(
        Log,
        requestHandler->MetricRequest,
        *requestHandler->CallContext);

    with_lock (Lock) {
        RequestsInFlight.PushBack(requestHandler.get());
    }

    return requestHandler;
}

void TPlugin::UnregisterRequest(
    TRequestHandler& requestHandler,
    const NProto::TError& error)
{
    with_lock (Lock) {
        requestHandler.Unlink();
    }

    ClientStats->RequestCompleted(
        Log,
        requestHandler.MetricRequest,
        *requestHandler.CallContext,
        error);
}

size_t TPlugin::CollectRequests(const TIncompleteRequestsCollector& collector)
{
    size_t count = 0;
    with_lock (Lock) {
        ui64 now = GetCycleCount();
        for (auto& request: RequestsInFlight) {
            ++count;
            auto requestTime = request.CallContext->CalcRequestTime(now);
            if (requestTime) {
                collector(
                    *request.CallContext,
                    request.MetricRequest.VolumeInfo,
                    request.MetricRequest.MediaKind,
                    request.MetricRequest.RequestType,
                    requestTime);
            }
        }
    }
    return count;
}

void TPlugin::SetupHeaders(NProto::THeaders& headers, ui64 requestId) const
{
    headers.SetRequestId(requestId);
    headers.SetClientId(Config->GetClientConfig().GetClientId());
}

////////////////////////////////////////////////////////////////////////////////

TString TPlugin::GetCountersJson() const
{
    return NMonitoring::ToJson(*Monitoring->GetCounters());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPluginPtr CreatePlugin(
    BlockPluginHost* host,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr clientStats,
    IThrottlerPtr throttler,
    IClientPtr client,
    NBD::IClientPtr nbdClient,
    TClientAppConfigPtr config)
{
    return std::make_shared<TPlugin>(
        host,
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(requestStats),
        std::move(volumeStats),
        std::move(clientStats),
        std::move(throttler),
        std::move(client),
        std::move(nbdClient),
        std::move(config));
}

}   // namespace NCloud::NBlockStore::NPlugin
