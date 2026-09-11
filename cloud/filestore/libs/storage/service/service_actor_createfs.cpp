#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/compressed_bitmap.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateCreateFileSystemRequest(
    const NProto::TCreateFileStoreRequest& request)
{
    const auto& fileSystemId = request.GetFileSystemId();
    if (!fileSystemId) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "missing file system identifier");
    }

    NProto::TError fileSystemIdError = ValidateFilesystemId(fileSystemId);
    if (HasError(fileSystemIdError)) {
        return fileSystemIdError;
    }

    const auto& cloudId = request.GetCloudId();
    if (!cloudId) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "missing cloud identifier");
    }

    const auto& folderId = request.GetFolderId();
    if (!folderId) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "missing folder identifier");
    }

    ui32 blockSize = request.GetBlockSize();
    if (!blockSize
            || !IsAligned(blockSize, 4_KB)
            || blockSize < 4_KB
            || blockSize > 128_KB)
    {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid block size: " << blockSize);
    }

    ui64 blocksCount = request.GetBlocksCount();
    if (!blocksCount || blockSize * blocksCount < 1_MB) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid blocks count: " << blocksCount);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TCreateFileStoreActor final
    : public TActorBootstrapped<TCreateFileStoreActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const NProto::TCreateFileStoreRequest Request;
    const TString& LogTag;
    TMultiShardFileStoreConfig FileStoreConfig;

    bool MainFileSystemCreated = false;
    ui32 NextShardToCreate = 0;
    ui32 ShardsToCreate = 0;
    ui32 NextShardToConfigure = 0;
    ui32 ShardsToConfigure = 0;
    bool ShardConfigurationStarted = false;

    NProtoPrivate::TFileSystemShardCreationState ShardCreationState;
    ui32 ShardCreationStateVersion = 0;
    bool InitialShardCreationStateRead = false;
    ui64 ShardBitmapBitCount = 0;
    std::unique_ptr<NCloud::TCompressedBitmap> CreatedShardBitmap;

public:
    TCreateFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TCreateFileStoreRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateMainFileStore(const TActorContext& ctx);
    void CreateShards(const TActorContext& ctx);
    void ContinueCreateShards(const TActorContext& ctx, ui32 limit);
    void CreateShard(const TActorContext& ctx, const ui32 shardIndex);
    void ConfigureShards(const TActorContext& ctx);
    void ConfigureShard(const TActorContext& ctx, const ui32 shardIndex);
    void ConfigureMainFileStore(const TActorContext& ctx);

    bool IsShardCreated(ui32 shardIndex) const;
    void ReadShardCreationState(const TActorContext& ctx);
    void SetupCreatedShardBitmap();
    void MergeCreatedShardBitmap(
        const NProtoPrivate::TCompressedBitmapData& bitmap);
    bool HasUnpersistedCreatedShards() const;
    void UpdateShardCreationState(const TActorContext& ctx);
    void UpdateShardCreatedState(
        const TActorContext& ctx,
        ui32 shardIndex);

    void HandleCreateFileStoreResponse(
        const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleShardCreationStateResponse(
        const TEvIndexTablet::TEvUnsafeChangeTabletStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigureShardResponse(
        const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigureMainFileStoreResponse(
        const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvCreateFileStoreResponse> response);
};

////////////////////////////////////////////////////////////////////////////////

TCreateFileStoreActor::TCreateFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TCreateFileStoreRequest request)
    : StorageConfig(std::move(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , LogTag(Request.GetFileSystemId())
{}

void TCreateFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    CreateMainFileStore(ctx);
    Become(&TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateFileStoreActor::CreateMainFileStore(const TActorContext& ctx)
{
    NKikimrFileStore::TConfig config;
    config.SetFileSystemId(Request.GetFileSystemId());
    config.SetProjectId(Request.GetProjectId());
    config.SetFolderId(Request.GetFolderId());
    config.SetCloudId(Request.GetCloudId());
    config.SetBlockSize(Request.GetBlockSize());
    config.SetBlocksCount(Request.GetBlocksCount());
    config.SetStorageMediaKind(Request.GetStorageMediaKind());
    config.SetRangeIdHasherType(1);

    if (StorageConfig->GetAutomaticShardCreationEnabled() ||
        Request.GetShardCount() > 0)
    {
        FileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            config,
            Request.GetPerformanceProfile(),
            Request.GetShardCount());
        ShardsToCreate = FileStoreConfig.ShardConfigs.size();
        ShardsToConfigure = ShardsToCreate;
        config = FileStoreConfig.MainFileSystemConfig;

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Will create filesystem with %u shards",
            LogTag.c_str(),
            FileStoreConfig.ShardConfigs.size());
    } else {
        SetupFileStorePerformanceAndChannels(
            false,  // do not allocate mixed0 channel
            *StorageConfig,
            config,
            Request.GetPerformanceProfile());

        FileStoreConfig.MainFileSystemConfig = config;
    }

    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        std::move(config));

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TCreateFileStoreActor::CreateShards(const TActorContext& ctx)
{
    if (ShardsToCreate > 0) {
        NextShardToCreate = 0;
        ContinueCreateShards(
            ctx,
            StorageConfig->GetMaxShardManagementRequestsInFlight());
    }

    if (ShardsToCreate == 0) {
        ConfigureShards(ctx);
    }
}

void TCreateFileStoreActor::ContinueCreateShards(
    const TActorContext& ctx,
    const ui32 limit)
{
    ui32 requests = 0;
    while (NextShardToCreate < FileStoreConfig.ShardConfigs.size() &&
           (limit == 0 || requests < limit))
    {
        if (limit != 0) {
            if (IsShardCreated(NextShardToCreate)) {
                ++NextShardToCreate;
                Y_DEBUG_ABORT_UNLESS(ShardsToCreate);
                --ShardsToCreate;
                continue;
            }
        }

        CreateShard(ctx, NextShardToCreate);
        ++NextShardToCreate;
        ++requests;
    }
}

void TCreateFileStoreActor::CreateShard(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        FileStoreConfig.ShardConfigs[shardIndex]);

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Creating shard %s",
        LogTag.c_str(),
        request->Config.GetFileSystemId().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        shardIndex   // cookie
    );
}

void TCreateFileStoreActor::ConfigureShards(const TActorContext& ctx)
{
    ShardConfigurationStarted = true;
    NextShardToConfigure = 0;
    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    const ui32 endShardIndex = (limit == 0)
                                   ? FileStoreConfig.ShardConfigs.size()
                                   : std::min<ui32>(
                                         NextShardToConfigure + limit,
                                         FileStoreConfig.ShardConfigs.size());
    for (ui32 i = 0; i < endShardIndex; ++i) {
        ConfigureShard(ctx, i);
        NextShardToConfigure = i + 1;
    }
}

void TCreateFileStoreActor::ConfigureShard(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvConfigureAsShardRequest>();
    request->Record.SetFileSystemId(
        FileStoreConfig.ShardConfigs[shardIndex].GetFileSystemId());
    request->Record.SetShardNo(shardIndex + 1);
    request->Record.SetMainFileSystemId(Request.GetFileSystemId());
    request->Record.SetDirectoryCreationInShardsEnabled(
        StorageConfig->GetDirectoryCreationInShardsEnabled());
    request->Record.SetStrictFileSystemSizeEnforcementEnabled(
        StorageConfig->GetStrictFileSystemSizeEnforcementEnabled());

    if (StorageConfig->GetDirectoryCreationInShardsEnabled() ||
        StorageConfig->GetStrictFileSystemSizeEnforcementEnabled())
    {
        for (const auto& shard: FileStoreConfig.ShardConfigs) {
            request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
        }
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring shard %s",
        LogTag.c_str(),
        request->Record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request),
        shardIndex   // cookie
    );
}

void TCreateFileStoreActor::ConfigureMainFileStore(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvConfigureShardsRequest>();
    request->Record.SetFileSystemId(
        FileStoreConfig.MainFileSystemConfig.GetFileSystemId());
    request->Record.SetDirectoryCreationInShardsEnabled(
        StorageConfig->GetDirectoryCreationInShardsEnabled());
    request->Record.SetStrictFileSystemSizeEnforcementEnabled(
        StorageConfig->GetStrictFileSystemSizeEnforcementEnabled());

    for (const auto& shard: FileStoreConfig.ShardConfigs) {
        request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring main filesystem %s",
        LogTag.c_str(),
        request->Record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

bool TCreateFileStoreActor::IsShardCreated(const ui32 shardIndex) const
{
    return CreatedShardBitmap && CreatedShardBitmap->Test(shardIndex);
}

void TCreateFileStoreActor::ReadShardCreationState(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();

    request->Record.SetFileSystemId(Request.GetFileSystemId());
    // Unset Version means just return current state.
    request->Record.MutableShardCreationState();

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TCreateFileStoreActor::SetupCreatedShardBitmap()
{
    ShardBitmapBitCount = FileStoreConfig.ShardConfigs.size();
    CreatedShardBitmap =
        std::make_unique<NCloud::TCompressedBitmap>(LoadCompressedBitmap(
            ShardCreationState.GetCreatedShardBitmap(),
            ShardBitmapBitCount));
}

void TCreateFileStoreActor::MergeCreatedShardBitmap(
    const NProtoPrivate::TCompressedBitmapData& bitmap)
{
    Y_DEBUG_ABORT_UNLESS(CreatedShardBitmap);

    for (const auto& chunk: bitmap.GetChunks()) {
        CreatedShardBitmap->Merge(
            {.ChunkIdx = chunk.GetChunkIdx(), .Data = chunk.GetData()});
    }
}

bool TCreateFileStoreActor::HasUnpersistedCreatedShards() const
{
    Y_DEBUG_ABORT_UNLESS(CreatedShardBitmap);

    const auto persisted = LoadCompressedBitmap(
        ShardCreationState.GetCreatedShardBitmap(),
        ShardBitmapBitCount);

    for (ui64 shardIndex = 0; shardIndex < ShardBitmapBitCount; ++shardIndex) {
        if (CreatedShardBitmap->Test(shardIndex) && !persisted.Test(shardIndex))
        {
            return true;
        }
    }

    return false;
}

void TCreateFileStoreActor::UpdateShardCreationState(const TActorContext& ctx)
{
    if (!CreatedShardBitmap) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard bitmap not initialized, "
            "shard creation state unavailable",
            LogTag.c_str());
        return;
    }

    auto request =
        std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();
    request->Record.SetFileSystemId(Request.GetFileSystemId());
    auto* shardCreationState = request->Record.MutableShardCreationState();
    shardCreationState->SetVersion(ShardCreationStateVersion);
    SaveCompressedBitmap(
        *CreatedShardBitmap,
        ShardBitmapBitCount,
        *shardCreationState->MutableCreatedShardBitmap());

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TCreateFileStoreActor::UpdateShardCreatedState(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    Y_DEBUG_ABORT_UNLESS(CreatedShardBitmap);
    if (!CreatedShardBitmap) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard bitmap not initialized, "
            "shard creation state unavailable",
            LogTag.c_str());
        return;
    }

    CreatedShardBitmap->Set(shardIndex, shardIndex + 1);
    UpdateShardCreationState(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateFileStoreActor::HandleCreateFileStoreResponse(
    const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Filesystem creation error: %s",
            LogTag.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (MainFileSystemCreated) {
        Y_ABORT_UNLESS(ev->Cookie < FileStoreConfig.ShardConfigs.size());

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Created shard %s",
            LogTag.c_str(),
            FileStoreConfig.ShardConfigs[ev->Cookie].GetFileSystemId().c_str());

        Y_DEBUG_ABORT_UNLESS(ShardsToCreate);
        --ShardsToCreate;

        if (StorageConfig->GetMaxShardManagementRequestsInFlight()) {
            if (CreatedShardBitmap) {
                UpdateShardCreatedState(ctx, ev->Cookie);
            }

            if (ShardsToCreate > 0 &&
                NextShardToCreate < FileStoreConfig.ShardConfigs.size())
            {
                ContinueCreateShards(ctx, 1);
            }

            if (CreatedShardBitmap) {
                return;
            }
        }

        if (ShardsToCreate == 0) {
            ConfigureShards(ctx);
        }

        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Created main filesystem",
        LogTag.c_str());

    MainFileSystemCreated = true;
    if (ShardsToCreate) {
        if (StorageConfig->GetMaxShardManagementRequestsInFlight()) {
            ReadShardCreationState(ctx);
        } else {
            CreateShards(ctx);
        }
        return;
    }
    if (StorageConfig->GetDirectoryCreationInShardsEnabled() ||
        StorageConfig->GetStrictFileSystemSizeEnforcementEnabled())
    {
        // If no shards are to be created, but directory sharding or allocation
        // of shards of filesystem size is enabled, we need to configure the
        // main filestore in order to set the flags in the main filestore
        ConfigureMainFileStore(ctx);
        return;
    }

    auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>();
    // TODO: fill filestore info

    ReplyAndDie(ctx, std::move(response));
}

void TCreateFileStoreActor::HandleShardCreationStateResponse(
    const TEvIndexTablet::TEvUnsafeChangeTabletStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] UnsafeChangeTabletState failed: %s",
            LogTag.c_str(),
            FormatError(msg->GetError()).Quote().c_str());
        return;
    }

    if (!msg->Record.HasShardCreationState()) {
        if (!InitialShardCreationStateRead) {
            // Rolling upgrade compatibility: the filesystem's old IndexTablet
            // accepts UnsafeChangeTabletState but does not return
            // ShardCreationState yet. Fallback to preexisting non-persistent
            // create flow.
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "[%s] UnsafeChangeTabletState returned no shard creation "
                "state, continuing without persistent shard creation state",
                LogTag.c_str());

            InitialShardCreationStateRead = true;
            CreateShards(ctx);
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "[%s] UnsafeChangeTabletState returned no shard creation state",
                LogTag.c_str());
        }

        return;
    }

    const auto& shardCreationState = msg->Record.GetShardCreationState();
    if (!InitialShardCreationStateRead) {
        ShardCreationState = shardCreationState;
        ShardCreationStateVersion = shardCreationState.GetVersion();
        InitialShardCreationStateRead = true;
        SetupCreatedShardBitmap();
        CreateShards(ctx);
        return;
    }

    if (!CreatedShardBitmap) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard bitmap not initialized, "
            "shard creation state unavailable",
            LogTag.c_str());
        return;
    }

    if (shardCreationState.GetVersion() < ShardCreationStateVersion) {
        return;
    }

    MergeCreatedShardBitmap(shardCreationState.GetCreatedShardBitmap());
    ShardCreationState = shardCreationState;
    ShardCreationStateVersion = shardCreationState.GetVersion();

    if (HasUnpersistedCreatedShards()) {
        UpdateShardCreationState(ctx);
        return;
    }

    if (ShardsToCreate == 0 && !ShardConfigurationStarted) {
        ConfigureShards(ctx);
    }
}

void TCreateFileStoreActor::HandleConfigureShardResponse(
    const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard configuration error: %s",
            LogTag.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Y_ABORT_UNLESS(ev->Cookie < FileStoreConfig.ShardConfigs.size());

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configured shard %s",
        LogTag.c_str(),
        FileStoreConfig.ShardConfigs[ev->Cookie].GetFileSystemId().c_str());

    Y_DEBUG_ABORT_UNLESS(ShardsToConfigure);
    if (--ShardsToConfigure == 0) {
        ConfigureMainFileStore(ctx);
    } else if (StorageConfig->GetMaxShardManagementRequestsInFlight()) {
        if (NextShardToConfigure < FileStoreConfig.ShardConfigs.size()) {
            ConfigureShard(ctx, NextShardToConfigure);
            ++NextShardToConfigure;
        }
    }
}

void TCreateFileStoreActor::HandleConfigureMainFileStoreResponse(
    const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configured main filesystem",
        LogTag.c_str());

    auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>();
    // TODO: fill filestore info

    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TCreateFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TCreateFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvCreateFileStoreResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

void TCreateFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvCreateFileStoreResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TCreateFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvCreateFileStoreResponse,
            HandleCreateFileStoreResponse);
        HFunc(
            TEvIndexTablet::TEvConfigureAsShardResponse,
            HandleConfigureShardResponse);
        HFunc(
            TEvIndexTablet::TEvConfigureShardsResponse,
            HandleConfigureMainFileStoreResponse);
        HFunc(
            TEvIndexTablet::TEvUnsafeChangeTabletStateResponse,
            HandleShardCreationStateResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleCreateFileStore(
    const TEvService::TEvCreateFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->AccessProfileLogRequest(), msg->Record);

    auto error = ValidateCreateFileSystemRequest(msg->Record);
    if (HasError(error)) {
        auto response =
            std::make_unique<TEvService::TEvCreateFileStoreResponse>(error);
        InFlightRequests->CompleteAndErase(
            ctx.Now(),
            error,
            *inflight,
            cookie);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    auto actor = std::make_unique<TCreateFileStoreActor>(
        StorageConfig,
        std::move(requestInfo),
        msg->Record);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
