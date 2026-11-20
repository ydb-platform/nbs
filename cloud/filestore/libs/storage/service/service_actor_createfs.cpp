#include "service_actor.h"

#include "service_actor_fastshard.h"
#include "shard_creation_state_companion.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/bitops.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Without an explicit count a filesystem gets the fast shards its size needs.
ui64 GetFastShardCount(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request)
{
    if (!request.HasFastShardStorageKind()) {
        return 0;
    }

    if (request.GetFastShardCount()) {
        return request.GetFastShardCount();
    }

    return CalculateFastShardCount(
        config,
        request.GetBlocksCount(),
        request.GetBlockSize());
}

NProto::TError ValidateFastShards(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request)
{
    if (request.GetFastShardCount() && !request.HasFastShardStorageKind()) {
        return MakeError(E_ARGUMENT, "fast shard count needs a storage kind");
    }

    if (!request.HasFastShardStorageKind()) {
        return {};
    }

    if (!config.GetDirectoryCreationInShardsEnabled()) {
        return MakeError(
            E_PRECONDITION_FAILED,
            "fast shards need directory creation in shards");
    }

    // The directories are not created in the fast shards.
    if (!request.GetShardCount()) {
        return MakeError(
            E_ARGUMENT,
            "fast shards need an explicit shard count");
    }

    const auto storageKind = request.GetFastShardStorageKind();
    if (!NProto::EFastShardStorageKind_IsValid(storageKind)) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "invalid fast shard storage kind: "
                << static_cast<int>(storageKind));
    }

    const ui64 requiredFastShardCount = CalculateFastShardCount(
        config,
        request.GetBlocksCount(),
        request.GetBlockSize());
    if (request.GetFastShardCount() &&
        request.GetFastShardCount() < requiredFastShardCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "fast shard count " << request.GetFastShardCount()
                << " cannot hold the filesystem, " << requiredFastShardCount
                << " needed");
    }

    return {};
}

NProto::TError ValidateTotalShards(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request)
{
    ui64 shardCount = request.GetShardCount();
    shardCount += GetFastShardCount(config, request);
    if (shardCount > config.GetMaxShardCount()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "shard count " << shardCount << " should not be bigger than "
                << config.GetMaxShardCount());
    }

    return {};
}

NProto::TError ValidateCreateFileSystemRequest(
    const TStorageConfig& config,
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

    // The block arithmetic all over the tablet and the client masks by the
    // block size, so it has to be a power of two.
    ui32 blockSize = request.GetBlockSize();
    if (!IsPowerOf2(blockSize)
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

    if (auto error = ValidateFastShards(config, request); HasError(error)) {
        return error;
    }

    return ValidateTotalShards(config, request);
}

////////////////////////////////////////////////////////////////////////////////

class TCreateFileStoreActor final
    : public TActorBootstrapped<TCreateFileStoreActor>
    , public IShardCreationOwner
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const NProto::TCreateFileStoreRequest Request;
    const TString& LogTag;
    const ui32 FastShardCount;
    TMultiShardFileStoreConfig FileStoreConfig;
    NProtoPrivate::TShardTopology ShardTopology;

    bool MainFileSystemCreated = false;
    ui32 NextShardToConfigure = 0;
    ui32 ShardsToConfigure = 0;
    bool ShardConfigurationStarted = false;

    TShardCreationStateCompanion ShardCreationState;

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
    void CreateShard(
        const TActorContext& ctx,
        ui32 shardIndex) override;
    void AllocateFastShards(const TActorContext& ctx);
    void ConfigureShards(const TActorContext& ctx);
    void ConfigureShard(const TActorContext& ctx, const ui32 shardIndex);
    void ConfigureMainFileStore(const TActorContext& ctx);

    void HandleCreateFileStoreResponse(
        const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleShardCreationStateResponse(
        const TEvIndexTablet::TEvUnsafeChangeTabletStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleFastShardsAllocated(
        const TEvServicePrivate::TEvFastShardsAllocated::TPtr& ev,
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
    , FastShardCount(GetFastShardCount(*StorageConfig, Request))
    , ShardCreationState(
          Request.GetFileSystemId(),
          TShardCreationStateCompanion::EMode::Create,
          this)
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
            Request.GetShardCount() + FastShardCount);
        const auto error = ShardCreationState.InitializeShardCreation(
            0,
            FileStoreConfig.ShardConfigs.size(),
            StorageConfig->GetMaxShardManagementRequestsInFlight());
        // InitializeShardCreation can't return an error here,
        // current shard count is zero while creating new FS.
        Y_DEBUG_ABORT_UNLESS(!HasError(error));

        ShardsToConfigure = FileStoreConfig.ShardConfigs.size();
        config = FileStoreConfig.MainFileSystemConfig;

        // TODO(#8034): use the main filestore config topology once it is there
        // The fast shards are last, their entries come after allocation.
        const auto& shardConfigs = FileStoreConfig.ShardConfigs;
        ui32 fastShardsBegin = shardConfigs.size() - FastShardCount;
        for (ui32 i = 0; i < fastShardsBegin; ++i) {
            AddOrdinaryShard(shardConfigs[i].GetFileSystemId(), ShardTopology);
        }

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
    if (ShardCreationState.IsPersistentStateSupported()) {
        // Unlike resize, create has no existing shards to load from topology.
        // The target configs are already known when shard creation starts.
        const auto error = ShardCreationState.SetupCreatedShardBitmap(
            0,
            FileStoreConfig.ShardConfigs);
        if (HasError(error)) {
            ReplyAndDie(ctx, error);
            return;
        }
    }

    ShardCreationState.StartShardCreation(ctx);

    if (!ShardCreationState.HasShardsToCreate()) {
        AllocateFastShards(ctx);
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

void TCreateFileStoreActor::AllocateFastShards(const TActorContext& ctx)
{
    if (!FastShardCount) {
        ConfigureShards(ctx);
        return;
    }

    NCloud::Register(
        ctx,
        CreateAllocateFastShardsActor(
            StorageConfig,
            SelfId(),
            LogTag,
            FileStoreConfig.MainFileSystemConfig,
            FileStoreConfig.ShardConfigs,
            FastShardCount,
            Request.GetFastShardStorageKind()));
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
        *request->Record.MutableShardTopology() = ShardTopology;
    }

    const auto& shard = ShardTopology.GetShards(shardIndex);
    if (shard.HasFastShardConfig()) {
        request->Record.SetIsFastShard(true);
        *request->Record.MutableFastShardConfig() = shard.GetFastShardConfig();
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

    request->Record.MutableShardFileSystemIds()->Reserve(FileStoreConfig.ShardConfigs.size());
    for (const auto& shard: FileStoreConfig.ShardConfigs) {
        request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
    }
    *request->Record.MutableShardTopology() = ShardTopology;

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
        const auto shardIndex = ev->Cookie;
        Y_ABORT_UNLESS(shardIndex < FileStoreConfig.ShardConfigs.size());

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Created shard %s",
            LogTag.c_str(),
            FileStoreConfig.ShardConfigs[shardIndex].GetFileSystemId().c_str());

        const auto error = ShardCreationState.OnShardCreated(
            ctx,
            shardIndex);
        if (HasError(error)) {
            ReplyAndDie(ctx, error);
            return;
        }

        if (ShardCreationState.HasCreatedShardBitmap()) {
            // Wait for HandleShardCreationStateResponse to call ConfigureShards
            // after all shards are created and their bits are persisted in the
            // Tablet's shard creation bitmap.
            return;
        }

        if (!ShardCreationState.HasShardsToCreate()) {
            AllocateFastShards(ctx);
        }

        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Created main filesystem",
        LogTag.c_str());

    MainFileSystemCreated = true;
    if (ShardCreationState.HasShardsToCreate()) {
        if (StorageConfig->GetMaxShardManagementRequestsInFlight()) {
            ShardCreationState.ReadShardCreationState(ctx);
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
    const auto result = ShardCreationState.HandleShardCreationStateResponse(
        ev->Get()->Record,
        ctx);
    if (HasError(result)) {
        ReplyAndDie(ctx, result.GetError());
        return;
    }

    switch (result.GetResult()) {
        case TShardCreationStateCompanion::
            EChangeTabletStateResponseAction::StartWithoutPersistentState:
        case TShardCreationStateCompanion::EChangeTabletStateResponseAction::InitialStateLoaded:
            CreateShards(ctx);
            return;
        case TShardCreationStateCompanion::EChangeTabletStateResponseAction::IgnoreStaleResponse:
            // A newer state is already saved. Discarding this stale response
            // does not stall the operation.
            return;
        case TShardCreationStateCompanion::EChangeTabletStateResponseAction::PersistLocalProgress:
            // The returned bitmap lacks local progress. The update response
            // will resume the operation after the merged bits are persisted.
            ShardCreationState.UpdateShardCreationState(ctx);
            return;
        case TShardCreationStateCompanion::EChangeTabletStateResponseAction::Continue:
            if (!ShardCreationState.HasShardsToCreate() &&
                !ShardConfigurationStarted)
            {
                ConfigureShards(ctx);
            }
            return;
    }
}

void TCreateFileStoreActor::HandleFastShardsAllocated(
    const TEvServicePrivate::TEvFastShardsAllocated::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    ShardTopology.MergeFrom(msg->Topology);

    ConfigureShards(ctx);
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
    auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>(
        error);

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
            TEvServicePrivate::TEvFastShardsAllocated,
            HandleFastShardsAllocated);
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

    auto error = ValidateCreateFileSystemRequest(*StorageConfig, msg->Record);
    if (HasError(error)) {
        auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>(
            error);

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
