#include "service_actor.h"

#include "helpers.h"
#include "service_actor_resizeshard.h"
#include "shard_configuration_companion.h"
#include "shard_creation_state_companion.h"
#include "shard_resize_companion.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Resizes a main filesystem and its shards:
// 1. builds the new configs of the main filesystem and of the shards;
// 2. describes the existing shards, their new configs need the versions;
// 3. resizes the main filesystem;
// 4. resizes the existing shards;
// 5. creates the new shards;
// 6. configures the new shards, the existing shards and the main filesystem.
// The steps 2 and 4 are for the strict mode only, in which the shards have the
// size of the main filesystem. The shard requests of a step are limited by
// MaxShardManagementRequestsInFlight.

class TResizeFileStoreActor final
    : public TActorBootstrapped<TResizeFileStoreActor>
    , public IShardCreationOwner
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const NProto::TResizeFileStoreRequest Request;
    const bool DirectoryCreationInShardsEnabled;
    const bool DirectoryCreationInShardsForced;
    const bool StrictFileSystemSizeEnforcementEnabled;

    const NKikimrFileStore::TConfig MainFileStoreOriginalConfig;
    TMultiShardFileStoreConfig FileStoreConfig;

    NProtoPrivate::TShardTopology ShardTopology;
    const ui32 ExistingShardCount;

    TShardResizeCompanion ShardResize;
    TShardCreationStateCompanion ShardCreationState;
    TShardConfigurationCompanion ShardConfiguration;

    const ui64 MainFileStoreCookie = Max<ui64>();

public:
    TResizeFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TResizeFileStoreRequest request,
        NKikimrFileStore::TConfig config,
        const NProtoPrivate::TGetFileSystemTopologyResponse& topology);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void PatchStorageConfig(ui32 maxShardCount);
    NProto::TError BuildFileStoreConfig();
    NProto::TError ValidateShardIds() const;
    NProto::TError PrepareNewShards();

    void DescribeShards(const TActorContext& ctx);
    void ResizeMainFileStore(const TActorContext& ctx);
    void ResizeShards(const TActorContext& ctx);
    void CreateShards(const TActorContext& ctx);
    void CreateShard(
        const TActorContext& ctx,
        ui32 shardIndex) override;
    void ConfigureShards(const TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAlterFileStoreResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateFileStoreResponse(
        const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
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

    TString GetFileSystemIdForLogByCookie(const ui64 cookie) const
    {
        if (cookie == MainFileStoreCookie) {
            return Request.GetFileSystemId();
        }

        if (cookie < FileStoreConfig.ShardConfigs.size()) {
            return FileStoreConfig.ShardConfigs[cookie].GetFileSystemId();
        }

        return "UNKNOWN";
    }
};

////////////////////////////////////////////////////////////////////////////////

TResizeFileStoreActor::TResizeFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TResizeFileStoreRequest request,
        NKikimrFileStore::TConfig config,
        const NProtoPrivate::TGetFileSystemTopologyResponse& topology)
    // Copying is important: the config is shared by the whole service, and
    // some functions here change it in place (e.g. PatchStorageConfig()).
    // Without the copy, a change made for one filesystem would apply to all
    // the filesystems on this host.
    : StorageConfig(std::make_shared<TStorageConfig>(*storageConfig))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , DirectoryCreationInShardsEnabled(
          Request.GetEnableDirectoryCreationInShards() ||
          topology.GetDirectoryCreationInShardsEnabled())
    , DirectoryCreationInShardsForced(
          Request.GetForceDirectoryCreationInShards() ||
          topology.GetForceDirectoryCreationInShards())
    , StrictFileSystemSizeEnforcementEnabled(
          Request.GetEnableStrictFileSystemSizeEnforcement() ||
          topology.GetStrictFileSystemSizeEnforcementEnabled())
    , MainFileStoreOriginalConfig(std::move(config))
    , ShardTopology(GetShardTopology(topology))
    , ExistingShardCount(ShardTopology.ShardsSize())
    , ShardResize(StorageConfig, Request.GetPerformanceProfile())
    , ShardCreationState(
          Request.GetFileSystemId(),
          TShardCreationStateCompanion::EMode::Alter,
          this)
    , ShardConfiguration(
          StorageConfig->GetMaxShardManagementRequestsInFlight())
{
    PatchStorageConfig(topology.GetMaxShardCount());
}

void TResizeFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (auto error = BuildFileStoreConfig(); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Will resize filesystem to have %u shards",
        Request.GetFileSystemId().c_str(),
        FileStoreConfig.ShardConfigs.size());

    if (auto error = ValidateShardIds(); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    if (auto error = PrepareNewShards(); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    DescribeShards(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::PatchStorageConfig(ui32 maxShardCount)
{
    NProto::TStorageConfig protoConfig;
    protoConfig.SetStrictFileSystemSizeEnforcementEnabled(
        StrictFileSystemSizeEnforcementEnabled);
    protoConfig.SetMaxShardCount(maxShardCount);
    StorageConfig->Merge(protoConfig);
}

NProto::TError TResizeFileStoreActor::BuildFileStoreConfig()
{
    NKikimrFileStore::TConfig currentConfig = MainFileStoreOriginalConfig;

    if (currentConfig.GetBlocksCount() > Request.GetBlocksCount() &&
        !Request.GetForce())
    {
        return MakeError(E_ARGUMENT, "Cannot decrease filestore size");
    }

    currentConfig.SetBlocksCount(Request.GetBlocksCount());

    const bool allocateMixed0 = HasMixed0Channel(currentConfig);
    // The filesystem is sharded if automatic shard creation is enabled or if
    // it was created with an explicitly specified shard count
    const bool isSharded = StorageConfig->GetAutomaticShardCreationEnabled() ||
                           Request.GetShardCount() > 0 ||
                           ExistingShardCount > 0;
    if (!allocateMixed0 && isSharded) {
        ui32 shardCount = Request.GetShardCount();
        if (shardCount == 0 &&
            !StorageConfig->GetAutomaticShardCreationEnabled())
        {
            // Keep the current number of shards
            shardCount = ExistingShardCount;
        }
        FileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            currentConfig,
            Request.GetPerformanceProfile(),
            shardCount);
    } else {
        SetupFileStorePerformanceAndChannels(
            allocateMixed0,
            *StorageConfig,
            currentConfig,
            Request.GetPerformanceProfile());

        FileStoreConfig.MainFileSystemConfig = currentConfig;
    }

    if (const auto version = Request.GetConfigVersion()) {
        FileStoreConfig.MainFileSystemConfig.SetVersion(version);
    }

    if (FileStoreConfig.ShardConfigs.size() < ExistingShardCount) {
        if (!StrictFileSystemSizeEnforcementEnabled ||
            Request.GetShardCount())
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Cannot decrease number of shards: current="
                    << ExistingShardCount
                    << ", requested="
                    << FileStoreConfig.ShardConfigs.size());
        }

        // The shards are kept in the strict mode: all of them are resized to
        // the size of the main filesystem.
        FileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            FileStoreConfig.MainFileSystemConfig,
            Request.GetPerformanceProfile(),
            ExistingShardCount);
    }

    if (StrictFileSystemSizeEnforcementEnabled) {
        for (auto& shard: FileStoreConfig.ShardConfigs) {
            shard.SetBlocksCount(Request.GetBlocksCount());
        }
    }

    return {};
}

NProto::TError TResizeFileStoreActor::ValidateShardIds() const
{
    for (ui32 i = 0; i < ExistingShardCount; ++i) {
        if (ShardTopology.GetShards(i).GetShardId() !=
            FileStoreConfig.ShardConfigs[i].GetFileSystemId())
        {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Shard FileSystemId mismatch at pos " << i << ": "
                    << ShardTopology.GetShards(i).GetShardId() << " != "
                    << FileStoreConfig.ShardConfigs[i].GetFileSystemId());
        }
    }

    return {};
}

NProto::TError TResizeFileStoreActor::PrepareNewShards()
{
    const auto& shardConfigs = FileStoreConfig.ShardConfigs;

    const auto error = ShardCreationState.InitializeShardCreation(
        ExistingShardCount,
        shardConfigs.size(),
        StorageConfig->GetMaxShardManagementRequestsInFlight());
    if (HasError(error)) {
        return error;
    }

    for (ui32 i = ExistingShardCount; i < shardConfigs.size(); ++i) {
        AddOrdinaryShard(shardConfigs[i].GetFileSystemId(), ShardTopology);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::DescribeShards(const TActorContext& ctx)
{
    // The shards keep their size unless the strict mode is on.
    if (StrictFileSystemSizeEnforcementEnabled) {
        const auto& shardConfigs = FileStoreConfig.ShardConfigs;
        ShardResize.InitializeShardResize(
            {shardConfigs.begin(), shardConfigs.begin() + ExistingShardCount});
    }

    ShardResize.StartShardDescription(ctx);
    if (!ShardResize.HasShardsToDescribe()) {
        ResizeMainFileStore(ctx);
    }
}

void TResizeFileStoreActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Describing filestore %s (cookie: %lu) failed: %s",
            Request.GetFileSystemId().c_str(),
            GetFileSystemIdForLogByCookie(ev->Cookie).Quote().c_str(),
            ev->Cookie,
            FormatError(msg->GetError()).c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto error = ShardResize.OnShardDescribed(
        ctx,
        ev->Cookie,
        msg->PathDescription.GetFileStoreDescription().GetConfig());
    if (HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    if (!ShardResize.HasShardsToDescribe()) {
        ResizeMainFileStore(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::ResizeMainFileStore(const TActorContext& ctx)
{
    FileStoreConfig.MainFileSystemConfig.ClearBlockSize();
    FileStoreConfig.MainFileSystemConfig.SetAlterTs(ctx.Now().MicroSeconds());
    auto request = std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(
        FileStoreConfig.MainFileSystemConfig);
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        MainFileStoreCookie);
}

void TResizeFileStoreActor::ResizeShards(const TActorContext& ctx)
{
    ShardResize.StartShardResize(ctx);
    if (!ShardResize.HasShardsToResize()) {
        CreateShards(ctx);
    }
}

void TResizeFileStoreActor::HandleAlterFileStoreResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Altering of filestore %s (cookie: %lu) failed: %s",
            Request.GetFileSystemId().c_str(),
            GetFileSystemIdForLogByCookie(ev->Cookie).Quote().c_str(),
            ev->Cookie,
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (ev->Cookie == MainFileStoreCookie) {
        ResizeShards(ctx);
        return;
    }

    const auto error = ShardResize.OnShardResized(ctx, ev->Cookie);
    if (HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    if (!ShardResize.HasShardsToResize()) {
        CreateShards(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::CreateShards(const TActorContext& ctx)
{
    ShardCreationState.StartShardCreation(ctx);

    if (!ShardCreationState.HasShardsToCreate()) {
        ConfigureShards(ctx);
    }
}

void TResizeFileStoreActor::CreateShard(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        FileStoreConfig.ShardConfigs[shardIndex]);

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Creating shard %s",
        Request.GetFileSystemId().c_str(),
        request->Config.GetFileSystemId().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        shardIndex   // cookie
    );
}

void TResizeFileStoreActor::HandleCreateFileStoreResponse(
    const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto shardIndex = ev->Cookie;

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard %s (cookie: %lu) creation failed: %s",
            Request.GetFileSystemId().c_str(),
            GetFileSystemIdForLogByCookie(shardIndex).Quote().c_str(),
            shardIndex,
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Created shard %s",
        Request.GetFileSystemId().c_str(),
        GetFileSystemIdForLogByCookie(shardIndex).Quote().c_str());

    const auto error = ShardCreationState.OnShardCreated(
        ctx,
        shardIndex);
    if (HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    if (!ShardCreationState.HasShardsToCreate()) {
        ConfigureShards(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::ConfigureShards(const TActorContext& ctx)
{
    const auto& shardConfigs = FileStoreConfig.ShardConfigs;

    // The shards and the main filesystem know everything already if the
    // request neither adds shards nor enables a mode.
    if (shardConfigs.size() == ExistingShardCount &&
        !Request.GetEnableStrictFileSystemSizeEnforcement() &&
        !Request.GetEnableDirectoryCreationInShards())
    {
        ReplyAndDie(ctx);
        return;
    }

    NProtoPrivate::TConfigureShardsRequest request;
    request.SetFileSystemId(Request.GetFileSystemId());
    request.SetDirectoryCreationInShardsEnabled(
        DirectoryCreationInShardsEnabled);
    request.SetForceDirectoryCreationInShards(DirectoryCreationInShardsForced);
    request.SetStrictFileSystemSizeEnforcementEnabled(
        StrictFileSystemSizeEnforcementEnabled);
    for (const auto& shard: shardConfigs) {
        request.AddShardFileSystemIds(shard.GetFileSystemId());
    }
    *request.MutableShardTopology() = ShardTopology;

    ShardConfiguration.InitializeShardConfiguration(
        std::move(request),
        ExistingShardCount);
    ShardConfiguration.StartShardConfiguration(ctx);
    if (!ShardConfiguration.HasShardsToConfigure()) {
        ShardConfiguration.ConfigureMainFileStore(ctx);
    }
}

void TResizeFileStoreActor::HandleConfigureShardResponse(
    const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard %s (cookie: %lu) configuration failed: %s",
            Request.GetFileSystemId().c_str(),
            GetFileSystemIdForLogByCookie(ev->Cookie).Quote().c_str(),
            ev->Cookie,
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configured shard %s",
        Request.GetFileSystemId().c_str(),
        GetFileSystemIdForLogByCookie(ev->Cookie).c_str());

    const auto error = ShardConfiguration.OnShardConfigured(ctx, ev->Cookie);
    if (HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    if (!ShardConfiguration.HasShardsToConfigure()) {
        ShardConfiguration.ConfigureMainFileStore(ctx);
    }
}

void TResizeFileStoreActor::HandleConfigureMainFileStoreResponse(
    const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configuring main filesystem failed: %s",
            Request.GetFileSystemId().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configured main filesystem",
        Request.GetFileSystemId().c_str());

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TResizeFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvResizeFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

STFUNC(TResizeFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvDescribeFileStoreResponse,
            HandleDescribeFileStoreResponse);
        HFunc(
            TEvSSProxy::TEvAlterFileStoreResponse,
            HandleAlterFileStoreResponse);
        HFunc(
            TEvSSProxy::TEvCreateFileStoreResponse,
            HandleCreateFileStoreResponse);
        HFunc(
            TEvIndexTablet::TEvConfigureAsShardResponse,
            HandleConfigureShardResponse);
        HFunc(
            TEvIndexTablet::TEvConfigureShardsResponse,
            HandleConfigureMainFileStoreResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////
// Describes the filestore, gets its topology and starts the actor that resizes
// either a shard or a main filesystem with its shards.

class TResizeRequestActor final
    : public TActorBootstrapped<TResizeRequestActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const NProto::TResizeFileStoreRequest Request;

    NKikimrFileStore::TConfig Config;

public:
    TResizeRequestActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TResizeFileStoreRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetFileSystemTopologyResponse(
        const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TResizeRequestActor::TResizeRequestActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TResizeFileStoreRequest request)
    : StorageConfig(std::move(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TResizeRequestActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        Request.GetFileSystemId());
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TResizeRequestActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Describing filestore failed: %s",
            Request.GetFileSystemId().c_str(),
            FormatError(msg->GetError()).c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Config = msg->PathDescription.GetFileStoreDescription().GetConfig();

    auto request =
        std::make_unique<TEvIndexTablet::TEvGetFileSystemTopologyRequest>();
    request->Record.SetFileSystemId(Request.GetFileSystemId());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

void TResizeRequestActor::HandleGetFileSystemTopologyResponse(
    const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Getting file system topology failed: %s",
            Request.GetFileSystemId().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (msg->Record.GetShardNo()) {
        NCloud::Register(
            ctx,
            CreateResizeShardActor(
                StorageConfig,
                RequestInfo,
                Request,
                std::move(Config),
                msg->Record));
    } else {
        NCloud::Register<TResizeFileStoreActor>(
            ctx,
            StorageConfig,
            RequestInfo,
            Request,
            std::move(Config),
            msg->Record);
    }

    Die(ctx);
}

void TResizeRequestActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TResizeRequestActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvResizeFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

STFUNC(TResizeRequestActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvDescribeFileStoreResponse,
            HandleDescribeFileStoreResponse);
        HFunc(
            TEvIndexTablet::TEvGetFileSystemTopologyResponse,
            HandleGetFileSystemTopologyResponse);

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

void TStorageServiceActor::HandleResizeFileStore(
    const TEvService::TEvResizeFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->AccessProfileLogRequest(), msg->Record);

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    auto actor = std::make_unique<TResizeRequestActor>(
        StorageConfig,
        std::move(requestInfo),
        msg->Record);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
