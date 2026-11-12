#include "service_actor.h"

#include "helpers.h"
#include "service_actor_fastshard.h"
#include "shard_creation_state_companion.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/model/channel_data_kind.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////
// This class actually performs two different actions: alter and resize
// In order not mix them we have two different state functions: AlterStateWork,
// ResizeStateWork. Alter mode is quite simple and consits of the following
// steps: Describe main filestore, Alter main filestore
// The steps of the resize mode:
// 1. Describe main filestore. Store NKikimrFileStore::TConfig of the main
// filesystem for later use.
// 2. Get storage statistics to determine SevenBytesHandlesCount.
// We need this to prohibit a user from creating more than 255 shards
// if the filesystem has obsolete handles (with a non-zero seventh byte).
// 3. Get filesystem topology. Using the topology and the config from the
// previous step, calculate all parameters of the transformation performed by
// resize action: ShardsToCreate, ShardsToConfigure, ShardsToAlter,
// ShouldConfigureMainFileStore.
// 4. Describe shards. We need this step to get config version of shards in case
// we need to resize them. If MaxShardManagementRequestsInFlight is non-zero, we
// cap number of in-flight requests, emitting new requests on each response.
// If MaxShardManagementRequestsInFlight is zero, we send requests for all
// shards at once.
// 5. Alter (actually resize) main filestore.
// 6. Alter shards (if we resize them). If MaxShardManagementRequestsInFlight is
// non-zero, we cap number of in-flight requests.
// 7. Create shards if needed. If MaxShardManagementRequestsInFlight is
// non-zero, we cap number of in-flight requests.
// 8. Allocate devices for the new fast shards if needed.
// 9. Configure shards if we created some new ones. Done in two phases: the
// newly created shards first, the pre-existing shards next.
// If MaxShardManagementRequestsInFlight is non-zero, we cap number of in-flight
// requests within each phase.
// 10. Configure main filestore if new shards were created.
// The end!

class TAlterFileStoreActor final
    : public TActorBootstrapped<TAlterFileStoreActor>
    , public IShardCreationOwner
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const TString FileSystemId;
    const NProto::TFileStorePerformanceProfile PerformanceProfile;
    const bool Alter;
    const bool Force;
    const bool EnableDirectoryCreationInShards;
    const bool ForceDirectoryCreationInShards;
    const bool EnableStrictFileSystemSizeEnforcement;

    const ui32 ExplicitOrdinaryShardCount;
    const ui32 ExplicitFastShardCount;

    NKikimrFileStore::TConfig TargetConfig;
    NKikimrFileStore::TConfig MainFileStoreOriginalConfig;

    TMultiShardFileStoreConfig FileStoreConfig;

    NProtoPrivate::TShardTopology ShardTopology;

    ui32 ExistingOrdinaryShardCount = 0;
    ui32 ExistingFastShardCount = 0;
    ui32 ExistingTotalShardCount = 0;

    ui32 NewFastShardCount = 0;

    ui32 NextShardToConfigure = 0;
    ui32 ShardsToConfigure = 0;
    ui32 EndShardToConfigure = 0;

    enum class EShardConfigPhase {
        NewShards,
        OldShards,
    };
    EShardConfigPhase ShardConfigPhase = EShardConfigPhase::NewShards;

    ui32 NextShardToAlter = 0;
    ui32 ShardsToAlter = 0;

    ui32 NextShardToDescribe = 0;
    ui32 ShardsToDescribe = 0;

    ui32 MaxShardCount = 0;
    ui64 SevenBytesHandlesCount = 0;

    TShardCreationStateCompanion ShardCreationState;

    // These flags are set by HandleGetFileSystemTopologyResponse.
    bool DirectoryCreationInShardsEnabled = false;
    bool DirectoryCreationInShardsForced = false;
    bool StrictFileSystemSizeEnforcementEnabled = false;
    bool ShouldConfigureMainFileStore = false;

    const ui64 MainFileStoreCookie = Max<ui64>();

public:
    TAlterFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        const NProto::TAlterFileStoreRequest& request);

    TAlterFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        const NProto::TResizeFileStoreRequest& request);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(ResizeStateWork);
    STFUNC(AlterStateWork);

    void DescribeMainFileStore(const TActorContext& ctx);
    void DescribeShards(const TActorContext& ctx);
    void DescribeShard(const TActorContext& ctx, const ui32 shardIndex);
    void GetStorageStats(const TActorContext& ctx);
    void AlterFileStore(const TActorContext& ctx);
    void AlterShards(const TActorContext& ctx);
    void AlterShard(const TActorContext& ctx, const ui32 shardIndex);
    void GetFileSystemTopology(const TActorContext& ctx);
    void CreateShards(const TActorContext& ctx);
    void CreateShard(
        const TActorContext& ctx,
        ui32 shardIndex) override;
    void AllocateFastShards(const TActorContext& ctx);
    void ConfigureShards(const TActorContext& ctx);
    void ConfigureShardRange(
        const TActorContext& ctx,
        ui32 beginShardIndex,
        ui32 endShardIndex,
        EShardConfigPhase phase);
    void ConfigureShard(const TActorContext& ctx, const ui32 shardIndex);
    void ConfigureMainFileStore(const TActorContext& ctx);

    void PatchStorageConfig();
    NProto::TError ValidateOrdinaryShards() const;
    NProto::TError ValidateFastShards() const;
    NProto::TError ValidateTotalShards(ui64 totalShardCount) const;
    void FillMultiShardFileStoreConfig(
        const TActorContext& ctx,
        ui32 shardCount);

    void HandleDescribeFileStoreForAlterResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleFastShardsAllocated(
        const TEvServicePrivate::TEvFastShardsAllocated::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetStorageStatsResponse(
        const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetFileSystemTopologyResponse(
        const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
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

    void HandleAlterFileStoreResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAlterFileStoreForAlterResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleResizeFileStoreResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ReportInvalidCookieAndDie(const TActorContext& ctx, ui64 cookie);

    const char* GetOperationString() const
    {
        return !Alter ? "resize" : "alter";
    }

    bool IsCookieValid(const ui64 cookie) const
    {
        return cookie == MainFileStoreCookie ||
               cookie < FileStoreConfig.ShardConfigs.size();
    }

    TString GetFileSystemIdForLogByCookie(const ui64 cookie) const
    {
        if (!IsCookieValid(cookie)) {
            return "UNKNOWN";
        }

        if (cookie == MainFileStoreCookie) {
            return FileSystemId;
        }

        return FileStoreConfig.ShardConfigs[cookie].GetFileSystemId();
    }
};

////////////////////////////////////////////////////////////////////////////////

TAlterFileStoreActor::TAlterFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        const NProto::TAlterFileStoreRequest& request)
    // Copying is important: the config is shared by the whole service, and
    // some functions here change it in place (e.g. PatchStorageConfig()).
    // Without the copy, a change made for one filesystem would apply to all
    // the filesystems on this host.
    : StorageConfig(std::make_shared<TStorageConfig>(*storageConfig))
    , RequestInfo(std::move(requestInfo))
    , FileSystemId(request.GetFileSystemId())
    , Alter(true)
    , Force(false)
    , EnableDirectoryCreationInShards(false)
    , ForceDirectoryCreationInShards(false)
    , EnableStrictFileSystemSizeEnforcement(false)
    , ExplicitOrdinaryShardCount(0)
    , ExplicitFastShardCount(0)
    , ShardCreationState(
          FileSystemId,
          TShardCreationStateCompanion::EMode::Alter,
          this)
{
    TargetConfig.SetCloudId(request.GetCloudId());
    TargetConfig.SetFolderId(request.GetFolderId());
    TargetConfig.SetProjectId(request.GetProjectId());
    TargetConfig.SetVersion(request.GetConfigVersion());
}

TAlterFileStoreActor::TAlterFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        const NProto::TResizeFileStoreRequest& request)
    // Copying is important: the config is shared by the whole service, and
    // some functions here change it in place (e.g. PatchStorageConfig()).
    // Without the copy, a change made for one filesystem would apply to all
    // the filesystems on this host.
    : StorageConfig(std::make_shared<TStorageConfig>(*storageConfig))
    , RequestInfo(std::move(requestInfo))
    , FileSystemId(request.GetFileSystemId())
    , PerformanceProfile(request.GetPerformanceProfile())
    , Alter(false)
    , Force(request.GetForce())
    , EnableDirectoryCreationInShards(
          request.GetEnableDirectoryCreationInShards())
    , ForceDirectoryCreationInShards(
        request.GetForceDirectoryCreationInShards())
    , EnableStrictFileSystemSizeEnforcement(
          request.GetEnableStrictFileSystemSizeEnforcement())
    , ExplicitOrdinaryShardCount(request.GetShardCount())
    , ExplicitFastShardCount(request.GetFastShardCount())
    , ShardCreationState(
          FileSystemId,
          TShardCreationStateCompanion::EMode::Alter,
          this)
{
    TargetConfig.SetBlocksCount(request.GetBlocksCount());
    TargetConfig.SetVersion(request.GetConfigVersion());
}

void TAlterFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    if (Alter) {
        Become(&TThis::AlterStateWork);
    } else {
        Become(&TThis::ResizeStateWork);
    }

    DescribeMainFileStore(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::DescribeMainFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        FileSystemId);
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        MainFileStoreCookie);
}

void TAlterFileStoreActor::DescribeShards(const TActorContext& ctx)
{
    if (ShardsToDescribe == 0) {
        AlterFileStore(ctx);
        return;
    }

    NextShardToDescribe = 0;
    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    const ui32 endShardIndex =
        (limit == 0)
            ? ShardsToDescribe
            : std::min<ui32>(NextShardToDescribe + limit, ShardsToDescribe);

    for (ui32 i = 0; i < endShardIndex; ++i) {
        DescribeShard(ctx, i);
        NextShardToDescribe = i + 1;
    }
}

void TAlterFileStoreActor::DescribeShard(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        FileStoreConfig.ShardConfigs[shardIndex].GetFileSystemId());

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), shardIndex);
}

void TAlterFileStoreActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const bool isCookieValid = IsCookieValid(ev->Cookie);
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Describing filestore %s (cookie: %lu) failed: %s",
            FileSystemId.c_str(),
            GetFileSystemIdForLogByCookie(ev->Cookie).Quote().c_str(),
            ev->Cookie,
            FormatError(msg->GetError()).c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (!isCookieValid) {
        ReportInvalidCookieAndDie(ctx, ev->Cookie);
        return;
    }

    const auto& fileStore = msg->PathDescription.GetFileStoreDescription();
    NKikimrFileStore::TConfig currentConfig = fileStore.GetConfig();

    // A shard is described
    if (ev->Cookie != MainFileStoreCookie) {
        const ui64 shardNo = ev->Cookie;
        const ui64 blocksCount = FileStoreConfig.ShardConfigs[shardNo]
            .GetBlocksCount();

        FileStoreConfig.ShardConfigs[shardNo] = currentConfig;
        FileStoreConfig.ShardConfigs[shardNo].SetBlocksCount(blocksCount);
        // We need to reconfigure the shard, as only at this point do we know
        // the original number of channels, and the calculated number of
        // channels may be less than the real.
        SetupFileStorePerformanceAndChannels(
            false /* allocateMixed0Channel */,
            *StorageConfig,
            FileStoreConfig.ShardConfigs[shardNo],
            PerformanceProfile);

        FileStoreConfig.ShardConfigs[shardNo].SetVersion(
            fileStore.GetConfig().GetVersion());

        Y_ABORT_UNLESS(ShardsToDescribe);
        if (--ShardsToDescribe == 0) {
            AlterFileStore(ctx);
        } else if (StorageConfig->GetMaxShardManagementRequestsInFlight()) {
            if (NextShardToDescribe < ExistingTotalShardCount) {
                DescribeShard(ctx, NextShardToDescribe);
                ++NextShardToDescribe;
            }
        }
        return;
    }

    MainFileStoreOriginalConfig = currentConfig;

    GetStorageStats(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::GetStorageStats(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvGetStorageStatsRequest>();
    request->Record.SetFileSystemId(FileSystemId);

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TAlterFileStoreActor::HandleGetStorageStatsResponse(
    const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Getting file system statistics failed: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    SevenBytesHandlesCount = msg->Record.GetStats().GetSevenBytesHandlesCount();

    GetFileSystemTopology(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::PatchStorageConfig()
{
    NProto::TStorageConfig protoConfig;
    protoConfig.SetStrictFileSystemSizeEnforcementEnabled(
        StrictFileSystemSizeEnforcementEnabled);
    protoConfig.SetMaxShardCount(MaxShardCount);
    StorageConfig->Merge(protoConfig);
}

NProto::TError TAlterFileStoreActor::ValidateOrdinaryShards() const
{
    if (ExplicitOrdinaryShardCount &&
        ExplicitOrdinaryShardCount < ExistingOrdinaryShardCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Cannot decrease number of shards: current="
                << ExistingOrdinaryShardCount << ", requested="
                << ExplicitOrdinaryShardCount);
    }

    return {};
}

NProto::TError TAlterFileStoreActor::ValidateFastShards() const
{
    if (!ExistingFastShardCount) {
        if (ExplicitFastShardCount) {
            return MakeError(
                E_ARGUMENT,
                "Cannot add fast shards to a filesystem created without them.");
        }

        // nothing to do
        return {};
    }

    if (!StorageConfig->GetFastShardRuntimeEnabled()) {
        return MakeError(E_PRECONDITION_FAILED, "fast shards are disabled");
    }

    if (TargetConfig.GetBlocksCount() != MainFileStoreOriginalConfig.GetBlocksCount()) {
        return MakeError(
            E_ARGUMENT,
            "Cannot resize fast shard.");
    }

    if (ExplicitFastShardCount &&
        ExplicitFastShardCount < ExistingFastShardCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Cannot decrease number of fast shards: current="
                << ExistingFastShardCount << ", requested="
                << ExplicitFastShardCount);
    }

    return ValidateFastShardCapacity(
        *StorageConfig,
        MainFileStoreOriginalConfig.GetBlocksCount() *
            MainFileStoreOriginalConfig.GetBlockSize());
}

NProto::TError TAlterFileStoreActor::ValidateTotalShards(
    ui64 totalShardCount) const
{
    // Only the shards that the request adds are checked against the limit.
    if (totalShardCount > ExistingTotalShardCount &&
        totalShardCount > MaxShardCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "shard count " << totalShardCount
                << " should not be bigger than " << MaxShardCount);
    }

    return {};
}

void TAlterFileStoreActor::FillMultiShardFileStoreConfig(
    const TActorContext& ctx,
    ui32 shardCount)
{
    NKikimrFileStore::TConfig currentConfig = MainFileStoreOriginalConfig;

    // Allocate legacy mixed0 channel in case it was already present
    Y_ABORT_UNLESS(currentConfig.ExplicitChannelProfilesSize() >= 4);
    const auto thirdChannelDataKind = static_cast<EChannelDataKind>(
        currentConfig.GetExplicitChannelProfiles(3)
            .GetDataKind());
    const bool allocateMixed0 =
        thirdChannelDataKind == EChannelDataKind::Mixed0;

    if (currentConfig.GetBlocksCount() > TargetConfig.GetBlocksCount() &&
        !Force)
    {
        ReplyAndDie(
            ctx,
            MakeError(E_ARGUMENT, "Cannot decrease filestore size"));
        return;
    }

    Y_ABORT_UNLESS(
        TargetConfig.GetCloudId().empty() &&
        TargetConfig.GetFolderId().empty() &&
        TargetConfig.GetProjectId().empty());

    currentConfig.SetBlocksCount(TargetConfig.GetBlocksCount());
    // The filesystem is sharded if automatic shard creation is enabled or if
    // it was created with an explicitly specified shard count
    const bool isSharded = StorageConfig->GetAutomaticShardCreationEnabled() ||
                           shardCount > 0;
    if (!allocateMixed0 && isSharded) {
        FileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            currentConfig,
            PerformanceProfile,
            shardCount);
    } else {
        SetupFileStorePerformanceAndChannels(
            allocateMixed0,
            *StorageConfig,
            currentConfig,
            PerformanceProfile);

        FileStoreConfig.MainFileSystemConfig = currentConfig;
    }

    if (const auto version = TargetConfig.GetVersion()) {
        FileStoreConfig.MainFileSystemConfig.SetVersion(version);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::AlterFileStore(const TActorContext& ctx)
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

void TAlterFileStoreActor::AlterShards(const TActorContext& ctx)
{
    if (ShardsToAlter == 0) {
        CreateShards(ctx);
        return;
    }

    NextShardToAlter = 0;
    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    const ui32 endShardIndex =
        (limit == 0) ? ShardsToAlter
                     : std::min<ui32>(NextShardToAlter + limit, ShardsToAlter);

    for (ui32 i = 0; i < endShardIndex; ++i) {
        AlterShard(ctx, i);
        NextShardToAlter = i + 1;
    }
}

void TAlterFileStoreActor::AlterShard(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    FileStoreConfig.ShardConfigs[shardIndex].ClearBlockSize();
    FileStoreConfig.ShardConfigs[shardIndex].SetAlterTs(
        ctx.Now().MicroSeconds());
    auto request = std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(
        FileStoreConfig.ShardConfigs[shardIndex]);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), shardIndex);
}

void TAlterFileStoreActor::HandleAlterFileStoreResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const bool isCookieValid = IsCookieValid(ev->Cookie);
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Altering of filestore %s (cookie: %lu) failed: %s",
            FileSystemId.c_str(),
            GetFileSystemIdForLogByCookie(ev->Cookie).Quote().c_str(),
            ev->Cookie,
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (!isCookieValid) {
        ReportInvalidCookieAndDie(ctx, ev->Cookie);
        return;
    }

    if (ev->Cookie == MainFileStoreCookie) {
        AlterShards(ctx);
        return;
    }

    Y_ABORT_UNLESS(ShardsToAlter);
    if (--ShardsToAlter == 0) {
        CreateShards(ctx);
    } else if (StorageConfig->GetMaxShardManagementRequestsInFlight()) {
        if (NextShardToAlter < ExistingTotalShardCount) {
            AlterShard(ctx, NextShardToAlter);
            ++NextShardToAlter;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::GetFileSystemTopology(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(!Alter);

    auto request =
        std::make_unique<TEvIndexTablet::TEvGetFileSystemTopologyRequest>();
    request->Record.SetFileSystemId(FileSystemId);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

void TAlterFileStoreActor::HandleGetFileSystemTopologyResponse(
    const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Getting file system topology failed: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    DirectoryCreationInShardsEnabled =
        EnableDirectoryCreationInShards ||
        msg->Record.GetDirectoryCreationInShardsEnabled();
    DirectoryCreationInShardsForced =
        ForceDirectoryCreationInShards ||
        msg->Record.GetForceDirectoryCreationInShards();
    StrictFileSystemSizeEnforcementEnabled =
        EnableStrictFileSystemSizeEnforcement ||
        msg->Record.GetStrictFileSystemSizeEnforcementEnabled();
    MaxShardCount = msg->Record.GetMaxShardCount();

    // A shard has no shards of its own, its topology lists the sibling shards.
    if (!msg->Record.GetShardNo()) {
        ShardTopology = GetShardTopology(msg->Record);
        ExistingTotalShardCount = ShardTopology.ShardsSize();
        for (const auto& shard: ShardTopology.GetShards()) {
            if (shard.GetType() == NProtoPrivate::TShardTopology::E_FASTSHARD) {
                ++ExistingFastShardCount;
            }
        }

        ExistingOrdinaryShardCount =
            ExistingTotalShardCount - ExistingFastShardCount;
    }

    if (auto error = ValidateOrdinaryShards(); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    if (auto error = ValidateFastShards(); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    ui64 totalShardCount = ExistingTotalShardCount;
    if (ExplicitOrdinaryShardCount) {
        totalShardCount += ExplicitOrdinaryShardCount - ExistingOrdinaryShardCount;
    }

    if (ExplicitFastShardCount) {
        NewFastShardCount = ExplicitFastShardCount - ExistingFastShardCount;
        totalShardCount += NewFastShardCount;
    }

    if (auto error = ValidateTotalShards(totalShardCount); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    // Automatic shard creation chooses the shard count by the size. It does
    // not apply to explicit counts and to a filesystem with fast shards.
    ui32 shardCount = totalShardCount;
    if (StorageConfig->GetAutomaticShardCreationEnabled() &&
        !ExplicitOrdinaryShardCount && !ExplicitFastShardCount &&
        !ExistingFastShardCount)
    {
        shardCount = 0;
    }

    PatchStorageConfig();
    FillMultiShardFileStoreConfig(ctx, shardCount);

    if (SevenBytesHandlesCount > 0 &&
        ExistingTotalShardCount <= MaxOneByteShardCount &&
        FileStoreConfig.ShardConfigs.size() > MaxOneByteShardCount)
    {
        // The capped shard count cannot hold the new fast shards.
        if (NewFastShardCount) {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_PRECONDITION_FAILED,
                    "Cannot add fast shards if the shard count is capped"));
            return;
        }

        ReportTryingToUseTwoBytesShardNoWithObsoleteHandles(
            TStringBuilder()
            << "Filesystem: " << FileSystemId
            << ". Cannot increase the shard count to more than 255 if there "
               "are handles with the non-zero 7th byte.");

        FillMultiShardFileStoreConfig(ctx, MaxOneByteShardCount);
    }

    if (FileStoreConfig.ShardConfigs.size() < ExistingTotalShardCount) {
        if (!StrictFileSystemSizeEnforcementEnabled ||
            ExplicitOrdinaryShardCount)
        {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "Cannot decrease number of shards: current="
                        << ExistingTotalShardCount
                        << ", requested="
                        << FileStoreConfig.ShardConfigs.size()));
            return;
        } else {
            // We can't reduce shards count but in strict mode we should resize
            // all the shards to have the same size as the main filesystem.
            // So, FileStoreConfig.ShardConfigs should have the same size as
            // ExistingTotalShardCount.
            FileStoreConfig.ShardConfigs.clear();
            FileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
                *StorageConfig,
                FileStoreConfig.MainFileSystemConfig,
                PerformanceProfile,
                ExistingTotalShardCount);
        }
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Will resize filesystem to have %u shards",
        FileSystemId.c_str(),
        FileStoreConfig.ShardConfigs.size());

    const ui32 shardsToCheck = msg->Record.GetShardNo()
                                   ? 0
                                   : Min<ui32>(
                                         ExistingTotalShardCount,
                                         FileStoreConfig.ShardConfigs.size());
    for (ui32 i = 0; i < shardsToCheck; ++i) {
        if (ShardTopology.GetShards(i).GetShardId() !=
            FileStoreConfig.ShardConfigs[i].GetFileSystemId())
        {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "Shard FileSystemId mismatch at pos " << i << ": "
                        << ShardTopology.GetShards(i).GetShardId() << " != "
                        << FileStoreConfig.ShardConfigs[i].GetFileSystemId()));
            return;
        }
    }

    // Set shards size equal to the file system size in case of
    // StrictFileSystemSizeEnforcement
    if (StrictFileSystemSizeEnforcementEnabled) {
        for (auto& shard: FileStoreConfig.ShardConfigs) {
            shard.SetBlocksCount(TargetConfig.GetBlocksCount());
        }
    }

    if (msg->Record.GetShardNo()) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] GetFileSystemTopology - resized a shard (%u), no subshards"
            " will be added",
            FileSystemId.c_str(),
            msg->Record.GetShardNo());
        Y_ABORT_UNLESS(
            !ShardCreationState.HasShardsToCreate() &&
            ShardsToConfigure == 0 &&
            ShardsToAlter == 0 && ShardsToDescribe == 0 &&
            !ShouldConfigureMainFileStore);

        if (StrictFileSystemSizeEnforcementEnabled) {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_ARGUMENT,
                    "Cannot change shard size if "
                    "'StrictFileSystemSizeEnforcementEnabled'"));
            return;
        }

        // A fast shard keeps the size its devices were allocated for.
        const auto topology = GetShardTopology(msg->Record);
        const ui32 shardIndex = msg->Record.GetShardNo() - 1;
        if (shardIndex < topology.ShardsSize() &&
            topology.GetShards(shardIndex).GetType() ==
                NProtoPrivate::TShardTopology::E_FASTSHARD)
        {
            ReplyAndDie(
                ctx,
                MakeError(E_ARGUMENT, "Cannot resize fast shard."));
            return;
        }

        FileStoreConfig.ShardConfigs.clear();

    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Will resize filesystem to have %u shards",
            FileSystemId.c_str(),
            FileStoreConfig.ShardConfigs.size());
        const auto error = ShardCreationState.InitializeShardCreation(
            ExistingTotalShardCount,
            FileStoreConfig.ShardConfigs.size(),
            StorageConfig->GetMaxShardManagementRequestsInFlight());
        if (HasError(error)) {
            ReplyAndDie(ctx, error);
            return;
        }

        // TODO(#8034): use the main filestore config topology once it is there
        // The new fast shards are last, their entries come after allocation.
        const auto& shardConfigs = FileStoreConfig.ShardConfigs;
        const ui32 fastShardsBegin = shardConfigs.size() - NewFastShardCount;
        for (ui32 i = ExistingTotalShardCount; i < fastShardsBegin; ++i) {
            AddOrdinaryShard(shardConfigs[i].GetFileSystemId(), ShardTopology);
        }

        if (ShardCreationState.HasShardsToCreate() ||
            EnableStrictFileSystemSizeEnforcement ||
            EnableDirectoryCreationInShards)
        {
            ShouldConfigureMainFileStore = true;
            ShardsToConfigure = FileStoreConfig.ShardConfigs.size();
        }

        // The shards are resized only if
        // StrictFileSystemSizeEnforcementEnabled, otherwise the shards don't
        // change their size.
        if (StrictFileSystemSizeEnforcementEnabled) {
            ShardsToDescribe = ExistingTotalShardCount;
            ShardsToAlter = ExistingTotalShardCount;
        }
    }

    DescribeShards(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::CreateShards(const TActorContext& ctx)
{
    ShardCreationState.StartShardCreation(ctx);

    if (!ShardCreationState.HasShardsToCreate()) {
        AllocateFastShards(ctx);
    }
}

void TAlterFileStoreActor::CreateShard(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        FileStoreConfig.ShardConfigs[shardIndex]);

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Creating shard %s",
        FileSystemId.c_str(),
        request->Config.GetFileSystemId().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        shardIndex   // cookie
    );
}

void TAlterFileStoreActor::HandleCreateFileStoreResponse(
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
            FileSystemId.c_str(),
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
        FileSystemId.c_str(),
        GetFileSystemIdForLogByCookie(shardIndex).Quote().c_str());

    const auto error = ShardCreationState.OnShardCreated(
        ctx,
        shardIndex);
    if (HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    if (!ShardCreationState.HasShardsToCreate()) {
        AllocateFastShards(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::AllocateFastShards(const TActorContext& ctx)
{
    if (!NewFastShardCount) {
        ConfigureShards(ctx);
        return;
    }

    // The new fast shards get the storage kind of an existing one.
    for (const auto& shard: ShardTopology.GetShards()) {
        const auto& storageGroups = shard.GetFastShardConfig()
            .GetPersistentConfig()
            .GetStorageGroups();
        if (!storageGroups.empty()) {
            NCloud::Register(
                ctx,
                CreateAllocateFastShardsActor(
                    StorageConfig,
                    SelfId(),
                    FileSystemId,
                    MainFileStoreOriginalConfig,
                    FileStoreConfig.ShardConfigs,
                    NewFastShardCount,
                    storageGroups[0].GetType()));
            return;
        }
    }

    ReplyAndDie(
        ctx,
        MakeError(E_INVALID_STATE, "no storage kind for the new fast shards"));
}

void TAlterFileStoreActor::HandleFastShardsAllocated(
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

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::ConfigureShards(const TActorContext& ctx)
{
    if (ShardsToConfigure == 0) {
        ConfigureMainFileStore(ctx);
        return;
    }

    const ui32 totalShardCount = FileStoreConfig.ShardConfigs.size();

    if (ExistingTotalShardCount < totalShardCount) {
        ConfigureShardRange(
            ctx,
            ExistingTotalShardCount,
            totalShardCount,
            EShardConfigPhase::NewShards);
    } else {
        // No new shards (e.g. a resize that only toggles strict-size /
        // directory-creation-in-shards) - configure the existing shards now.
        ConfigureShardRange(
            ctx,
            0,
            ExistingTotalShardCount,
            EShardConfigPhase::OldShards);
    }
}

void TAlterFileStoreActor::ConfigureShardRange(
    const TActorContext& ctx,
    const ui32 beginShardIndex,
    const ui32 endShardIndex,
    const EShardConfigPhase phase)
{
    ShardConfigPhase = phase;
    EndShardToConfigure = endShardIndex;
    NextShardToConfigure = beginShardIndex;
    ShardsToConfigure = endShardIndex - beginShardIndex;

    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    const ui32 cappedEndIndex =
        (limit == 0)
            ? endShardIndex
            : std::min<ui32>(beginShardIndex + limit, endShardIndex);

    for (ui32 i = beginShardIndex; i < cappedEndIndex; ++i) {
        ConfigureShard(ctx, i);
        NextShardToConfigure = i + 1;
    }
}

void TAlterFileStoreActor::ConfigureShard(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvConfigureAsShardRequest>();
    request->Record.SetFileSystemId(
        FileStoreConfig.ShardConfigs[shardIndex].GetFileSystemId());
    request->Record.SetShardNo(shardIndex + 1);
    request->Record.SetMainFileSystemId(FileSystemId);
    request->Record.SetDirectoryCreationInShardsEnabled(
        DirectoryCreationInShardsEnabled);
    request->Record.SetForceDirectoryCreationInShards(
        DirectoryCreationInShardsForced);
    request->Record.SetStrictFileSystemSizeEnforcementEnabled(
        StrictFileSystemSizeEnforcementEnabled);

    if (DirectoryCreationInShardsEnabled ||
        StrictFileSystemSizeEnforcementEnabled)
    {
        for (const auto& shard: FileStoreConfig.ShardConfigs) {
            request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
        }
        *request->Record.MutableShardTopology() = ShardTopology;
    }

    // Only a new fast shard gets its config, the existing ones keep theirs.
    const auto& shard = ShardTopology.GetShards(shardIndex);
    if (shardIndex >= ExistingTotalShardCount && shard.HasFastShardConfig()) {
        request->Record.SetIsFastShard(true);
        *request->Record.MutableFastShardConfig() = shard.GetFastShardConfig();
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring shard %s",
        FileSystemId.c_str(),
        request->Record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request),
        shardIndex   // cookie
    );
}

void TAlterFileStoreActor::HandleConfigureShardResponse(
    const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const bool isCookieValid = ev->Cookie < FileStoreConfig.ShardConfigs.size();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard %s (cookie: %lu) configuration failed: %s",
            FileSystemId.c_str(),
            GetFileSystemIdForLogByCookie(ev->Cookie).Quote().c_str(),
            ev->Cookie,
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (!isCookieValid) {
        ReportInvalidCookieAndDie(ctx, ev->Cookie);
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configured shard %s",
        FileSystemId.c_str(),
        FileStoreConfig.ShardConfigs[ev->Cookie].GetFileSystemId().c_str());

    Y_DEBUG_ABORT_UNLESS(ShardsToConfigure);
    if (--ShardsToConfigure == 0) {
        if (ShardConfigPhase == EShardConfigPhase::NewShards
                && ExistingTotalShardCount > 0)
        {
            // The newly created shards are all configured now - configure the
            // pre-existing shards so that they pick up the new shard list.
            ConfigureShardRange(
                ctx,
                0,
                ExistingTotalShardCount,
                EShardConfigPhase::OldShards);
        } else {
            ConfigureMainFileStore(ctx);
        }
    } else if (StorageConfig->GetMaxShardManagementRequestsInFlight()) {
        if (NextShardToConfigure < EndShardToConfigure) {
            ConfigureShard(ctx, NextShardToConfigure);
            ++NextShardToConfigure;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::ConfigureMainFileStore(const TActorContext& ctx)
{
    // As a shard is being resized, we can't send ConfigureShardsRequest to it
    if (!ShouldConfigureMainFileStore) {
        ReplyAndDie(ctx);
        return;
    }

    auto request =
        std::make_unique<TEvIndexTablet::TEvConfigureShardsRequest>();
    request->Record.SetFileSystemId(
        FileStoreConfig.MainFileSystemConfig.GetFileSystemId());
    request->Record.SetDirectoryCreationInShardsEnabled(
        DirectoryCreationInShardsEnabled);
    request->Record.SetForceDirectoryCreationInShards(
        DirectoryCreationInShardsForced);
    request->Record.SetStrictFileSystemSizeEnforcementEnabled(
        StrictFileSystemSizeEnforcementEnabled);

    for (const auto& shard: FileStoreConfig.ShardConfigs) {
        request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
    }
    *request->Record.MutableShardTopology() = ShardTopology;

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring main filesystem %s",
        FileSystemId.c_str(),
        request->Record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

void TAlterFileStoreActor::HandleConfigureMainFileStoreResponse(
    const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configuring main filesystem failed: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configured main filesystem",
        FileSystemId.c_str());

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::HandleDescribeFileStoreForAlterResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configuring main filesystem failed: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& fileStore = msg->PathDescription.GetFileStoreDescription();
    auto config = fileStore.GetConfig();

    if (const auto& cloud = TargetConfig.GetCloudId()) {
        config.SetCloudId(cloud);
    }
    if (const auto& project = TargetConfig.GetProjectId()) {
        config.SetProjectId(project);
    }
    if (const auto& folder = TargetConfig.GetFolderId()) {
        config.SetFolderId(folder);
    }
    if (auto version = TargetConfig.GetVersion()) {
        config.SetVersion(version);
    }

    config.SetAlterTs(ctx.Now().MicroSeconds());
    config.ClearBlockSize();

    FileStoreConfig.MainFileSystemConfig.Swap(&config);

    AlterFileStore(ctx);
}

void TAlterFileStoreActor::HandleAlterFileStoreForAlterResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();
    if (HasError(error)) {
        LOG_ERROR(ctx, TFileStoreComponents::SERVICE,
            "[%s] Altering of main filestore failed: %s",
            FileSystemId.Quote().c_str(),
            FormatError(msg->GetError()).Quote().c_str());
    }

    ReplyAndDie(ctx, error);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TAlterFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (Alter) {
        auto response = std::make_unique<TEvService::TEvAlterFileStoreResponse>(
            error);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    } else {
        auto response =
            std::make_unique<TEvService::TEvResizeFileStoreResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

void TAlterFileStoreActor::ReportInvalidCookieAndDie(const TActorContext& ctx, ui64 cookie)
{
    ReplyAndDie(
        ctx,
        MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "invalid cookie: " << cookie));
}

STFUNC(TAlterFileStoreActor::AlterStateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvDescribeFileStoreResponse,
            HandleDescribeFileStoreForAlterResponse);
        HFunc(
            TEvSSProxy::TEvAlterFileStoreResponse,
            HandleAlterFileStoreForAlterResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TAlterFileStoreActor::ResizeStateWork)
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
            TEvIndexTablet::TEvGetFileSystemTopologyResponse,
            HandleGetFileSystemTopologyResponse);
        HFunc(
            TEvIndexTablet::TEvGetStorageStatsResponse,
            HandleGetStorageStatsResponse);
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

void TStorageServiceActor::HandleAlterFileStore(
    const TEvService::TEvAlterFileStoreRequest::TPtr& ev,
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

    auto actor = std::make_unique<TAlterFileStoreActor>(
        StorageConfig,
        std::move(requestInfo),
        msg->Record);

    NCloud::Register(ctx, std::move(actor));
}

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

    auto actor = std::make_unique<TAlterFileStoreActor>(
        StorageConfig,
        std::move(requestInfo),
        msg->Record);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
