#include "service_actor.h"

#include "helpers.h"
#include "service_actor_fastshard.h"
#include "service_actor_multishard_alter.h"
#include "service_actor_multishard_configure.h"
#include "service_actor_multishard_create.h"
#include "service_actor_multishard_describe.h"
#include "service_private.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/model/channel_data_kind.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/string/builder.h>

#include <optional>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TResizePlan
{
    // The filesystem and the shards that are resized with it.
    TMultiShardFileStoreConfig Alter;
    std::optional<TMultiShardCreateConfig> Create;
    std::optional<TMultiShardConfigureConfig> Configure;
};

////////////////////////////////////////////////////////////////////////////////

class TResizeFileStoreActor final
    : public TActorBootstrapped<TResizeFileStoreActor>
{
private:
    // A copy: the filesystem state patches it for this request only.
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const NProto::TResizeFileStoreRequest Request;
    const ui32 ExplicitOrdinaryShardCount;
    const ui32 ExplicitFastShardCount;

    TEvServicePrivate::TMultiShardDescribeResponse Described;
    // The shards of the filesystem. A shard has none of its own.
    NProtoPrivate::TShardTopology ShardTopology;
    ui32 ExistingOrdinaryShardCount = 0;
    ui32 ExistingFastShardCount = 0;
    ui64 ExistingFastShardCapacity = 0;
    ui32 ExistingTotalShardCount = 0;
    // The new fast shards get the storage kind of the existing ones.
    std::optional<NProto::EFastShardStorageKind> FastShardStorageKind;
    ui64 NewFastShardCount = 0;

    TResizePlan Plan;
    // The step actor in flight.
    TActorId Worker;

public:
    TResizeFileStoreActor(
        const TStorageConfig& storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TResizeFileStoreRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ConfigureShards(const TActorContext& ctx);
    void PatchStorageConfig();

    NProto::TError BuildPlan();
    NProto::TError BuildShardPlan();
    NProto::TError CollectFastShardStats();
    NProto::TError ValidateOrdinaryShards() const;
    NProto::TError ValidateFastShards() const;
    NProto::TError ValidateTotalShards(ui64 totalShardCount) const;
    ui64 CalculateRequiredNewFastShardCount() const;
    ui64 GetNewFastShardCount() const;
    TResultOrError<TMultiShardFileStoreConfig> BuildFileStoreConfig(
        ui64 totalShardCount) const;

    TResultOrError<TMultiShardFileStoreConfig> SetupFileStoreConfig(
        ui32 explicitShardCount) const;
    TVector<NKikimrFileStore::TConfig> BuildShardConfigs() const;
    TResultOrError<TMultiShardCreateConfig> MakeCreateConfig(
        const TVector<NKikimrFileStore::TConfig>& shardConfigs) const;

    TMultiShardConfigureConfig MakeConfigureConfig() const;

    void HandleMultiShardDescribeResponse(
        const TEvServicePrivate::TEvMultiShardDescribeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleMultiShardAlterResponse(
        const TEvServicePrivate::TEvMultiShardAlterResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleMultiShardCreateResponse(
        const TEvServicePrivate::TEvMultiShardCreateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleMultiShardConfigureResponse(
        const TEvServicePrivate::TEvMultiShardConfigureResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TResizeFileStoreActor::TResizeFileStoreActor(
        const TStorageConfig& storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TResizeFileStoreRequest request)
    : StorageConfig(std::make_shared<TStorageConfig>(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , ExplicitOrdinaryShardCount(Request.GetShardCount())
    , ExplicitFastShardCount(Request.GetFastShardCount())
{}

void TResizeFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto actor = CreateMultiShardDescribeActor(
        StorageConfig,
        SelfId(),
        Request.GetFileSystemId());
    Worker = NCloud::Register(ctx, std::move(actor));
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::HandleMultiShardDescribeResponse(
    const TEvServicePrivate::TEvMultiShardDescribeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Described = *msg;
    PatchStorageConfig();

    if (const auto error = BuildPlan(); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    ui64 totalShardCount = ExistingTotalShardCount;
    if (Plan.Create) {
        totalShardCount += Plan.Create->ShardsToCreate.size();
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Will resize filesystem to have %lu shards",
        Request.GetFileSystemId().c_str(),
        totalShardCount);

    auto actor =
        CreateMultiShardAlterActor(StorageConfig, SelfId(), Plan.Alter);
    Worker = NCloud::Register(ctx, std::move(actor));
}

void TResizeFileStoreActor::PatchStorageConfig()
{
    const auto& state = Described.Topology;

    NProto::TStorageConfig patch;
    patch.SetStrictFileSystemSizeEnforcementEnabled(
        Request.GetEnableStrictFileSystemSizeEnforcement() ||
        state.GetStrictFileSystemSizeEnforcementEnabled());
    patch.SetMaxShardCount(state.GetMaxShardCount());
    StorageConfig->Merge(patch);
}

NProto::TError TResizeFileStoreActor::BuildPlan()
{
    if (Described.Topology.GetShardNo()) {
        return BuildShardPlan();
    }

    ShardTopology = GetShardTopology(Described.Topology);
    ExistingTotalShardCount = ShardTopology.ShardsSize();
    if (const auto error = CollectFastShardStats(); HasError(error)) {
        return error;
    }
    ExistingOrdinaryShardCount =
        ExistingTotalShardCount - ExistingFastShardCount;

    if (const auto error = ValidateOrdinaryShards(); HasError(error)) {
        return error;
    }

    if (const auto error = ValidateFastShards(); HasError(error)) {
        return error;
    }

    NewFastShardCount = GetNewFastShardCount();

    ui64 totalShardCount = ExistingTotalShardCount + NewFastShardCount;
    if (ExplicitOrdinaryShardCount) {
        totalShardCount +=
            ExplicitOrdinaryShardCount - ExistingOrdinaryShardCount;
    }

    if (const auto error = ValidateTotalShards(totalShardCount);
        HasError(error))
    {
        return error;
    }

    auto fileStoreConfig = BuildFileStoreConfig(totalShardCount);
    if (HasError(fileStoreConfig)) {
        return fileStoreConfig.GetError();
    }
    const auto& shardConfigs = fileStoreConfig.GetResult().ShardConfigs;

    Plan.Alter.MainFileSystemConfig =
        fileStoreConfig.GetResult().MainFileSystemConfig;

    if (StorageConfig->GetStrictFileSystemSizeEnforcementEnabled()) {
        Plan.Alter.ShardConfigs = BuildShardConfigs();
    }

    // A resize without new shards is checked against the saved state too: it
    // should not get ahead of an unfinished shard creation.
    auto createConfig = MakeCreateConfig(shardConfigs);
    if (HasError(createConfig)) {
        return createConfig.GetError();
    }

    const bool newShards = shardConfigs.size() > ExistingTotalShardCount;
    if (newShards) {
        Plan.Create = createConfig.ExtractResult();
    }

    if (newShards || Request.GetEnableStrictFileSystemSizeEnforcement() ||
        Request.GetEnableDirectoryCreationInShards())
    {
        Plan.Configure = MakeConfigureConfig();
    }

    return {};
}

// A shard is resized alone: it has no shards of its own.
NProto::TError TResizeFileStoreActor::BuildShardPlan()
{
    if (ExplicitOrdinaryShardCount || ExplicitFastShardCount) {
        return MakeError(E_ARGUMENT, "Cannot add shards to a shard");
    }

    auto fileStoreConfig = SetupFileStoreConfig(0);
    if (HasError(fileStoreConfig)) {
        return fileStoreConfig.GetError();
    }

    if (StorageConfig->GetStrictFileSystemSizeEnforcementEnabled()) {
        return MakeError(
            E_ARGUMENT,
            "Cannot change shard size if "
            "'StrictFileSystemSizeEnforcementEnabled'");
    }

    // A fast shard keeps the size its devices were allocated for.
    const auto siblings = GetShardTopology(Described.Topology);
    const ui32 shardIndex = Described.Topology.GetShardNo() - 1;
    if (shardIndex < siblings.ShardsSize() &&
        siblings.GetShards(shardIndex).GetType() ==
            NProtoPrivate::TShardTopology::E_FASTSHARD)
    {
        return MakeError(E_ARGUMENT, "Cannot resize fast shard");
    }

    Plan.Alter.MainFileSystemConfig =
        fileStoreConfig.GetResult().MainFileSystemConfig;
    return {};
}

NProto::TError TResizeFileStoreActor::CollectFastShardStats()
{
    for (const auto& shard: ShardTopology.GetShards()) {
        if (shard.GetType() != NProtoPrivate::TShardTopology::E_FASTSHARD) {
            continue;
        }

        const auto& config = shard.GetFastShardConfig().GetPersistentConfig();
        if (config.GetStorageGroups().empty()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "fast shard " << shard.GetShardId()
                    << " has no storage group");
        }

        ++ExistingFastShardCount;
        ExistingFastShardCapacity += config.GetExpectedGroupCapacity();

        for (const auto& storageGroup: config.GetStorageGroups()) {
            const auto storageKind = storageGroup.GetType();
            if (FastShardStorageKind && storageKind != *FastShardStorageKind) {
                return MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "fast shard " << shard.GetShardId()
                        << " has storage kind "
                        << EFastShardStorageKind_Name(storageKind)
                        << ", the other fast shards have "
                        << EFastShardStorageKind_Name(*FastShardStorageKind));
            }

            FastShardStorageKind = storageKind;
        }
    }

    return {};
}

NProto::TError TResizeFileStoreActor::ValidateOrdinaryShards() const
{
    if (ExplicitOrdinaryShardCount &&
        ExplicitOrdinaryShardCount < ExistingOrdinaryShardCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Cannot decrease number of shards: current="
                << ExistingOrdinaryShardCount
                << ", requested=" << ExplicitOrdinaryShardCount);
    }

    return {};
}

NProto::TError TResizeFileStoreActor::ValidateFastShards() const
{
    if (!ExistingFastShardCount) {
        if (ExplicitFastShardCount) {
            return MakeError(
                E_ARGUMENT,
                "Cannot add fast shards to a filesystem created without them");
        }

        return {};
    }

    if (!ExplicitFastShardCount) {
        return {};
    }

    if (ExplicitFastShardCount < ExistingFastShardCount) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Cannot decrease number of fast shards: current="
                << ExistingFastShardCount
                << ", requested=" << ExplicitFastShardCount);
    }

    const ui64 requiredNewFastShardCount = CalculateRequiredNewFastShardCount();
    if (ExplicitFastShardCount - ExistingFastShardCount <
        requiredNewFastShardCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "fast shard count " << ExplicitFastShardCount
                << " cannot hold the filesystem, "
                << ExistingFastShardCount + requiredNewFastShardCount
                << " needed");
    }

    return {};
}

// Only the shards that the request adds are checked against the limit.
NProto::TError TResizeFileStoreActor::ValidateTotalShards(
    ui64 totalShardCount) const
{
    const ui32 maxShardCount = Described.Topology.GetMaxShardCount();
    if (totalShardCount > ExistingTotalShardCount &&
        totalShardCount > maxShardCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "shard count " << totalShardCount
                << " should not be bigger than " << maxShardCount);
    }

    return {};
}

// The fast shards that the part of the filesystem over the existing fast shard
// capacity needs.
ui64 TResizeFileStoreActor::CalculateRequiredNewFastShardCount() const
{
    const ui32 blockSize = Described.Config.MainFileSystemConfig.GetBlockSize();
    const ui64 blocksCount = Request.GetBlocksCount();
    const ui64 missingBlocksCount =
        blocksCount - Min(blocksCount, ExistingFastShardCapacity / blockSize);
    return CalculateFastShardCount(
        *StorageConfig,
        missingBlocksCount,
        blockSize);
}

ui64 TResizeFileStoreActor::GetNewFastShardCount() const
{
    if (!ExistingFastShardCount) {
        return 0;
    }

    if (ExplicitFastShardCount) {
        return ExplicitFastShardCount - ExistingFastShardCount;
    }

    return CalculateRequiredNewFastShardCount();
}

auto TResizeFileStoreActor::BuildFileStoreConfig(ui64 totalShardCount) const
    -> TResultOrError<TMultiShardFileStoreConfig>
{
    // Without an explicit count automatic shard creation chooses it by the
    // size. Requested counts and fast shards make the count explicit.
    ui32 explicitShardCount = totalShardCount;
    if (StorageConfig->GetAutomaticShardCreationEnabled() &&
        !ExplicitOrdinaryShardCount && !ExplicitFastShardCount &&
        !ExistingFastShardCount)
    {
        explicitShardCount = 0;
    }

    auto result = SetupFileStoreConfig(explicitShardCount);
    if (HasError(result)) {
        return result.GetError();
    }
    auto fileStoreConfig = result.ExtractResult();

    if (StorageConfig->GetOneByteShardCountCapEnabled() &&
        ExistingTotalShardCount <= MaxOneByteShardCount &&
        fileStoreConfig.ShardConfigs.size() > MaxOneByteShardCount)
    {
        // The capped shard count cannot hold the new fast shards.
        if (NewFastShardCount) {
            return MakeError(
                E_PRECONDITION_FAILED,
                "Cannot add fast shards if the shard count is capped");
        }

        auto capped = SetupFileStoreConfig(MaxOneByteShardCount);
        if (HasError(capped)) {
            return capped.GetError();
        }
        fileStoreConfig = capped.ExtractResult();
    }

    if (fileStoreConfig.ShardConfigs.size() < ExistingTotalShardCount) {
        if (!StorageConfig->GetStrictFileSystemSizeEnforcementEnabled() ||
            ExplicitOrdinaryShardCount)
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Cannot decrease number of shards: current="
                    << ExistingTotalShardCount << ", requested="
                    << fileStoreConfig.ShardConfigs.size());
        }

        // A strict filesystem keeps its shards, each of the filesystem size.
        fileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            fileStoreConfig.MainFileSystemConfig,
            Request.GetPerformanceProfile(),
            ExistingTotalShardCount);
    }

    for (ui32 i = 0; i < ExistingTotalShardCount; ++i) {
        const auto& shardId = ShardTopology.GetShards(i).GetShardId();
        const auto& targetShardId =
            fileStoreConfig.ShardConfigs[i].GetFileSystemId();
        if (shardId != targetShardId) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Shard FileSystemId mismatch at pos " << i << ": "
                    << shardId << " != " << targetShardId);
        }
    }

    return fileStoreConfig;
}

auto TResizeFileStoreActor::SetupFileStoreConfig(ui32 explicitShardCount) const
    -> TResultOrError<TMultiShardFileStoreConfig>
{
    const auto& mainConfig = Described.Config.MainFileSystemConfig;
    if (mainConfig.GetBlocksCount() > Request.GetBlocksCount() &&
        !Request.GetForce())
    {
        return MakeError(E_ARGUMENT, "Cannot decrease filestore size");
    }

    // Allocate legacy mixed0 channel in case it was already present
    Y_ABORT_UNLESS(mainConfig.ExplicitChannelProfilesSize() >= 4);
    const auto thirdChannelDataKind = static_cast<EChannelDataKind>(
        mainConfig.GetExplicitChannelProfiles(3).GetDataKind());
    const bool allocateMixed0 =
        thirdChannelDataKind == EChannelDataKind::Mixed0;

    auto targetConfig = mainConfig;
    targetConfig.SetBlocksCount(Request.GetBlocksCount());

    // The filesystem is sharded if automatic shard creation is enabled or if
    // it was created with an explicitly specified shard count. A shard is not.
    const bool sharded = !Described.Topology.GetShardNo() &&
        (StorageConfig->GetAutomaticShardCreationEnabled() ||
            explicitShardCount > 0);

    TMultiShardFileStoreConfig result;
    if (!allocateMixed0 && sharded) {
        result = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            targetConfig,
            Request.GetPerformanceProfile(),
            explicitShardCount);
    } else {
        SetupFileStorePerformanceAndChannels(
            allocateMixed0,
            *StorageConfig,
            targetConfig,
            Request.GetPerformanceProfile());
        result.MainFileSystemConfig = targetConfig;
    }

    if (const auto version = Request.GetConfigVersion()) {
        result.MainFileSystemConfig.SetVersion(version);
    }

    return result;
}

// The shards of a strict filesystem have the filesystem size.
TVector<NKikimrFileStore::TConfig> TResizeFileStoreActor::BuildShardConfigs() const
{
    TVector<NKikimrFileStore::TConfig> result;
    for (auto shardConfig: Described.Config.ShardConfigs) {
        const ui64 version = shardConfig.GetVersion();
        shardConfig.SetBlocksCount(Request.GetBlocksCount());
        // A described shard can have more channels than the calculated ones.
        SetupFileStorePerformanceAndChannels(
            false,   // allocateMixed0Channel
            *StorageConfig,
            shardConfig,
            Request.GetPerformanceProfile());
        shardConfig.SetVersion(version);
        result.push_back(std::move(shardConfig));
    }

    return result;
}

TResultOrError<TMultiShardCreateConfig> TResizeFileStoreActor::MakeCreateConfig(
    const TVector<NKikimrFileStore::TConfig>& shardConfigs) const
{
    using TShardTopology = NProtoPrivate::TShardTopology;

    TMultiShardCreateConfig config;
    config.FileSystemId = Request.GetFileSystemId();
    config.ExistingShardCount = ExistingTotalShardCount;

    // The new fast shards are the last ones.
    const ui32 fastShardsBegin = shardConfigs.size() - NewFastShardCount;
    for (ui32 i = ExistingTotalShardCount; i < shardConfigs.size(); ++i) {
        auto& shard = config.ShardsToCreate.emplace_back();
        shard.Config = shardConfigs[i];
        if (i < fastShardsBegin) {
            shard.Type = TShardTopology::E_ORDINARY;
            shard.RoleMask = ProtoFlag(TShardTopology::E_FILE) |
                ProtoFlag(TShardTopology::E_DIRECTORY);
        } else {
            shard.Type = TShardTopology::E_FASTSHARD;
            shard.RoleMask = ProtoFlag(TShardTopology::E_FILE);
            shard.FastShardStorageKind = FastShardStorageKind;
        }
    }

    if (Described.ShardCreationState) {
        auto state =
            InitShardCreationState(*Described.ShardCreationState, config);
        if (HasError(state)) {
            return state.GetError();
        }
        config.State = state.ExtractResult();
    }

    return config;
}

TMultiShardConfigureConfig TResizeFileStoreActor::MakeConfigureConfig() const
{
    const auto& state = Described.Topology;

    TMultiShardConfigureConfig config;
    config.FileSystemId = Request.GetFileSystemId();
    config.Topology = ShardTopology;
    config.ExistingShardCount = ExistingTotalShardCount;
    config.DirectoryCreationInShardsEnabled =
        Request.GetEnableDirectoryCreationInShards() ||
        state.GetDirectoryCreationInShardsEnabled();
    config.ForceDirectoryCreationInShards =
        Request.GetForceDirectoryCreationInShards() ||
        state.GetForceDirectoryCreationInShards();
    config.StrictFileSystemSizeEnforcementEnabled =
        StorageConfig->GetStrictFileSystemSizeEnforcementEnabled();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::HandleMultiShardAlterResponse(
    const TEvServicePrivate::TEvMultiShardAlterResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (!Plan.Create) {
        ConfigureShards(ctx);
        return;
    }

    auto actor = CreateMultiShardCreateActor(
        StorageConfig,
        SelfId(),
        std::move(*Plan.Create));
    Worker = NCloud::Register(ctx, std::move(actor));
}

void TResizeFileStoreActor::HandleMultiShardCreateResponse(
    const TEvServicePrivate::TEvMultiShardCreateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Plan.Configure->Topology.MergeFrom(msg->Topology);
    ConfigureShards(ctx);
}

void TResizeFileStoreActor::ConfigureShards(const TActorContext& ctx)
{
    if (!Plan.Configure) {
        ReplyAndDie(ctx);
        return;
    }

    auto actor = CreateMultiShardConfigureActor(
        StorageConfig,
        SelfId(),
        std::move(*Plan.Configure));
    Worker = NCloud::Register(ctx, std::move(actor));
}

void TResizeFileStoreActor::HandleMultiShardConfigureResponse(
    const TEvServicePrivate::TEvMultiShardConfigureResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->GetError());
}

////////////////////////////////////////////////////////////////////////////////

void TResizeFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    NCloud::Send(ctx, Worker, std::make_unique<TEvents::TEvPoisonPill>());
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TResizeFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Resizing filesystem failed: %s",
            Request.GetFileSystemId().c_str(),
            FormatError(error).c_str());
    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Resized filesystem",
            Request.GetFileSystemId().c_str());
    }

    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<TEvService::TEvResizeFileStoreResponse>(error));
    Die(ctx);
}

STFUNC(TResizeFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvServicePrivate::TEvMultiShardDescribeResponse,
            HandleMultiShardDescribeResponse);
        HFunc(
            TEvServicePrivate::TEvMultiShardAlterResponse,
            HandleMultiShardAlterResponse);
        HFunc(
            TEvServicePrivate::TEvMultiShardCreateResponse,
            HandleMultiShardCreateResponse);
        HFunc(
            TEvServicePrivate::TEvMultiShardConfigureResponse,
            HandleMultiShardConfigureResponse);

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

    auto requestInfo = CreateRequestInfo(SelfId(), cookie, msg->CallContext);

    NCloud::Register(
        ctx,
        std::make_unique<TResizeFileStoreActor>(
            *StorageConfig,
            std::move(requestInfo),
            msg->Record));
}

}   // namespace NCloud::NFileStore::NStorage
