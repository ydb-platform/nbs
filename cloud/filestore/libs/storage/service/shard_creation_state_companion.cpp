#include "shard_creation_state_companion.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/log.h>

#include <util/digest/city.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 ShardCreationStateHashSeed = 0x6f4c9ad921f7b55dULL;

NKikimrFileStore::TConfig BuildShardCreationTargetConfigForHash(
    const NKikimrFileStore::TConfig& shardConfig)
{
    auto configForHash = shardConfig;

    // Hash the desired shard config conservatively, excluding only operational
    // metadata that may change between retries.
    configForHash.ClearVersion();
    configForHash.ClearCreationTs();
    configForHash.ClearAlterTs();
    return configForHash;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<ui64> CalculateNewShardConfigsHash(
    const ui32 topologyShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs)
{
    if (topologyShardCount > shardConfigs.size()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Invalid shard creation range: topology="
                << topologyShardCount
                << ", target=" << shardConfigs.size());
    }

    ui64 hash = ShardCreationStateHashSeed;
    for (size_t shardIndex = topologyShardCount;
         shardIndex < shardConfigs.size();
         ++shardIndex)
    {
        const auto shardConfig =
            BuildShardCreationTargetConfigForHash(shardConfigs[shardIndex]);

        TString serializedConfig;
        const bool serialized =
            shardConfig.SerializeToString(&serializedConfig);
        if (!serialized) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Failed to serialize shard config " << shardIndex
                    << " while calculating new shard configs hash");
        }

        hash = CityHash64WithSeed(serializedConfig, hash);
    }

    return hash;
}

////////////////////////////////////////////////////////////////////////////////

TShardCreationStateCompanion::TShardCreationStateCompanion(
    TString fileSystemId,
    EMode mode)
    : FileSystemId(std::move(fileSystemId))
    , LogTag(FileSystemId)
    , Mode(mode)
{}

bool TShardCreationStateCompanion::IsPersistentStateLoaded() const
{
    return ShardCreationStateStatus != EShardCreationStateStatus::Unknown;
}

bool TShardCreationStateCompanion::IsPersistentStateSupported() const
{
    return ShardCreationStateStatus == EShardCreationStateStatus::Supported;
}

void TShardCreationStateCompanion::MarkPersistentStateUnsupported()
{
    ShardCreationStateStatus = EShardCreationStateStatus::Unsupported;
}

bool TShardCreationStateCompanion::HasCreatedShardBitmap() const
{
    return !!CreatedShardBitmap;
}

bool TShardCreationStateCompanion::IsShardCreated(const ui32 shardIndex) const
{
    if (!CreatedShardBitmap ||
        shardIndex >= ShardCreationState.GetTargetShardCount())
    {
        return false;
    }

    return CreatedShardBitmap->Test(shardIndex);
}

ui32 TShardCreationStateCompanion::GetShardCreationStateVersion() const
{
    return ShardCreationState.GetVersion();
}

ui32 TShardCreationStateCompanion::GetTopologyShardCount() const
{
    return ShardCreationState.GetTopologyShardCount();
}

ui32 TShardCreationStateCompanion::GetTargetShardCount() const
{
    return ShardCreationState.GetTargetShardCount();
}

ui64 TShardCreationStateCompanion::GetNewShardConfigsHash() const
{
    return ShardCreationState.GetTargetShardConfigHash();
}

void TShardCreationStateCompanion::SetShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& state)
{
    ShardCreationStateStatus = EShardCreationStateStatus::Supported;
    ShardCreationState = state;
}

TResultOrError<TShardCreationStateCompanion::EResponseAction>
TShardCreationStateCompanion::HandleResponse(
    const NProtoPrivate::TUnsafeChangeTabletStateResponse& response,
    const TActorContext& ctx)
{
    if (HasError(response.GetError())) {
        const auto* action = IsPersistentStateLoaded() ? "update" : "read";
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "failed to " << action
                             << " shard creation state");
    }

    if (!response.HasShardCreationState()) {
        if (!IsPersistentStateLoaded()) {
            // Older Tablets accept the request but do not return this state.
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "[%s] UnsafeChangeTabletState returned no shard creation "
                "state, continuing without persistent shard creation state",
                LogTag.c_str());
            MarkPersistentStateUnsupported();
            return EResponseAction::StartWithoutPersistentState;
        }

        if (IsPersistentStateSupported()) {
            ReportShardCreationStateResponseMissing(
                TStringBuilder() << "Filesystem " << LogTag.Quote()
                                 << ": response is missing shard creation state"
                                 << " after persistent state support was confirmed");
            return MakeError(
                E_REJECTED,
                "shard creation state response is missing");
        }

        ReportShardCreationStateResponseMissing(
            TStringBuilder() << "Filesystem " << LogTag.Quote()
                             << ": unexpected response without shard creation"
                             << " state after switching to non-persistent flow");
        return MakeError(
            E_INVALID_STATE,
            "shard creation state response received after switching to "
            "non-persistent flow");
    }

    const auto& state = response.GetShardCreationState();
    if (!IsPersistentStateLoaded()) {
        SetShardCreationState(state);
        return EResponseAction::InitialStateLoaded;
    }

    if (!HasCreatedShardBitmap()) {
        ReportShardCreationBitmapMissing(
            TStringBuilder() << "Filesystem " << LogTag.Quote()
                             << ": shard creation state response received"
                             << " without an initialized shard bitmap");
        return MakeError(
            E_INVALID_STATE,
            "shard creation bitmap is not initialized");
    }

    const auto mergeResult = MergeShardCreationState(state);
    if (HasError(mergeResult)) {
        return mergeResult.GetError();
    }

    switch (mergeResult.GetResult()) {
        case EMergeAction::IgnoreStaleResponse:
            return EResponseAction::IgnoreStaleResponse;
        case EMergeAction::PersistLocalProgress:
            return EResponseAction::PersistLocalProgress;
        case EMergeAction::Continue:
            return EResponseAction::Continue;
    }
    Y_ABORT("Unexpected shard creation merge action");
}

TResultOrError<TShardCreationStateCompanion::EMergeAction>
TShardCreationStateCompanion::MergeShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& state)
{
    if (state.GetVersion() < ShardCreationState.GetVersion()) {
        return EMergeAction::IgnoreStaleResponse;
    }

    // Merge shard bits only when the current request matches the persistent
    // shard creation/resize plan (topology and target shard counts and config
    // hash).
    if (ShardCreationState.GetTopologyShardCount() !=
            state.GetTopologyShardCount() ||
        ShardCreationState.GetTargetShardCount() !=
            state.GetTargetShardCount() ||
        ShardCreationState.GetTargetShardConfigHash() !=
            state.GetTargetShardConfigHash())
    {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "Shard creation/resize plan does not match the current request"
                << " for filesystem " << FileSystemId.Quote()
                << ": request topology="
                << ShardCreationState.GetTopologyShardCount()
                << ", target=" << ShardCreationState.GetTargetShardCount()
                << ", hash=" << ShardCreationState.GetTargetShardConfigHash()
                << "; stored topology=" << state.GetTopologyShardCount()
                << ", target=" << state.GetTargetShardCount()
                << ", hash=" << state.GetTargetShardConfigHash()
                << ". Check the stored shard creation state before retrying");
    }

    if (CreatedShardBitmap) {
        MergeCompressedBitmap(
            *CreatedShardBitmap,
            state.GetCreatedShardBitmap());
    }
    SetShardCreationState(state);
    const auto unpersisted = HasUnpersistedCreatedShards();
    if (HasError(unpersisted)) {
        return unpersisted.GetError();
    }
    return unpersisted.GetResult()
        ? EMergeAction::PersistLocalProgress
        : EMergeAction::Continue;
}

NProto::TError TShardCreationStateCompanion::SetupCreatedShardBitmap(
    const ui32 topologyShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs)
{
    const ui32 targetShardCount = shardConfigs.size();
    if (topologyShardCount > targetShardCount) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Invalid shard creation range for filesystem "
                << FileSystemId.Quote() << ": topology="
                << topologyShardCount
                << ", target=" << targetShardCount);
    }

    const auto newShardConfigsHashResult =
        CalculateNewShardConfigsHash(topologyShardCount, shardConfigs);
    if (HasError(newShardConfigsHashResult)) {
        return newShardConfigsHashResult.GetError();
    }
    const ui64 newShardConfigsHash = newShardConfigsHashResult.GetResult();

    const auto& fsPersistentShardBitmap =
        ShardCreationState.GetCreatedShardBitmap();
    const ui64 fsBitmapBitCount = Max<ui64>(
        fsPersistentShardBitmap.GetBitCount(),
        ShardCreationState.GetTargetShardCount());
    auto bitmap = LoadCompressedBitmap(
        fsPersistentShardBitmap,
        Max<ui64>(fsBitmapBitCount, targetShardCount),
        MaxShardCount);
    if (HasError(bitmap)) {
        return bitmap.GetError();
    }

    const auto canResetOldPlan = CanResetFsShardCreationState(
        topologyShardCount,
        targetShardCount,
        newShardConfigsHash,
        *bitmap.GetResult(),
        fsBitmapBitCount);
    if (HasError(canResetOldPlan)) {
        return canResetOldPlan.GetError();
    }

    if (canResetOldPlan.GetResult()) {
        ShardCreationState.MutableCreatedShardBitmap()->Clear();
        ShardCreationState.MutableCreatedShardBitmap()->SetBitCount(
            targetShardCount);
        ShardCreationState.SetTopologyShardCount(topologyShardCount);
        ShardCreationState.SetTargetShardCount(targetShardCount);
        ShardCreationState.SetTargetShardConfigHash(newShardConfigsHash);
        bitmap.GetResult()->Clear();
    }

    ShardBitmapBitCount = targetShardCount;
    CreatedShardBitmap = bitmap.ExtractResult();

    return {};
}

void TShardCreationStateCompanion::InitializeShardCreation(
    const ui32 topologyShardCount,
    const ui32 targetShardCount)
{
    Y_DEBUG_ABORT_UNLESS(topologyShardCount <= targetShardCount);
    NextShardToCreate = topologyShardCount;
    ShardsToCreate = targetShardCount - topologyShardCount;
    TargetShardCount = targetShardCount;
}

void TShardCreationStateCompanion::StartShardCreation(
    const TActorContext& ctx,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs,
    const ui32 limit)
{
    ui32 requestsSent = 0;
    while (ShardsToCreate && NextShardToCreate < TargetShardCount &&
           (limit == 0 || requestsSent < limit))
    {
        if (IsShardCreated(NextShardToCreate)) {
            Y_DEBUG_ABORT_UNLESS(ShardsToCreate);
            --ShardsToCreate;
        } else {
            CreateShard(ctx, shardConfigs, NextShardToCreate);
            ++requestsSent;
        }
        ++NextShardToCreate;
    }
}

void TShardCreationStateCompanion::OnShardCreated(
    const TActorContext& ctx,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs,
    const ui32 limit)
{
    Y_DEBUG_ABORT_UNLESS(ShardsToCreate);
    --ShardsToCreate;

    if (ShardsToCreate && limit) {
        StartShardCreation(ctx, shardConfigs, 1);
    }
}

void TShardCreationStateCompanion::CreateShard(
    const TActorContext& ctx,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs,
    const ui32 shardIndex)
{
    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        shardConfigs[shardIndex]);

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
        shardIndex);
}

bool TShardCreationStateCompanion::HasShardsToCreate() const
{
    return ShardsToCreate != 0;
}

TResultOrError<bool>
TShardCreationStateCompanion::HasUnpersistedCreatedShards() const
{
    if (!CreatedShardBitmap) {
        return false;
    }

    const auto bitmap = LoadCompressedBitmap(
        ShardCreationState.GetCreatedShardBitmap(),
        ShardBitmapBitCount,
        MaxShardCount);
    if (HasError(bitmap)) {
        return bitmap.GetError();
    }
    const auto& persisted = *bitmap.GetResult();

    for (ui64 shardIndex = 0; shardIndex < ShardBitmapBitCount; ++shardIndex) {
        if (CreatedShardBitmap->Test(shardIndex) && !persisted.Test(shardIndex))
        {
            return true;
        }
    }

    return false;
}

void TShardCreationStateCompanion::ReadShardCreationState(
    const TActorContext& ctx) const
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();
    request->Record.SetFileSystemId(FileSystemId);
    // Unset Version means just return current state.
    request->Record.MutableShardCreationState();

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TShardCreationStateCompanion::UpdateShardCreationState(
    const TActorContext& ctx) const
{
    if (!CreatedShardBitmap) {
        LogStateUnavailable(ctx);
        return;
    }

    auto request =
        std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();
    request->Record.SetFileSystemId(FileSystemId);
    auto* shardCreationState = request->Record.MutableShardCreationState();
    shardCreationState->SetVersion(ShardCreationState.GetVersion());
    shardCreationState->SetTopologyShardCount(
        ShardCreationState.GetTopologyShardCount());
    shardCreationState->SetTargetShardCount(
        ShardCreationState.GetTargetShardCount());
    shardCreationState->SetTargetShardConfigHash(
        ShardCreationState.GetTargetShardConfigHash());
    SaveCompressedBitmap(
        *CreatedShardBitmap,
        ShardBitmapBitCount,
        *shardCreationState->MutableCreatedShardBitmap());

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TShardCreationStateCompanion::OnShardCreated(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    if (!CreatedShardBitmap) {
        LogStateUnavailable(ctx);
        return;
    }

    const bool validShardIndex =
        shardIndex < ShardCreationState.GetTargetShardCount();
    if (!validShardIndex) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Invalid created shard index: %u, topology: %u, target: %u",
            LogTag.c_str(),
            shardIndex,
            ShardCreationState.GetTopologyShardCount(),
            ShardCreationState.GetTargetShardCount());
        return;
    }

    CreatedShardBitmap->Set(shardIndex, shardIndex + 1);
    UpdateShardCreationState(ctx);
}

void TShardCreationStateCompanion::LogStateUnavailable(
    const TActorContext& ctx) const
{
    const char* message = "shard creation state unavailable";
    switch (Mode) {
        case EMode::Create:
            message =
                "Shard bitmap not initialized, "
                "shard creation state unavailable";
            break;
        case EMode::Alter:
            message =
                "FS topology not yet read, "
                "shard creation state unavailable";
            break;
    }

    LOG_WARN(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] %s",
        LogTag.c_str(),
        message);
}

TResultOrError<bool> TShardCreationStateCompanion::CanResetFsShardCreationState(
    const ui32 topologyShardCount,
    const ui32 targetShardCount,
    const ui64 newShardConfigsHash,
    const NCloud::TCompressedBitmap& fsBitmap,
    const ui64 fsBitmapBitCount) const
{
    const bool canResetOldPlan = topologyShardCount >= fsBitmapBitCount ||
        fsBitmap.Count(topologyShardCount, fsBitmapBitCount) == 0;
    if (!canResetOldPlan &&
        (ShardCreationState.GetTopologyShardCount() != topologyShardCount ||
         ShardCreationState.GetTargetShardCount() != targetShardCount ||
         ShardCreationState.GetTargetShardConfigHash() != newShardConfigsHash))
    {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "Unfinished shard creation state conflicts with current plan"
                << " for filesystem " << FileSystemId.Quote()
                << ": stored topology="
                << ShardCreationState.GetTopologyShardCount()
                << ", target=" << ShardCreationState.GetTargetShardCount()
                << ", hash=" << ShardCreationState.GetTargetShardConfigHash()
                << "; current topology=" << topologyShardCount << ", target="
                << targetShardCount << ", hash=" << newShardConfigsHash
                << ". Retry the previous create/resize request with the same"
                << " parameters or ask devops to reconcile already-created shards");
    }

    return canResetOldPlan;
}

}   // namespace NCloud::NFileStore::NStorage
