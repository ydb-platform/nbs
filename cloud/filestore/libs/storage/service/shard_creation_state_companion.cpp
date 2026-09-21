#include "shard_creation_state_companion.h"

#include <cloud/filestore/libs/storage/api/components.h>
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
    auto targetConfig = shardConfig;

    // Hash the desired shard config conservatively, excluding only operational
    // metadata that may change between retries.
    targetConfig.ClearVersion();
    targetConfig.ClearCreationTs();
    targetConfig.ClearAlterTs();
    return targetConfig;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<ui64> CalculateShardCreationTargetHash(
    const ui32 baseShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs)
{
    if (baseShardCount > shardConfigs.size()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Invalid shard creation range: base=" << baseShardCount
                << ", target=" << shardConfigs.size());
    }

    ui64 hash = ShardCreationStateHashSeed;
    for (size_t shardIndex = baseShardCount; shardIndex < shardConfigs.size();
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
                    << " while calculating shard creation target hash");
        }

        hash = CityHash64WithSeed(serializedConfig, hash);
    }

    return hash;
}

////////////////////////////////////////////////////////////////////////////////

TShardCreationStateCompanion::TShardCreationStateCompanion(
    TString fileSystemId,
    TString logTag,
    EMode mode)
    : FileSystemId(std::move(fileSystemId))
    , LogTag(std::move(logTag))
    , Mode(mode)
{}

bool TShardCreationStateCompanion::IsPersistentStateRead() const
{
    return PersistentShardCreationStateStatus !=
           EPersistentShardCreationStateStatus::Unknown;
}

bool TShardCreationStateCompanion::IsPersistentStateSupported() const
{
    return PersistentShardCreationStateStatus ==
           EPersistentShardCreationStateStatus::Supported;
}

void TShardCreationStateCompanion::MarkPersistentStateUnsupported()
{
    PersistentShardCreationStateStatus =
        EPersistentShardCreationStateStatus::Unsupported;
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

ui32 TShardCreationStateCompanion::GetBaseShardCount() const
{
    return ShardCreationState.GetBaseShardCount();
}

ui32 TShardCreationStateCompanion::GetTargetShardCount() const
{
    return ShardCreationState.GetTargetShardCount();
}

ui64 TShardCreationStateCompanion::GetTargetShardConfigHash() const
{
    return ShardCreationState.GetTargetShardConfigHash();
}

void TShardCreationStateCompanion::SetShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& state)
{
    PersistentShardCreationStateStatus =
        EPersistentShardCreationStateStatus::Supported;
    ShardCreationState = state;
}

NProto::TError TShardCreationStateCompanion::ValidateTarget(
    const NProtoPrivate::TFileSystemShardCreationState& state) const
{
    if (ShardCreationState.GetBaseShardCount() == state.GetBaseShardCount() &&
        ShardCreationState.GetTargetShardCount() ==
            state.GetTargetShardCount() &&
        ShardCreationState.GetTargetShardConfigHash() ==
            state.GetTargetShardConfigHash())
    {
        return {};
    }

    return MakeError(
        E_INVALID_STATE,
        TStringBuilder()
            << "Shard creation target changed while request was in progress"
            << " for filesystem " << FileSystemId.Quote()
            << ": request base=" << ShardCreationState.GetBaseShardCount()
            << ", target=" << ShardCreationState.GetTargetShardCount()
            << ", hash=" << ShardCreationState.GetTargetShardConfigHash()
            << "; stored base=" << state.GetBaseShardCount()
            << ", target=" << state.GetTargetShardCount()
            << ", hash=" << state.GetTargetShardConfigHash()
            << ". Retry the request after the concurrent create/resize"
            << " operation completes");
}

bool TShardCreationStateCompanion::MergeShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& state)
{
    MergeCreatedShardBitmap(state.GetCreatedShardBitmap());
    SetShardCreationState(state);
    return HasUnpersistedCreatedShards();
}

NProto::TError TShardCreationStateCompanion::SetupCreatedShardBitmap(
    const ui32 baseShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs)
{
    const ui32 targetShardCount = shardConfigs.size();
    if (baseShardCount > targetShardCount) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Invalid shard creation range for filesystem "
                << FileSystemId.Quote() << ": base=" << baseShardCount
                << ", target=" << targetShardCount);
    }

    const auto targetShardConfigHashResult =
        CalculateShardCreationTargetHash(baseShardCount, shardConfigs);
    if (HasError(targetShardConfigHashResult)) {
        return targetShardConfigHashResult.GetError();
    }
    const ui64 targetShardConfigHash = targetShardConfigHashResult.GetResult();

    auto error = ValidateOrResetShardCreationState(
        baseShardCount,
        targetShardCount,
        targetShardConfigHash);
    if (HasError(error)) {
        return error;
    }

    ShardBitmapBitCount = targetShardCount;
    CreatedShardBitmap =
        std::make_unique<NCloud::TCompressedBitmap>(LoadCompressedBitmap(
            ShardCreationState.GetCreatedShardBitmap(),
            ShardBitmapBitCount));

    return {};
}

void TShardCreationStateCompanion::MergeCreatedShardBitmap(
    const NProtoPrivate::TCompressedBitmapData& bitmap)
{
    if (!CreatedShardBitmap) {
        return;
    }

    for (const auto& chunk: bitmap.GetChunks()) {
        CreatedShardBitmap->Merge(
            {.ChunkIdx = chunk.GetChunkIdx(), .Data = chunk.GetData()});
    }
}

bool TShardCreationStateCompanion::HasUnpersistedCreatedShards() const
{
    if (!CreatedShardBitmap) {
        return false;
    }

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

void TShardCreationStateCompanion::UpdateShardCreationState(
    const TActorContext& ctx) const
{
    if (!CreatedShardBitmap) {
        LogStateUnavailable(ctx);
        return;
    }

    // Implement this block after adding ShardCreationState
    // into TEvUnsafeChangeTabletStateRequest
    Y_ABORT("add ShardCreationState into TEvUnsafeChangeTabletStateRequest");

    // auto request =
    // std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();
    // request->Record.SetFileSystemId(FileSystemId);
    // auto* shardCreationState = request->Record.MutableShardCreationState();
    // shardCreationState->SetVersion(ShardCreationState.GetVersion());
    // shardCreationState->SetBaseShardCount(
    //     ShardCreationState.GetBaseShardCount());
    // shardCreationState->SetTargetShardCount(
    //     ShardCreationState.GetTargetShardCount());
    // shardCreationState->SetTargetShardConfigHash(
    //     ShardCreationState.GetTargetShardConfigHash());
    // SaveCompressedBitmap(
    //     *CreatedShardBitmap,
    //     ShardBitmapBitCount,
    //     *shardCreationState->MutableCreatedShardBitmap());

    // NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TShardCreationStateCompanion::UpdateShardCreatedState(
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
            "[%s] Invalid created shard index: %u, base: %u, target: %u",
            LogTag.c_str(),
            shardIndex,
            ShardCreationState.GetBaseShardCount(),
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

bool TShardCreationStateCompanion::HasUncommittedCreatedShards(
    const ui32 committedShardCount) const
{
    const ui32 targetShardCount = ShardCreationState.GetTargetShardCount();
    const ui64 bitCount = Max<ui64>(
        ShardCreationState.GetCreatedShardBitmap().GetBitCount(),
        targetShardCount);

    const auto bitmap = LoadCompressedBitmap(
        ShardCreationState.GetCreatedShardBitmap(),
        bitCount);

    for (ui64 shardIndex = 0; shardIndex < bitCount; ++shardIndex) {
        if (bitmap.Test(shardIndex) && shardIndex >= committedShardCount) {
            return true;
        }
    }

    return false;
}

NProto::TError TShardCreationStateCompanion::ValidateOrResetShardCreationState(
    const ui32 baseShardCount,
    const ui32 targetShardCount,
    const ui64 targetShardConfigHash)
{
    if (!HasUncommittedCreatedShards(baseShardCount)) {
        ShardCreationState.MutableCreatedShardBitmap()->Clear();
        ShardCreationState.MutableCreatedShardBitmap()->SetBitCount(
            targetShardCount);
        ShardCreationState.SetBaseShardCount(baseShardCount);
        ShardCreationState.SetTargetShardCount(targetShardCount);
        ShardCreationState.SetTargetShardConfigHash(targetShardConfigHash);
        return {};
    }

    if (ShardCreationState.GetBaseShardCount() == baseShardCount &&
        ShardCreationState.GetTargetShardCount() == targetShardCount &&
        ShardCreationState.GetTargetShardConfigHash() == targetShardConfigHash)
    {
        return {};
    }

    return MakeError(
        E_INVALID_STATE,
        TStringBuilder()
            << "Unfinished shard creation state conflicts with current target"
            << " for filesystem " << FileSystemId.Quote()
            << ": stored base=" << ShardCreationState.GetBaseShardCount()
            << ", target=" << ShardCreationState.GetTargetShardCount()
            << ", hash=" << ShardCreationState.GetTargetShardConfigHash()
            << "; current base=" << baseShardCount << ", target="
            << targetShardCount << ", hash=" << targetShardConfigHash
            << ". Retry the previous create/resize request with the same"
            << " parameters or ask devops to reconcile already-created shards");
}

}   // namespace NCloud::NFileStore::NStorage
