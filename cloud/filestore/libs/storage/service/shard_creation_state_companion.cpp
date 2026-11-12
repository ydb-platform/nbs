#include "shard_creation_state_companion.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/digest/city.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

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

TShardCreationStateCompanion::TShardCreationStateCompanion(TString fileSystemId)
    : FileSystemId(std::move(fileSystemId))
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
