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

TResultOrError<ui64> CalculateNewShardConfigsHash(
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

ui32 TShardCreationStateCompanion::GetBaseShardCount() const
{
    return ShardCreationState.GetBaseShardCount();
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
        CalculateNewShardConfigsHash(baseShardCount, shardConfigs);
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
    auto bitmap = LoadCompressedBitmap(
        ShardCreationState.GetCreatedShardBitmap(),
        ShardBitmapBitCount,
        MaxShardCount);
    if (HasError(bitmap)) {
        return bitmap.GetError();
    }

    CreatedShardBitmap = bitmap.ExtractResult();

    return {};
}

TResultOrError<bool> TShardCreationStateCompanion::HasUncommittedCreatedShards(
    const ui32 committedShardCount) const
{
    const ui32 targetShardCount = ShardCreationState.GetTargetShardCount();
    const ui64 bitCount = Max<ui64>(
        ShardCreationState.GetCreatedShardBitmap().GetBitCount(),
        targetShardCount);

    const auto bitmap = LoadCompressedBitmap(
        ShardCreationState.GetCreatedShardBitmap(),
        bitCount,
        MaxShardCount);
    if (HasError(bitmap)) {
        return bitmap.GetError();
    }
    const auto& createdShards = *bitmap.GetResult();

    for (ui64 shardIndex = 0; shardIndex < bitCount; ++shardIndex) {
        if (createdShards.Test(shardIndex) && shardIndex >= committedShardCount)
        {
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
    const auto hasUncommittedCreatedShards =
        HasUncommittedCreatedShards(baseShardCount);
    if (HasError(hasUncommittedCreatedShards)) {
        return hasUncommittedCreatedShards.GetError();
    }

    if (!hasUncommittedCreatedShards.GetResult()) {
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
            << "Unfinished shard creation state conflicts with current plan"
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
