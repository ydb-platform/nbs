#include "shard_creation_state_companion.h"

#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/log.h>

#include <util/system/yassert.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TShardCreationStateCompanion::TShardCreationStateCompanion(
        TString fileSystemId,
        TString logTag,
        TString stateUnavailableMessage)
    : FileSystemId(std::move(fileSystemId))
    , LogTag(std::move(logTag))
    , StateUnavailableMessage(std::move(stateUnavailableMessage))
{}

bool TShardCreationStateCompanion::HasCreatedShardBitmap() const
{
    return !!CreatedShardBitmap;
}

bool TShardCreationStateCompanion::IsShardCreated(const ui32 shardIndex) const
{
    return CreatedShardBitmap && CreatedShardBitmap->Test(shardIndex);
}

ui32 TShardCreationStateCompanion::GetShardCreationStateVersion() const
{
    return ShardCreationStateVersion;
}

void TShardCreationStateCompanion::SetShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& state)
{
    ShardCreationState = state;
    ShardCreationStateVersion = state.GetVersion();
}

void TShardCreationStateCompanion::SetupCreatedShardBitmap(
    const ui64 shardCount)
{
    ShardBitmapBitCount = shardCount;
    CreatedShardBitmap =
        std::make_unique<NCloud::TCompressedBitmap>(LoadCompressedBitmap(
            ShardCreationState.GetCreatedShardBitmap(),
            ShardBitmapBitCount));
}

void TShardCreationStateCompanion::MergeCreatedShardBitmap(
    const NProtoPrivate::TCompressedBitmapData& bitmap)
{
    Y_DEBUG_ABORT_UNLESS(CreatedShardBitmap);

    for (const auto& chunk: bitmap.GetChunks()) {
        CreatedShardBitmap->Merge(
            {.ChunkIdx = chunk.GetChunkIdx(), .Data = chunk.GetData()});
    }
}

bool TShardCreationStateCompanion::HasUnpersistedCreatedShards() const
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

void TShardCreationStateCompanion::UpdateShardCreationState(
    const TActorContext& ctx) const
{
    if (!CreatedShardBitmap) {
        LogShardCreationStateUnavailable(ctx);
        return;
    }

    auto request =
        std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();
    request->Record.SetFileSystemId(FileSystemId);
    auto* shardCreationState = request->Record.MutableShardCreationState();
    shardCreationState->SetVersion(ShardCreationStateVersion);
    SaveCompressedBitmap(
        *CreatedShardBitmap,
        ShardBitmapBitCount,
        *shardCreationState->MutableCreatedShardBitmap());

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TShardCreationStateCompanion::UpdateShardCreatedState(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    Y_DEBUG_ABORT_UNLESS(CreatedShardBitmap);
    if (!CreatedShardBitmap) {
        LogShardCreationStateUnavailable(ctx);
        return;
    }

    CreatedShardBitmap->Set(shardIndex, shardIndex + 1);
    UpdateShardCreationState(ctx);
}

void TShardCreationStateCompanion::LogShardCreationStateUnavailable(
    const TActorContext& ctx) const
{
    LOG_WARN(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] %s",
        LogTag.c_str(),
        StateUnavailableMessage.c_str());
}

}   // namespace NCloud::NFileStore::NStorage
