#pragma once

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/compressed_bitmap.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <util/generic/string.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TShardCreationStateCompanion
{
private:
    const TString FileSystemId;
    const TString LogTag;
    const TString StateUnavailableMessage;

    NProtoPrivate::TFileSystemShardCreationState ShardCreationState;
    ui32 ShardCreationStateVersion = 0;
    ui64 ShardBitmapBitCount = 0;
    std::unique_ptr<NCloud::TCompressedBitmap> CreatedShardBitmap;

public:
    TShardCreationStateCompanion(
        TString fileSystemId,
        TString logTag,
        TString stateUnavailableMessage);

    bool HasCreatedShardBitmap() const;
    bool IsShardCreated(ui32 shardIndex) const;

    ui32 GetShardCreationStateVersion() const;
    void SetShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);

    void SetupCreatedShardBitmap(ui64 shardCount);
    void MergeCreatedShardBitmap(
        const NProtoPrivate::TCompressedBitmapData& bitmap);
    bool HasUnpersistedCreatedShards() const;

    void UpdateShardCreationState(const NActors::TActorContext& ctx) const;
    void UpdateShardCreatedState(
        const NActors::TActorContext& ctx,
        ui32 shardIndex);

private:
    void LogShardCreationStateUnavailable(
        const NActors::TActorContext& ctx) const;
};

}   // namespace NCloud::NFileStore::NStorage
