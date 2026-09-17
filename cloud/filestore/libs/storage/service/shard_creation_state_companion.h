#pragma once

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/compressed_bitmap.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

ui64 CalculateShardCreationTargetHash(
    ui32 baseShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs);

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
    ui32 GetBaseShardCount() const;
    ui32 GetTargetShardCount() const;
    ui64 GetTargetShardConfigHash() const;

    void SetShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);

    NProto::TError SetupCreatedShardBitmap(
        ui32 baseShardCount,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs);

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

    bool HasUncommittedCreatedShards(ui32 committedShardCount) const;

    NProto::TError ValidateOrResetShardCreationState(
        ui32 baseShardCount,
        ui32 targetShardCount,
        ui64 targetShardConfigHash);
};

}   // namespace NCloud::NFileStore::NStorage
