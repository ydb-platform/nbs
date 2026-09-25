#pragma once

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/compressed_bitmap.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<ui64> CalculateShardCreationTargetHash(
    ui32 baseShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs);

////////////////////////////////////////////////////////////////////////////////

class TShardCreationStateCompanion
{
public:
    enum class EMode
    {
        Create,
        Alter,
    };

private:
    enum class EPersistentShardCreationStateStatus
    {
        Unknown,
        Unsupported,
        Supported,
    };

    const TString FileSystemId;
    const TString LogTag;
    const EMode Mode;

    EPersistentShardCreationStateStatus PersistentShardCreationStateStatus =
        EPersistentShardCreationStateStatus::Unknown;
    NProtoPrivate::TFileSystemShardCreationState ShardCreationState;
    ui64 ShardBitmapBitCount = 0;
    std::unique_ptr<NCloud::TCompressedBitmap> CreatedShardBitmap;

public:
    TShardCreationStateCompanion(
        TString fileSystemId,
        TString logTag,
        EMode mode);

    bool IsPersistentStateRead() const;
    bool IsPersistentStateSupported() const;
    void MarkPersistentStateUnsupported();

    bool HasCreatedShardBitmap() const;
    bool IsShardCreated(ui32 shardIndex) const;

    ui32 GetShardCreationStateVersion() const;
    ui32 GetBaseShardCount() const;
    ui32 GetTargetShardCount() const;
    ui64 GetTargetShardConfigHash() const;

    void SetShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);
    NProto::TError ValidateTarget(
        const NProtoPrivate::TFileSystemShardCreationState& state) const;
    bool MergeShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);

    NProto::TError SetupCreatedShardBitmap(
        ui32 baseShardCount,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs);

    void UpdateShardCreationState(const NActors::TActorContext& ctx) const;
    void UpdateShardCreatedState(
        const NActors::TActorContext& ctx,
        ui32 shardIndex);
    void LogStateUnavailable(const NActors::TActorContext& ctx) const;

private:
    void MergeCreatedShardBitmap(
        const NProtoPrivate::TCompressedBitmapData& bitmap);
    bool HasUnpersistedCreatedShards() const;
    bool HasUncommittedCreatedShards(ui32 committedShardCount) const;

    NProto::TError ValidateOrResetShardCreationState(
        ui32 baseShardCount,
        ui32 targetShardCount,
        ui64 targetShardConfigHash);
};

}   // namespace NCloud::NFileStore::NStorage
