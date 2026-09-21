#pragma once

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/compressed_bitmap_serialization.h>
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

// Tracks shard creation progress for one create/resize actor. The actor reads
// state from the main Tablet and calls SetShardCreationState, then
// SetupCreatedShardBitmap once the committed topology and desired shard configs
// are known. A response without state calls for MarkPersistentStateUnsupported
// and the actor's non-persistent fallback instead.
//
// CreatedShardBitmap contains both persisted bits and local creation successes;
// ShardCreationState holds the last received state, or the target prepared by
// SetupCreatedShardBitmap. OnShardCreated records a successful shard creation
// and requests persistence. The actor handles responses, checks their version
// and target, and calls MergeShardCreationState. A true result means local bits
// are still missing from the received state and must be persisted again.
//
// The actor owns request scheduling, retries, and the fallback for Tablets that
// do not support persistent state. This companion does not handle actor events.
class TShardCreationStateCompanion
{
public:
    enum class EMode
    {
        Create,
        Alter,
    };

private:
    enum class EShardCreationStateStatus
    {
        // The initial state request has not completed yet.
        Unknown,
        // The Tablet replied without state; use the non-persistent flow.
        Unsupported,
        // The Tablet returned shard creation state.
        Supported,
    };

    const TString FileSystemId;
    const TString LogTag;
    const EMode Mode;

    EShardCreationStateStatus ShardCreationStateStatus =
        EShardCreationStateStatus::Unknown;
    NProtoPrivate::TFileSystemShardCreationState ShardCreationState;
    ui64 ShardBitmapBitCount = 0;
    std::unique_ptr<NCloud::TCompressedBitmap> CreatedShardBitmap;

public:
    TShardCreationStateCompanion(
        TString fileSystemId,
        TString logTag,
        EMode mode);

    // The initial read is complete, including an Unsupported response. This
    // does not imply that persistent state exists or the local bitmap is ready.
    bool IsPersistentStateLoaded() const;
    // The Tablet supports reading and updating persistent shard creation state.
    bool IsPersistentStateSupported() const;
    // Record an initial response from a Tablet without persistent state
    // support.
    void MarkPersistentStateUnsupported();

    // The working bitmap has been initialized for the current target.
    bool HasCreatedShardBitmap() const;
    // Whether the shard is known to be created, including unpersisted progress.
    bool IsShardCreated(ui32 shardIndex) const;

    ui32 GetShardCreationStateVersion() const;
    ui32 GetBaseShardCount() const;
    ui32 GetTargetShardCount() const;
    ui64 GetTargetShardConfigHash() const;

    // Record the received state and support status; does not initialize or
    // replace the local working bitmap.
    void SetShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);
    // Require the same creation range and config hash as the current target.
    NProto::TError ValidateTarget(
        const NProtoPrivate::TFileSystemShardCreationState& state) const;
    // Merge received bits with local progress and record the received state.
    // Returns whether another persistence request is needed. The caller checks
    // the response version and target before merging.
    TResultOrError<bool> MergeShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);

    // Prepare the working bitmap from state and the committed shard count.
    // Reject a different target while created shards remain uncommitted;
    // otherwise reset completed history for the new target.
    NProto::TError SetupCreatedShardBitmap(
        ui32 baseShardCount,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs);

    // Request persistence of the working bitmap using the last state version.
    // The actor handles the response and any concurrent update reconciliation.
    void UpdateShardCreationState(const NActors::TActorContext& ctx) const;
    // Record a successfully created shard locally and request persistence.
    void OnShardCreated(const NActors::TActorContext& ctx, ui32 shardIndex);
    void LogStateUnavailable(const NActors::TActorContext& ctx) const;

private:
    TResultOrError<bool> HasUnpersistedCreatedShards() const;
    TResultOrError<bool> HasUncommittedCreatedShards(
        ui32 committedShardCount) const;

    NProto::TError ValidateOrResetShardCreationState(
        ui32 baseShardCount,
        ui32 targetShardCount,
        ui64 targetShardConfigHash);
};

}   // namespace NCloud::NFileStore::NStorage
