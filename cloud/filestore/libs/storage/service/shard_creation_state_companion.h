#pragma once

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <cloud/storage/core/libs/common/compressed_bitmap_serialization.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Calculate a hash of configs for shards added by this plan, starting at
// baseShardCount. Ignore operational metadata that changes between retries.
TResultOrError<ui64> CalculateNewShardConfigsHash(
    ui32 baseShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs);

////////////////////////////////////////////////////////////////////////////////

// Tracks shard creation progress for one create/resize operation. The caller
// records the state read from the main Tablet, then calls
// SetupCreatedShardBitmap once the committed topology and desired shard configs
// are known.
//
// CreatedShardBitmap contains persisted bits and local creation successes;
// ShardCreationState holds the last received state, or the plan prepared by
// SetupCreatedShardBitmap.
//
// The actor owns request scheduling, retries, and the fallback for Tablets that
// do not support persistent state. This companion does not handle actor events.
class TShardCreationStateCompanion
{
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

    EShardCreationStateStatus ShardCreationStateStatus =
        EShardCreationStateStatus::Unknown;
    NProtoPrivate::TFileSystemShardCreationState ShardCreationState;
    ui64 ShardBitmapBitCount = 0;
    std::unique_ptr<NCloud::TCompressedBitmap> CreatedShardBitmap;

public:
    // Track shard creation state for fileSystemId.
    explicit TShardCreationStateCompanion(TString fileSystemId);

    // Whether the initial read completed, including an unsupported response.
    bool IsPersistentStateLoaded() const;
    // Whether the Tablet returned persistent shard creation state.
    bool IsPersistentStateSupported() const;
    // Record that the initial read found no persistent state support.
    void MarkPersistentStateUnsupported();

    // Whether the working bitmap is initialized for the current plan.
    bool HasCreatedShardBitmap() const;
    // Whether this shard is marked created in persisted or local progress.
    bool IsShardCreated(ui32 shardIndex) const;

    // Version of the locally held persistent state, used for conditional updates.
    ui32 GetShardCreationStateVersion() const;
    // Number of shards in the committed topology before this plan.
    ui32 GetBaseShardCount() const;
    // Total number of shards after this plan completes.
    ui32 GetTargetShardCount() const;
    // Hash of configs for shards added by this plan.
    ui64 GetNewShardConfigsHash() const;

    // Record state returned by the Tablet without initializing the working bitmap.
    void SetShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);
    // Initialize the working bitmap for the requested plan; reject a conflicting
    // plan if previously created shards have not joined the committed topology.
    NProto::TError SetupCreatedShardBitmap(
        ui32 baseShardCount,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs);

private:
    TResultOrError<bool> HasUncommittedCreatedShards(
        ui32 committedShardCount) const;

    NProto::TError ValidateOrResetShardCreationState(
        ui32 baseShardCount,
        ui32 targetShardCount,
        ui64 targetShardConfigHash);
};

}   // namespace NCloud::NFileStore::NStorage
