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

// Calculate a hash of configs for shards added by this plan, starting at
// topologyShardCount. Ignore operational metadata that changes between retries.
TResultOrError<ui64> CalculateNewShardConfigsHash(
    ui32 topologyShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs);

////////////////////////////////////////////////////////////////////////////////

// Tracks shard creation progress for one create/resize operation. HandleResponse
// records the state read from the main Tablet. The caller invokes
// SetupCreatedShardBitmap once the committed topology and desired shard configs
// are known.
//
// CreatedShardBitmap contains persisted bits and local creation successes;
// ShardCreationState holds the last received state, or the plan prepared by
// SetupCreatedShardBitmap. HandleResponse reconciles both after an update.
//
// The companion schedules shard creation requests. The actor handles responses,
// retries, and the fallback for Tablets without persistent state support.
class TShardCreationStateCompanion
{
public:
    enum class EMode
    {
        Create,
        Alter,
    };

    enum class EResponseAction
    {
        // The Tablet did not return state on the initial read.
        StartWithoutPersistentState,
        // The initial persistent state was recorded.
        InitialStateLoaded,
        // A newer response has already been recorded.
        IgnoreStaleResponse,
        // Local creation progress is absent from the persistent bitmap.
        PersistLocalProgress,
        // All locally created shards are recorded in persistent storage.
        Continue,
    };

private:
    enum class EMergeAction
    {
        IgnoreStaleResponse,
        PersistLocalProgress,
        Continue,
    };

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
    ui32 NextShardToCreate = 0;
    ui32 ShardsToCreate = 0;
    ui32 TargetShardCount = 0;

public:
    // Track shard creation state for fileSystemId in the given mode.
    TShardCreationStateCompanion(TString fileSystemId, EMode mode);

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
    // Topology shard count when this shard creation/resize plan started.
    ui32 GetTopologyShardCount() const;
    // Total number of shards after this plan completes.
    ui32 GetTargetShardCount() const;
    // Hash of configs for shards added by this plan.
    ui64 GetNewShardConfigsHash() const;

    // Record state returned by the Tablet without initializing the working bitmap.
    void SetShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);
    // Reconcile a Tablet response with local progress and return the actor's action.
    TResultOrError<EResponseAction> HandleResponse(
        const NProtoPrivate::TUnsafeChangeTabletStateResponse& response,
        const NActors::TActorContext& ctx);
    // Initialize the working bitmap for the requested plan; reject a conflicting
    // plan if previously created shards have not joined the committed topology.
    NProto::TError SetupCreatedShardBitmap(
        ui32 topologyShardCount,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs);

    // Set the range [topologyShardCount, targetShardCount) of shards to create.
    // topologyShardCount is the current committed topology size.
    void InitializeShardCreation(ui32 topologyShardCount, ui32 targetShardCount);
    // Send creation requests for up to limit shards. Zero means no limit.
    // Advance the cursor after each request is sent.
    void StartShardCreation(
        const NActors::TActorContext& ctx,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs,
        ui32 limit);
    // Account for a successful creation response and send the next request
    // when the number of in-flight requests is limited.
    void OnShardCreated(
        const NActors::TActorContext& ctx,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs,
        ui32 limit);
    // Whether any shards in the range still need to be created.
    bool HasShardsToCreate() const;

    // Request the current state from the Tablet; HandleResponse processes the reply.
    void ReadShardCreationState(const NActors::TActorContext& ctx) const;
    // Send the current shard creation state and working bitmap to the Tablet
    // with the expected version.
    void UpdateShardCreationState(const NActors::TActorContext& ctx) const;
    // Mark a shard created locally and send the updated persistent state
    // to the Tablet.
    void OnShardCreated(const NActors::TActorContext& ctx, ui32 shardIndex);

private:
    void CreateShard(
        const NActors::TActorContext& ctx,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs,
        ui32 shardIndex);
    // Ignore stale responses. For a current response, verify that its shard
    // creation/resize plan matches the request, merge its created shard bits
    // with local progress, and record the returned persistent state.
    TResultOrError<EMergeAction> MergeShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);
    TResultOrError<bool> HasUnpersistedCreatedShards() const;
    void LogStateUnavailable(const NActors::TActorContext& ctx) const;

    // Return whether the saved plan can be reset because its bitmap has no bits
    // outside the current topology. Reject a conflicting plan otherwise.
    TResultOrError<bool> CanResetFsShardCreationState(
        ui32 topologyShardCount,
        ui32 targetShardCount,
        ui64 newShardConfigsHash,
        const NCloud::TCompressedBitmap& fsBitmap,
        ui64 fsBitmapBitCount) const;
};

}   // namespace NCloud::NFileStore::NStorage
