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

class IShardCreationOwner
{
public:
    virtual ~IShardCreationOwner() = default;

    // Send a CreateFileStore request for the owner's shard config at shardIndex
    // to SSProxy.
    // Use shardIndex as the response cookie.
    virtual void CreateShard(
        const NActors::TActorContext& ctx,
        ui32 shardIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Tracks shard creation progress for one create/resize operation.
// HandleShardCreationStateResponse records the state read from the main Tablet.
// The caller invokes
// SetupCreatedShardBitmap once the committed topology and desired shard configs
// are known.
//
// CreatedShardBitmap contains persisted bits and local creation successes;
// ShardCreationState holds the last received state, or the plan prepared by
// SetupCreatedShardBitmap. HandleShardCreationStateResponse reconciles both
// after an update.
//
// The companion schedules shard creation requests and records successful
// responses. Actors handle failures and the fallback for Tablets without
// persistent state support.
class TShardCreationStateCompanion
{
public:
    // Passed to the ctor selecting create/alter FS actor mode.
    enum class EMode
    {
        Create,
        Alter,
    };

    // Returned by HandleShardCreationStateResponse to tell its caller how to
    // proceed after an UnsafeChangeTabletState response.
    enum class EChangeTabletStateResponseAction
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
    // Returned by MergeShardCreationState and translated into an action for
    // the caller by HandleShardCreationStateResponse.
    enum class EMergeAction
    {
        // A newer version has already been recorded; discard this response.
        IgnoreStaleResponse,
        // Local shard creation progress is missing from the Tablet bitmap.
        PersistLocalProgress,
        // The Tablet bitmap includes all locally created shards.
        Continue,
    };

    // Tracks completion of the initial Tablet state read and whether the
    // Tablet supports persistent shard creation state.
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
    IShardCreationOwner* const Owner;

    EShardCreationStateStatus ShardCreationStateStatus =
        EShardCreationStateStatus::Unknown;
    NProtoPrivate::TFileSystemShardCreationState ShardCreationState;
    ui64 ShardBitmapBitCount = 0;
    std::unique_ptr<NCloud::TCompressedBitmap> CreatedShardBitmap;

    ui32 NextShardToCreate = 0;
    ui32 ShardsToCreate = 0;
    ui32 TargetShardCount = 0;
    ui32 MaxShardManagementRequestsInFlight = 0;

public:
    // Track shard creation state for fileSystemId in the given mode.
    TShardCreationStateCompanion(
        TString fileSystemId,
        EMode mode,
        IShardCreationOwner* owner);

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
    TResultOrError<EChangeTabletStateResponseAction>
    HandleShardCreationStateResponse(
        const NProtoPrivate::TUnsafeChangeTabletStateResponse& response,
        const NActors::TActorContext& ctx);
    // Initialize the working bitmap for the requested plan; reject a conflicting
    // plan if previously created shards have not joined the committed topology.
    NProto::TError SetupCreatedShardBitmap(
        ui32 topologyShardCount,
        const TVector<NKikimrFileStore::TConfig>& shardConfigs);

    // Set the range [topologyShardCount, targetShardCount) of shards to create
    // where topologyShardCount is the current shard count in the topology.
    // If maxShardManagementRequestsInFlight is zero, we send requests to
    // SchemeShard without limit (legacy mode).
    // Rejects any decrease number of shards requests.
    NProto::TError InitializeShardCreation(
        ui32 topologyShardCount,
        ui32 targetShardCount,
        ui32 maxShardManagementRequestsInFlight);
    // Send creation requests up to the configured limit. Already-created
    // shards do not consume the limit. Advance the cursor after each request.
    void StartShardCreation(const NActors::TActorContext& ctx);
    // Account for a successful response for shardIndex, update persistent
    // progress when supported, and send the next request when limited. Return
    // an error if no more shard creation acknowledgements are expected.
    NProto::TError OnShardCreated(
        const NActors::TActorContext& ctx,
        ui32 shardIndex);
    // Whether any shards in the range still need to be created.
    bool HasShardsToCreate() const;

    // Request the current state from the Tablet;
    // HandleShardCreationStateResponse processes the reply.
    void ReadShardCreationState(const NActors::TActorContext& ctx) const;
    // Send the current shard creation state and working bitmap to the Tablet
    // with the expected version.
    void UpdateShardCreationState(const NActors::TActorContext& ctx) const;

private:
    // Send up to limit creation requests, skipping shards already in the bitmap.
    // Zero limit sends all remaining requests.
    void SendShardCreationRequests(
        const NActors::TActorContext& ctx,
        ui32 limit);
    // Mark the shard in the working bitmap and send an update to the Tablet.
    // Log and skip if the bitmap is unavailable or the index is invalid.
    void RecordCreatedShard(
        const NActors::TActorContext& ctx,
        ui32 shardIndex);
    // Ignore stale responses. For a current response, verify that its shard
    // creation/resize plan matches the request, merge its created shard bits
    // with local progress, and record the returned persistent state.
    TResultOrError<EMergeAction> MergeShardCreationState(
        const NProtoPrivate::TFileSystemShardCreationState& state);
    // Check whether working bitmap bits are absent from the last Tablet state.
    TResultOrError<bool> HasUnpersistedCreatedShards() const;
    // Log why the current mode cannot update persistent shard creation state.
    void LogStateUnavailable(const NActors::TActorContext& ctx) const;
    // Return whether the saved creation/alteration plan can be reset because
    // its bitmap has no bits outside the current topology.
    // Reject a conflicting plan otherwise.
    TResultOrError<bool> CanResetFsShardCreationState(
        ui32 topologyShardCount,
        ui32 targetShardCount,
        ui64 newShardConfigsHash,
        const NCloud::TCompressedBitmap& fsBitmap,
        ui64 fsBitmapBitCount) const;
};

}   // namespace NCloud::NFileStore::NStorage
