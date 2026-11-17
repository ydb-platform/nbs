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

// Tracks shard creation progress for one create/resize operation. The caller
// records the state read from the main Tablet, then calls
// SetupCreatedShardBitmap once the committed topology and desired shard configs
// are known.
//
// CreatedShardBitmap contains persisted bits and local creation successes;
// ShardCreationState holds the last received state, or the plan prepared by
// SetupCreatedShardBitmap.
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
    // Send creation requests up to the configured limit.
    // Advance the cursor after each request is sent.
    void StartShardCreation(const NActors::TActorContext& ctx);
    // Account for a successful response for shardIndex and send the next
    // request when the number of in-flight requests is limited. Return an
    // error if no more shard creation acknowledgements are expected.
    NProto::TError OnShardCreated(
        const NActors::TActorContext& ctx,
        ui32 shardIndex);
    // Whether any shards in the range still need to be created.
    bool HasShardsToCreate() const;

private:
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
