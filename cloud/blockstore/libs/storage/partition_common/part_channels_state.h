#pragma once

#include <cloud/blockstore/libs/storage/api/public.h>
#include <cloud/blockstore/libs/storage/core/bs_group_operation_tracker.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/model/channel_permissions.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <list>
#include <utility>

namespace NCloud::NBlockStore::NStorage {

//////////////////////////////////////////////////////////////////////////////////

struct TQueuedRequest
{
    NActors::IActorPtr Actor;
    ui64 BSGroupOperationId = 0;
    ui32 Group = 0;
    TBSGroupOperationTimeTracker::EOperationType OperationType =
        TBSGroupOperationTimeTracker::EOperationType::Read;
    ui32 BlockSize = 0;
};

//////////////////////////////////////////////////////////////////////////////////

struct TChannelState
{
    EChannelPermissions Permissions = EChannelPermission::UserWritesAllowed
        | EChannelPermission::SystemWritesAllowed;
    double ApproximateFreeSpaceShare = 0;
    double FreeSpaceScore = 0;
    bool ReassignRequestedByBlobStorage = false;

    std::list<TQueuedRequest> IORequests;
    size_t IORequestsInFlight = 0;
    size_t IORequestsQueued = 0;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TFreeSpaceConfig
{
    double ChannelFreeSpaceThreshold = 0;
    double ChannelMinFreeSpace = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChannelsState
{
private:
    NProto::TPartitionConfig Config;
    const TFreeSpaceConfig FreeSpaceConfig;

    TVector<TChannelState> Channels;
    TVector<ui32> FreshChannels;
    ui32 FreshChannelSelector = -1;
    TVector<ui32> MixedChannels;
    bool HaveSeparateMixedChannels = false;
    ui32 MixedChannelSelector = -1;
    TVector<ui32> MergedChannels;
    ui32 MergedChannelSelector = -1;
    double SystemChannelSpaceScoreSum = 0;
    double DataChannelSpaceScoreSum = 0;
    double FreshChannelSpaceScoreSum = 0;
    double BackpressureDiskSpaceScore = 1;
    ui32 ChannelCount = 0;
    ui32 DataChannelCount = 0;
    ui32 FreshChannelCount = 0;
    ui32 AlmostFullChannelCount = 0;

    const ui32 MaxIORequestsInFlight;
    const ui32 ReassignChannelsPercentageThreshold;
    const ui32 ReassignFreshChannelsPercentageThreshold;
    const ui32 ReassignMixedChannelsPercentageThreshold;
    const bool ReassignSystemChannelsImmediately;

public:
    TPartitionChannelsState(
        NProto::TPartitionConfig config,
        const TFreeSpaceConfig& freeSpaceConfig,
        ui32 maxIORequestsInFlight,
        ui32 reassignChannelsPercentageThreshold,
        ui32 reassignFreshChannelsPercentageThreshold,
        ui32 reassignMixedChannelsPercentageThreshold,
        bool reassignSystemChannelsImmediately,
        ui32 channelCount);

    ui32 GetChannelCount() const
    {
        return ChannelCount;
    }

    ui32 GetFreshChannelCount() const
    {
        return FreshChannelCount;
    }

    EChannelDataKind GetChannelDataKind(ui32 channel) const;
    TVector<ui32> GetChannelsByKind(
        std::function<bool(EChannelDataKind)> predicate) const;

    bool UpdatePermissions(ui32 channel, EChannelPermissions permissions);
    bool CheckPermissions(ui32 channel, EChannelPermissions permissions) const;
    double GetFreeSpaceShare(ui32 channel) const;
    bool UpdateChannelFreeSpaceShare(ui32 channel, double share);
    bool CheckChannelFreeSpaceShare(ui32 channel) const;
    bool IsCompactionAllowed() const;
    bool IsWriteAllowed(EChannelPermissions permissions) const;
    void RegisterReassignRequestFromBlobStorage(ui32 channel);
    TVector<ui32> GetChannelsToReassign() const;
    ui32 GetAlmostFullChannelCount() const;
    void EnqueueIORequest(
        ui32 channel,
        NActors::IActorPtr requestActor,
        ui64 bsGroupOperationId,
        ui32 group,
        TBSGroupOperationTimeTracker::EOperationType operationType,
        ui32 blockSize);
    std::optional<TQueuedRequest> DequeueIORequest(ui32 channel);
    void CompleteIORequest(ui32 channel);
    ui32 GetIORequestsInFlight() const;
    ui32 GetIORequestsQueued() const;

    TPartialBlobId GenerateBlobId(
        EChannelDataKind kind,
        EChannelPermissions permissions,
        ui64 commitId,
        ui32 blobSize,
        ui32 blobIndex = 0);

    ui32 PickNextChannel(
        EChannelDataKind kind,
        EChannelPermissions permissions);

    double GetBackpressureDiskSpaceScore() const {
        return BackpressureDiskSpaceScore;
    }

protected:
    void InitChannels();

    TChannelState& GetChannel(ui32 channel);
    const TChannelState* GetChannel(ui32 channel) const;

    bool UpdateChannelFreeSpaceScore(TChannelState& channelState, ui32 channel);
};

}   // namespace NCloud::NBlockStore::NStorage
