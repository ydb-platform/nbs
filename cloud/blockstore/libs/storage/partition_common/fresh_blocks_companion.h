#pragma once

#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/long_running_operation_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/part_channels_state.h>
#include <cloud/blockstore/libs/storage/partition_common/part_fresh_blocks_state.h>

#include <cloud/storage/core/libs/actors/mortal_actor.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class IFreshBlocksCompanionClient: public IMortalActor
{
public:
    virtual void FreshBlobsLoaded(const NActors::TActorContext& ctx) = 0;

    ~IFreshBlocksCompanionClient() override = default;
};

class TFreshBlocksCompanion
{
private:
    const EStorageAccessMode StorageAccessMode;
    const NProto::TPartitionConfig PartitionConfig;
    const NKikimr::TTabletStorageInfoPtr TabletStorageInfo;

    IFreshBlocksCompanionClient& Client;

    TPartitionChannelsState& ChannelsState;
    TPartitionFreshBlobState& FreshBlobState;
    TPartitionFlushState& FlushState;
    TPartitionTrimFreshLogState& TrimFreshLogState;
    TPartitionFreshBlocksState& FreshBlocksState;

    TLogTitle LogTitle;

    TRunningActors Actors;

public:
    TFreshBlocksCompanion(
        EStorageAccessMode storageAccessMode,
        NProto::TPartitionConfig partitionConfig,
        NKikimr::TTabletStorageInfo* tabletStorageInfo,
        IFreshBlocksCompanionClient& client,
        TPartitionChannelsState& channelsState,
        TPartitionFreshBlobState& freshBlobState,
        TPartitionFlushState& flushState,
        TPartitionTrimFreshLogState& trimFreshLogState,
        TPartitionFreshBlocksState& freshBlocksState,
        TLogTitle logTitle);

    void LoadFreshBlobs(
        const NActors::TActorContext& ctx,
        ui64 persistedTrimFreshLogToCommitId);

    void HandleLoadFreshBlobsCompleted(
        const TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

private:
    auto Info()
    {
        return TabletStorageInfo;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
