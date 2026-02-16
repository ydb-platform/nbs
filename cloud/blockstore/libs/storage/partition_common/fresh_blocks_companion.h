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
    const TStorageConfigPtr Config;
    const EStorageAccessMode StorageAccessMode;
    const NProto::TPartitionConfig PartitionConfig;
    const NKikimr::TTabletStorageInfoPtr TabletStorageInfo;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

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
        TStorageConfigPtr config,
        EStorageAccessMode storageAccessMode,
        NProto::TPartitionConfig partitionConfig,
        NKikimr::TTabletStorageInfo* tabletStorageInfo,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        IFreshBlocksCompanionClient& client,
        TPartitionChannelsState& channelsState,
        TPartitionFreshBlobState& freshBlobState,
        TPartitionFlushState& flushState,
        TPartitionTrimFreshLogState& trimFreshLogState,
        TPartitionFreshBlocksState& freshBlocksState,
        TLogTitle logTitle);

    void KillActors(const NActors::TActorContext& ctx);

    void LoadFreshBlobs(
        const NActors::TActorContext& ctx,
        ui64 persistedTrimFreshLogToCommitId);

    void WriteFreshBlocks(
        const NActors::TActorContext& ctx,
        TArrayRef<TRequestInBuffer<TWriteBufferRequestData>> requestsInBuffer,
        ui64 commitId);

    void WriteFreshBlocksCompleted(
        const NActors::TActorContext& ctx,
        const NProto::TError& error,
        ui64 commitId,
        ui64 blockCount,
        NActors::TActorId actorId);

    void ZeroFreshBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        TBlockRange32 writeRange,
        ui64 commitId);

    void ZeroFreshBlocksCompleted(
        const NActors::TActorContext& ctx,
        const NProto::TError& error,
        ui64 commitId,
        ui64 blockCount,
        NActors::TActorId actorId);

    void RebootOnCommitIdOverflow(
        const NActors::TActorContext& ctx,
        const TStringBuf& requestName);

public:
    void HandleLoadFreshBlobsCompleted(
        const TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddFreshBlocks(
        const TEvPartitionCommonPrivate::TEvAddFreshBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

private:
    auto Info()
    {
        return TabletStorageInfo;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
