#pragma once

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/fresh_blocks_writer.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/pending_request.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/io_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/part_channels_state.h>
#include <cloud/blockstore/libs/storage/partition_common/part_fresh_blocks_state.h>

#include <cloud/storage/core/libs/actors/poison_pill_helper.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

////////////////////////////////////////////////////////////////////////////////

struct TIOCompanionClient;

class TFreshBlocksWriterActor final
    : public NActors::TActorBootstrapped<TFreshBlocksWriterActor>
    , public IMortalActor
{
    using TBase = NActors::TActorBootstrapped<TFreshBlocksWriterActor>;

    friend TIOCompanionClient;

private:
    const TStorageConfigPtr Config;
    const NProto::TPartitionConfig PartitionConfig;
    const EStorageAccessMode StorageAccessMode;
    const ui64 PartitionTabletID;
    const NActors::TActorId PartitionActorId;
    const NBlockCodecs::ICodec* BlobCodec;
    const NActors::TActorId VolumeActorId;
    const TDiagnosticsConfigPtr DiagnosticsConfig;

    NKikimr::TTabletStorageInfoPtr TabletStorageInfo;

    TPoisonPillHelper PoisonPillHelper;

    std::unique_ptr<TPartitionChannelsState> ChannelsState;
    std::unique_ptr<TCommitIdsState> CommitIdsState;
    std::unique_ptr<TPartitionFlushState> FlushState;
    std::unique_ptr<TPartitionTrimFreshLogState> TrimFreshLogState;

    ui64 TabletGeneration = 0;

    bool StateLoaded = false;

    TDeque<TPendingRequest> PendingRequests;

    TLogTitle LogTitle;

    ui64 BSGroupOperationId = 0;
    TBSGroupOperationTimeTracker BSGroupOperationTimeTracker;

    std::unique_ptr<TIOCompanionClient> IOCompanionClient;
    std::unique_ptr<TIOCompanion> IOCompanion;

public:
    TFreshBlocksWriterActor(
        TStorageConfigPtr config,
        NProto::TPartitionConfig partitionConfig,
        EStorageAccessMode storageAccessMode,
        ui32 partitionIndex,
        ui32 siblingCount,
        NActors::TActorId partitionActorId,
        NActors::TActorId volumeActorId,
        TDiagnosticsConfigPtr diagnosticsConfig,
        ui64 partitionTabletId);

    ~TFreshBlocksWriterActor() override;

    void Bootstrap(const NActors::TActorContext& ctx);

private:

    void Suicide(const NActors::TActorContext ctx)
    {
        NCloud::Send<NActors::TEvents::TEvPoisonPill>(ctx, ctx.SelfID);
    }

    void ScheduleYellowStateUpdate(const NActors::TActorContext& ctx);

    void UpdateYellowState(const NActors::TActorContext& ctx);

    void ReassignChannelsIfNeeded(const NActors::TActorContext& ctx);

    void UpdateChannelPermissions(
        const NActors::TActorContext& ctx,
        ui32 channel,
        EChannelPermissions permissions);

    // IMortalActor overrides

    void Poison(const NActors::TActorContext& ctx) override
    {
        CancelPendingRequests(ctx, PendingRequests);

        Die(ctx);
    }

private:
    STFUNC(StateWaitPartition);
    STFUNC(StateFreshBlobsLoading);
    STFUNC(StateWork);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePartitionReady(
        const NPartition::TEvPartition::TEvWaitReadyResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFreshChannelsInfo(
        const TEvPartitionCommonPrivate::TEvGetFreshChannelsInfoResponse::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);
    bool RejectRequests(STFUNC_SIG);

    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocks,               TEvService)
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks,              TEvService)
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks,               TEvService)
    BLOCKSTORE_IMPLEMENT_REQUEST(CreateCheckpoint,         TEvService)
    BLOCKSTORE_IMPLEMENT_REQUEST(DeleteCheckpoint,         TEvService)
    BLOCKSTORE_IMPLEMENT_REQUEST(GetChangedBlocks,         TEvService)
    BLOCKSTORE_IMPLEMENT_REQUEST(GetCheckpointStatus,      TEvService)
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocksLocal,          TEvService)
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal,         TEvService)


    BLOCKSTORE_IMPLEMENT_REQUEST(DescribeBlocks,           TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(GetUsedBlocks,            TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(GetPartitionInfo,         TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(CompactRange,             TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(GetCompactionStatus,      TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(DeleteCheckpointData,     TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(RebuildMetadata,          TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(GetRebuildMetadataStatus, TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(ScanDisk,                 TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(GetScanDiskStatus,        TEvVolume)
    BLOCKSTORE_IMPLEMENT_REQUEST(CheckRange,               TEvVolume)

    BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(
        BLOCKSTORE_IMPLEMENT_REQUEST,
        TEvFreshBlocksWriter);
};

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
