#include "fresh_blocks_writer_actor.h"

#include "io_companion_client.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition/part_counters.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

using namespace NActors;

using namespace NPartition;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 GetMaxIORequestsInFlight(
    const TStorageConfig& config,
    const NProto::TPartitionConfig& partitionConfig)
{
    if (GetThrottlingEnabled(config, partitionConfig)) {
        return Max<ui32>();
    }

    switch (partitionConfig.GetStorageMediaKind()) {
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD:
            return config.GetMaxIORequestsInFlightSSD();
        default:
            return config.GetMaxIORequestsInFlight();
    }
}

#define BLOCKSTORE_SERVICE_FWD_TO_PARTITION_REQUESTS(xxx, ...)                 \
    xxx(ReadBlocks,                  __VA_ARGS__)                              \
    xxx(WriteBlocks,                 __VA_ARGS__)                              \
    xxx(ZeroBlocks,                  __VA_ARGS__)                              \
    xxx(CreateCheckpoint,            __VA_ARGS__)                              \
    xxx(DeleteCheckpoint,            __VA_ARGS__)                              \
    xxx(GetChangedBlocks,            __VA_ARGS__)                              \
    xxx(GetCheckpointStatus,         __VA_ARGS__)                              \
    xxx(ReadBlocksLocal,             __VA_ARGS__)                              \
    xxx(WriteBlocksLocal,            __VA_ARGS__)                              \
// BLOCKSTORE_SERVICE_FWD_TO_PARTITION_REQUESTS

#define BLOCKSTORE_VOLUME_FWD_TO_PARTITION_REQUESTS(xxx, ...)                  \
    xxx(DescribeBlocks,              __VA_ARGS__)                              \
    xxx(GetUsedBlocks,               __VA_ARGS__)                              \
    xxx(GetPartitionInfo,            __VA_ARGS__)                              \
    xxx(CompactRange,                __VA_ARGS__)                              \
    xxx(GetCompactionStatus,         __VA_ARGS__)                              \
    xxx(DeleteCheckpointData,        __VA_ARGS__)                              \
    xxx(RebuildMetadata,             __VA_ARGS__)                              \
    xxx(GetRebuildMetadataStatus,    __VA_ARGS__)                              \
    xxx(ScanDisk,                    __VA_ARGS__)                              \
    xxx(GetScanDiskStatus,           __VA_ARGS__)                              \
    xxx(CheckRange,                  __VA_ARGS__)                              \
// BLOCKSTORE_VOLUME_FWD_TO_PARTITION_REQUESTS

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFreshBlocksWriterActor::TFreshBlocksWriterActor(
        TStorageConfigPtr config,
        NProto::TPartitionConfig partitionConfig,
        EStorageAccessMode storageAccessMode,
        ui64 partitionTabletId,
        ui32 partitionIndex,
        ui32 siblingCount,
        NActors::TActorId partitionActorId,
        NActors::TActorId volumeActorId,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        IProfileLogPtr profileLog)
    : Config(std::move(config))
    , PartitionConfig(std::move(partitionConfig))
    , StorageAccessMode(storageAccessMode)
    , PartitionTabletID(partitionTabletId)
    , PartitionActorId(partitionActorId)
    , BlobCodec(NBlockCodecs::Codec(Config->GetBlobCompressionCodec()))
    , VolumeActorId(volumeActorId)
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , ProfileLog(std::move(profileLog))
    , PoisonPillHelper(this)
    , LogTitle(
          GetCycleCount(),
          TLogTitle::TFreshBlocksWriter{
              .TabletId = partitionTabletId,
              .DiskId = PartitionConfig.GetDiskId(),
              .PartitionIndex = partitionIndex,
              .PartitionCount = siblingCount,
              .Generation = 0})
{}

TFreshBlocksWriterActor::~TFreshBlocksWriterActor() = default;

void TFreshBlocksWriterActor::Bootstrap(const NActors::TActorContext& ctx)
{
    Become(&TThis::StateWaitPartition);

    PoisonPillHelper.TakeOwnership(ctx, PartitionActorId);

    auto request =
        std::make_unique<TEvPartition::TEvWaitReadyRequest>();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Waiting for partition ready",
        LogTitle.GetWithTime().c_str());

    NCloud::Send(ctx, PartitionActorId, std::move(request));
}

void TFreshBlocksWriterActor::ScheduleYellowStateUpdate(
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Y_ABORT("Unimplemented");
}

void TFreshBlocksWriterActor::UpdateYellowState(
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Y_ABORT("Unimplemented");
}

void TFreshBlocksWriterActor::ReassignChannelsIfNeeded(
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Y_ABORT("Unimplemented");
}

void TFreshBlocksWriterActor::UpdateChannelPermissions(
    const NActors::TActorContext& ctx,
    ui32 channel,
    EChannelPermissions permissions)
{
    Y_UNUSED(ctx);

    ChannelsState->UpdatePermissions(channel, permissions);
    //TODO(issue-4875): add partition notification.
}

void TFreshBlocksWriterActor::Poison(const NActors::TActorContext& ctx)
{
    KillActors(ctx);

    CancelPendingRequests(ctx, PendingRequests);

    Die(ctx);
}

void TFreshBlocksWriterActor::KillActors(const NActors::TActorContext& ctx)
{
    for (const auto& actor: Actors.GetActors()) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

bool TFreshBlocksWriterActor::InitReadWriteBlockRange(
    ui64 blockIndex,
    ui32 blockCount,
    TBlockRange64* range) const
{
    if (!blockCount) {
        return false;
    }

    *range = TBlockRange64::WithLength(blockIndex, blockCount);

    return CheckBlockRange(*range) &&
           (range->Size() <= Config->GetMaxReadWriteRangeSize() /
                                 PartitionConfig.GetBlockSize());
}

bool TFreshBlocksWriterActor::CheckBlockRange(const TBlockRange64& range) const
{
    Y_DEBUG_ABORT_UNLESS(PartitionConfig.GetBlocksCount() <= Max<ui32>());
    const auto validRange =
        TBlockRange64::WithLength(0, PartitionConfig.GetBlocksCount());
    return validRange.Contains(range);
}

void TFreshBlocksWriterActor::RebootOnCommitIdOverflow(
    const TActorContext& ctx,
    const TStringBuf& requestName)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s CommitId overflow in %s. Restarting partition",
        LogTitle.GetWithTime().c_str(),
        ToString(requestName).c_str());
    ReportTabletCommitIdOverflow({{"disk", PartitionConfig.GetDiskId()}});
    Suicide(ctx);
}

void TFreshBlocksWriterActor::UpdateStats(const NProto::TPartitionStats& update)
{
    PartStats->Access([&](NProto::TPartitionStats& stats)
                      { UpdatePartitionCounters(stats, update); });

    auto blockSize = PartitionConfig.GetBlockSize();

    PartCounters->Access(
        [&](auto& partCounters)
        {
            partCounters->Cumulative.BytesWritten.Increment(
                update.GetUserWriteCounters().GetBlocksCount() * blockSize);

            partCounters->Cumulative.BytesRead.Increment(
                update.GetUserReadCounters().GetBlocksCount() * blockSize);

            partCounters->Cumulative.SysBytesWritten.Increment(
                update.GetSysWriteCounters().GetBlocksCount() * blockSize);

            partCounters->Cumulative.SysBytesRead.Increment(
                update.GetSysReadCounters().GetBlocksCount() * blockSize);

            partCounters->Cumulative.RealSysBytesWritten.Increment(
                update.GetRealSysWriteCounters().GetBlocksCount() * blockSize);

            partCounters->Cumulative.RealSysBytesRead.Increment(
                update.GetRealSysReadCounters().GetBlocksCount() * blockSize);

            partCounters->Cumulative.BatchCount.Increment(
                update.GetUserWriteCounters().GetBatchCount());
        });
}

void TFreshBlocksWriterActor::HandlePartitionReady(
    const TEvPartition::TEvWaitReadyResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    NCloud::Send(
        ctx,
        PartitionActorId,
        std::make_unique<
            TEvPartitionCommonPrivate::TEvGetFreshChannelsInfoRequest>());
}

void TFreshBlocksWriterActor::HandleFreshChannelsInfo(
    const TEvPartitionCommonPrivate::TEvGetFreshChannelsInfoResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    TabletStorageInfo = msg->TabletInfo;
    TabletGeneration = msg->Generation;

    LogTitle.SetGeneration(TabletGeneration);

    ui32 tabletChannelCount = TabletStorageInfo->Channels.size();
    ui32 configChannelCount = PartitionConfig.ExplicitChannelProfilesSize();

    TFreeSpaceConfig fsConfig{
        Config->GetChannelFreeSpaceThreshold() / 100.,
        Config->GetChannelMinFreeSpace() / 100.,
    };

    ChannelsState = std::make_unique<TPartitionChannelsState>(
        PartitionConfig,
        fsConfig,
        GetMaxIORequestsInFlight(*Config, PartitionConfig),
        Config->GetReassignChannelsPercentageThreshold(),
        Config->GetReassignFreshChannelsPercentageThreshold(),
        Config->GetReassignMixedChannelsPercentageThreshold(),
        Config->GetReassignSystemChannelsImmediately(),
        Min(tabletChannelCount, configChannelCount));

    for (ui32 channel = 0; channel < ChannelsState->GetChannelCount();
         ++channel)
    {
        ChannelsState->UpdatePermissions(
            channel,
            msg->ChannelPermissions[channel]);
    }

    CommitIdsState = std::make_unique<TCommitIdsState>(msg->CommitIdGenerator);
    FlushState = std::make_unique<TPartitionFlushState>();
    TrimFreshLogState =
        std::make_unique<TPartitionTrimFreshLogState>(*CommitIdsState);

    IOCompanionClient = std::make_unique<TIOCompanionClient>(*this);

    IOCompanion = std::make_unique<TIOCompanion>(
        Config,
        PartitionConfig,
        TabletStorageInfo,
        PartitionTabletID,
        BlobCodec,
        VolumeActorId,
        DiagnosticsConfig,
        StorageAccessMode,
        BSGroupOperationTimeTracker,
        BSGroupOperationId,
        *IOCompanionClient,
        *ChannelsState,
        LogTitle,
        msg->ResourceMetricsQueue,
        msg->GroupDowntimes,
        msg->PartCounters);
    Become(&TThis::StateWork);

    ResourceMetricsQueue = msg->ResourceMetricsQueue;
    PartCounters = msg->PartCounters;
    PartStats = msg->PartStats;

    StateLoaded = true;
    SendPendingRequests(ctx, PendingRequests);
}

void TFreshBlocksWriterActor::HandleWaitReady(
    const TEvFreshBlocksWriter::TEvWaitReadyRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!StateLoaded) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s WaitReady request delayed until partition is ready",
            LogTitle.GetWithTime().c_str());

        auto requestInfo =
            CreateRequestInfo<TEvFreshBlocksWriter::TWaitReadyMethod>(
                ev->Sender,
                ev->Cookie,
                ev->Get()->CallContext);

        PendingRequests.emplace_back(
            NActors::IEventHandlePtr(ev.Release()),
            requestInfo);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Received WaitReady request",
        LogTitle.GetWithTime().c_str());

    auto response =
        std::make_unique<TEvFreshBlocksWriter::TEvWaitReadyResponse>();

    NCloud::Reply(ctx, *ev, std::move(response));
}

bool TFreshBlocksWriterActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_SERVICE_FWD_TO_PARTITION_REQUESTS(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvService)
        BLOCKSTORE_VOLUME_FWD_TO_PARTITION_REQUESTS(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvVolume)

        BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvFreshBlocksWriter);

        default:
            return false;
    }

    return true;
}

bool TFreshBlocksWriterActor::RejectRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_SERVICE_FWD_TO_PARTITION_REQUESTS(
            BLOCKSTORE_REJECT_REQUEST,
            TEvService)
        BLOCKSTORE_VOLUME_FWD_TO_PARTITION_REQUESTS(
            BLOCKSTORE_REJECT_REQUEST,
            TEvVolume)

        BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(
            BLOCKSTORE_REJECT_REQUEST,
            TEvFreshBlocksWriter);

        default:
            return false;
    }

    return true;
}

STFUNC(TFreshBlocksWriterActor::StateWaitPartition)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, PoisonPillHelper.HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        HFunc(TEvFreshBlocksWriter::TEvWaitReadyRequest, HandleWaitReady);

        HFunc(TEvPartition::TEvWaitReadyResponse, HandlePartitionReady);

        HFunc(
            TEvPartitionCommonPrivate::TEvGetFreshChannelsInfoResponse,
            HandleFreshChannelsInfo);

        default:
            if (!RejectRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TFreshBlocksWriterActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, PoisonPillHelper.HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        HFunc(
            TEvPartitionCommonPrivate::TEvWriteFreshBlocksCompleted,
            HandleWriteBlocksCompleted);

        default:
            if (!IOCompanion->HandleRequests(ev, this->ActorContext()) &&
                !HandleRequests(ev))
            {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
