#include "fresh_blocks_writer_actor.h"

#include "fresh_blocks_companion_client.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

using namespace NActors;

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

#define BLOCKSTORE_SERVICE_FWD_TO_PARTITION_REQUESTS(xxx, ...) \
    xxx(ReadBlocks, __VA_ARGS__)                               \
    xxx(WriteBlocks, __VA_ARGS__)                              \
    xxx(ZeroBlocks, __VA_ARGS__)                               \
    xxx(CreateCheckpoint, __VA_ARGS__)                         \
    xxx(DeleteCheckpoint, __VA_ARGS__)                         \
    xxx(GetChangedBlocks, __VA_ARGS__)                         \
    xxx(GetCheckpointStatus, __VA_ARGS__)                      \
    xxx(ReadBlocksLocal, __VA_ARGS__)                          \
    xxx(WriteBlocksLocal, __VA_ARGS__)                         \
    // BLOCKSTORE_SERVICE_FWD_TO_PARTITION_REQUESTS

#define BLOCKSTORE_VOLUME_FWD_TO_PARTITION_REQUESTS(xxx, ...) \
    xxx(DescribeBlocks, __VA_ARGS__)                          \
    xxx(GetUsedBlocks, __VA_ARGS__)                           \
    xxx(GetPartitionInfo, __VA_ARGS__)                        \
    xxx(CompactRange, __VA_ARGS__)                            \
    xxx(GetCompactionStatus, __VA_ARGS__)                     \
    xxx(DeleteCheckpointData, __VA_ARGS__)                    \
    xxx(RebuildMetadata, __VA_ARGS__)                         \
    xxx(GetRebuildMetadataStatus, __VA_ARGS__)                \
    xxx(ScanDisk, __VA_ARGS__)                                \
    xxx(GetScanDiskStatus, __VA_ARGS__)                       \
    xxx(CheckRange, __VA_ARGS__)                              \
    // BLOCKSTORE_VOLUME_FWD_TO_PARTITION_REQUESTS

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFreshBlocksWriterActor::TFreshBlocksWriterActor(
        TStorageConfigPtr config,
        NProto::TPartitionConfig partitionConfig,
        EStorageAccessMode storageAccessMode,
        ui32 partitionIndex,
        ui32 siblingCount,
        NActors::TActorId partitionActorId,
        ui64 partitionTabletId)
    : Config(std::move(config))
    , PartitionConfig(std::move(partitionConfig))
    , StorageAccessMode(storageAccessMode)
    , PartitionTabletID(partitionTabletId)
    , PartitionActorId(partitionActorId)
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
        std::make_unique<NPartition::TEvPartition::TEvWaitReadyRequest>();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Waiting for partition ready",
        LogTitle.GetWithTime().c_str());

    NCloud::Send(ctx, PartitionActorId, std::move(request));
}

void TFreshBlocksWriterActor::FreshBlobsLoaded(
    const NActors::TActorContext& ctx)
{
    IsFreshBlobsLoaded = true;
    SendPendingRequests(ctx, PendingRequests);
    Become(&TThis::StateWork);
}

void TFreshBlocksWriterActor::KillActors(const NActors::TActorContext& ctx)
{
    if (FreshBlocksCompanion) {
        FreshBlocksCompanion->KillActors(ctx);
    }
}

void TFreshBlocksWriterActor::HandlePartitionReady(
    const NPartition::TEvPartition::TEvWaitReadyResponse::TPtr& ev,
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

    CommitIdsState =
        std::make_unique<TCommitIdsState>(TabletGeneration, /*lastCommitId=*/0);
    FreshBlobState = std::make_unique<TPartitionFreshBlobState>();
    FlushState = std::make_unique<TPartitionFlushState>();
    TrimFreshLogState =
        std::make_unique<TPartitionTrimFreshLogState>(*CommitIdsState);
    FreshBlocksState = std::make_unique<TPartitionFreshBlocksState>(
        *CommitIdsState,
        *FlushState,
        *TrimFreshLogState);

    FreshBlocksCompanionClient =
        std::make_unique<TFreshBlocksCompanionClient>(*this);

    FreshBlocksCompanion = std::make_unique<TFreshBlocksCompanion>(
        StorageAccessMode,
        PartitionConfig,
        TabletStorageInfo.Get(),
        *FreshBlocksCompanionClient,
        *ChannelsState,
        *FreshBlobState,
        *FlushState,
        *TrimFreshLogState,
        *FreshBlocksState,
        LogTitle);

    FreshBlocksCompanion->LoadFreshBlobs(
        ctx,
        msg->PersistedTrimFreshLogToCommitId);

    Become(&TThis::StateFreshBlobsLoading);
}

void TFreshBlocksWriterActor::HandleWaitReady(
    const TEvFreshBlocksWriter::TEvWaitReadyRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!IsFreshBlobsLoaded) {
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

        HFunc(
            NPartition::TEvPartition::TEvWaitReadyResponse,
            HandlePartitionReady);

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

STFUNC(TFreshBlocksWriterActor::StateFreshBlobsLoading)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, PoisonPillHelper.HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        HFunc(TEvFreshBlocksWriter::TEvWaitReadyRequest, HandleWaitReady);

        HFunc(
            TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted,
            FreshBlocksCompanion->HandleLoadFreshBlobsCompleted);

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

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
