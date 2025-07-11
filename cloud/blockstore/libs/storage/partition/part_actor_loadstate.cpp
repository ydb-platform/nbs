#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GetLastCollectCommitId(const TMaybe<NProto::TPartitionMeta>& meta)
{
    return meta ? meta->GetLastCollectCommitId() : 0;
}

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::SendGetUsedBlocksFromBaseDisk(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvGetUsedBlocksRequest>();

    request->Record.SetDiskId(State->GetBaseDiskId());

    auto event = std::make_unique<IEventHandle>(
        MakeVolumeProxyServiceId(),
        SelfId(),
        request.release());

    ctx.Send(event.release());
}

bool TPartitionActor::PrepareLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadState& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Reading state from local db",
        LogTitle.GetWithTime().c_str());

    // TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    const ui32 maxRangesPerTx = Config->GetMaxCompactionRangesLoadingPerTx();
    const bool shouldLoadCompactionMapLazily = maxRangesPerTx != 0;

    if (shouldLoadCompactionMapLazily) {
        CompactionMapLoadState = std::make_unique<TCompactionMapLoadState>(
            maxRangesPerTx,
            Config->GetMaxOutOfOrderCompactionMapChunksInflight());
    }

    std::initializer_list<bool> results = {
        db.ReadMeta(args.Meta),
        db.ReadFreshBlocks(args.FreshBlocks),
        shouldLoadCompactionMapLazily ? true : db.ReadCompactionMap(args.CompactionMap),
        db.ReadUsedBlocks(args.UsedBlocks),
        db.ReadLogicalUsedBlocks(args.LogicalUsedBlocks, args.ReadLogicalUsedBlocks),
        db.ReadCheckpoints(args.Checkpoints, args.CheckpointId2CommitId),
        db.ReadCleanupQueue(args.CleanupQueue),
        db.ReadGarbageBlobs(args.GarbageBlobs),
        db.ReadUnconfirmedBlobs(args.UnconfirmedBlobs),
    };

    bool ready = std::accumulate(
        results.begin(),
        results.end(),
        true,
        std::logical_and<>()
    );

    if (ready) {
        ready &= db.ReadNewBlobs(args.NewBlobs, GetLastCollectCommitId(args.Meta));
    }

    return ready;
}

void TPartitionActor::ExecuteLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadState& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s State data loaded",
        LogTitle.GetWithTime().c_str());

    // TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    if (!args.Meta) {
        // initialize with empty meta
        args.Meta = NProto::TPartitionMeta();
    }

    // override config
    args.Meta->MutableConfig()->CopyFrom(PartitionConfig);

    db.WriteMeta(*args.Meta);
}

void TPartitionActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxPartition::TLoadState& args)
{
    const auto& partitionConfig = args.Meta->GetConfig();

    // initialize state
    TBackpressureFeaturesConfig bpConfig {
        {
            Config->GetCompactionScoreLimitForBackpressure(),
            Config->GetCompactionScoreThresholdForBackpressure(),
            static_cast<double>(Config->GetCompactionScoreFeatureMaxValue()),
        },
        {
            Config->GetFreshByteCountLimitForBackpressure(),
            Config->GetFreshByteCountThresholdForBackpressure(),
            static_cast<double>(Config->GetFreshByteCountFeatureMaxValue()),
        },
        {
            Config->GetCleanupQueueBytesLimitForBackpressure(),
            Config->GetCleanupQueueBytesThresholdForBackpressure(),
            static_cast<double>(Config->GetCleanupQueueBytesFeatureMaxValue()),
        },
    };

    TFreeSpaceConfig fsConfig {
        Config->GetChannelFreeSpaceThreshold() / 100.,
        Config->GetChannelMinFreeSpace() / 100.,
    };

    ui32 tabletChannelCount = Info()->Channels.size();
    ui32 configChannelCount = partitionConfig.ExplicitChannelProfilesSize();

    if (tabletChannelCount < configChannelCount) {
        // either a race or a bug (if this situation occurs again after tablet restart)
        // example: CLOUDINC-2027
        ReportInvalidTabletConfig(
            TStringBuilder()
            << "DiskId" << partitionConfig.diskid()
            << ";tablet info differs from config: tabletChannelCount < "
               "configChannelCount ("
            << tabletChannelCount << " < " << configChannelCount << ")");

        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s tablet info differs from config: tabletChannelCount < "
            "configChannelCount (%u < %u)",
            LogTitle.GetWithTime().c_str(),
            tabletChannelCount,
            configChannelCount);
    } else if (tabletChannelCount > configChannelCount) {
        // legacy channel configuration
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s tablet info differs from config: tabletChannelCount > "
            "configChannelCount (%u > %u)",
            LogTitle.GetWithTime().c_str(),
            tabletChannelCount,
            configChannelCount);
    }

    const ui32 mixedIndexCacheSize = [&] {
        if (!Config->GetMixedIndexCacheV1Enabled() &&
            !Config->IsMixedIndexCacheV1FeatureEnabled(
                partitionConfig.GetCloudId(),
                partitionConfig.GetFolderId(),
                partitionConfig.GetDiskId()))
        {
            // disabled by default & not enabled for cloud
            return 0u;
        }

        const auto kind = partitionConfig.GetStorageMediaKind();
        if (kind == NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD) {
            return Config->GetMixedIndexCacheV1SizeSSD();
        }

        return 0u;
    }();

    const auto mediaKind = partitionConfig.GetStorageMediaKind();
    auto maxBlobsPerUnit = mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD ?
        Config->GetSSDMaxBlobsPerUnit() :
        Config->GetHDDMaxBlobsPerUnit();
    auto maxBlobsPerRange = mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD ?
        Config->GetSSDMaxBlobsPerRange() :
        Config->GetHDDMaxBlobsPerRange();

    State = std::make_unique<TPartitionState>(
        *args.Meta,
        Executor()->Generation(),
        BuildCompactionPolicy(partitionConfig, *Config, SiblingCount),
        Config->GetCompactionScoreHistorySize(),
        Config->GetCleanupScoreHistorySize(),
        bpConfig,
        fsConfig,
        GetMaxIORequestsInFlight(*Config, PartitionConfig),
        Config->GetReassignChannelsPercentageThreshold(),
        0,  // lastCommitId
        Min(tabletChannelCount, configChannelCount),  // channelCount
        mixedIndexCacheSize,
        GetAllocationUnit(*Config, mediaKind),
        maxBlobsPerUnit,
        maxBlobsPerRange,
        Config->GetCompactionRangeCountPerRun());

    MapBaseDiskIdToTabletId(ctx);

    State->InitFreshBlocks(args.FreshBlocks);
    State->GetUsedBlocks() = std::move(args.UsedBlocks);
    State->AccessStats().SetUsedBlocksCount(State->GetUsedBlocks().Count());

    if (CompactionMapLoadState) {
        LoadNextCompactionMapChunk(ctx);
    } else {
        State->GetCompactionMap().Update(
            args.CompactionMap,
            &State->GetUsedBlocks());
    }

    State->GetCheckpoints().Add(args.Checkpoints);
    State->GetCheckpoints().SetCheckpointMappings(args.CheckpointId2CommitId);
    State->GetCleanupQueue().Add(args.CleanupQueue);
    Y_ABORT_UNLESS(State->GetGarbageQueue().AddNewBlobs(args.NewBlobs));
    Y_ABORT_UNLESS(State->GetGarbageQueue().AddGarbageBlobs(args.GarbageBlobs));

    // Logical used blocks calculation is not implemented for proxy overlay
    // disks with multi-partition base disks, so we disable it.
    // Proxy overlay disks are system disks that are used for snapshot creation
    // or disk relocation.
    // Also, logical used blocks calculation is not needed for such disks
    // because they are temporary and do not belong to a user.
    if (State->GetBaseDiskId() && !partitionConfig.GetIsSystem()) {
        if (args.ReadLogicalUsedBlocks) {
            State->GetLogicalUsedBlocks() = std::move(args.LogicalUsedBlocks);
            State->AccessStats().SetLogicalUsedBlocksCount(
                State->GetLogicalUsedBlocks().Count()
            );
        } else {
            State->GetLogicalUsedBlocks().Update(State->GetUsedBlocks(), 0);
            State->AccessStats().SetLogicalUsedBlocksCount(
                State->AccessStats().GetUsedBlocksCount()
            );

            SendGetUsedBlocksFromBaseDisk(ctx);
            return;
        }
    } else {
        State->AccessStats().SetLogicalUsedBlocksCount(
            State->AccessStats().GetUsedBlocksCount()
        );
    }

    State->InitUnconfirmedBlobs(std::move(args.UnconfirmedBlobs));

    FinalizeLoadState(ctx);
}

void TPartitionActor::FinalizeLoadState(const TActorContext& ctx)
{
    auto totalBlocksCount = State->GetMixedBlocksCount() + State->GetMergedBlocksCount();
    UpdateStorageStat(totalBlocksCount * State->GetBlockSize());

    LoadFreshBlobs(ctx);
}

void TPartitionActor::FreshBlobsLoaded(const TActorContext& ctx)
{
    ConfirmBlobs(ctx);
}

void TPartitionActor::BlobsConfirmed(const TActorContext& ctx)
{
    Activate(ctx);
}

void TPartitionActor::HandleGetUsedBlocksResponse(
    const TEvVolume::TEvGetUsedBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s LoadState failed: GetUsedBlocks from base disk failed. error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(msg->GetError()).c_str());
        Suicide(ctx);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s LoadState completed",
        LogTitle.GetWithTime().c_str());

    for (const auto& block: msg->Record.GetUsedBlocks()) {
        State->GetLogicalUsedBlocks().Merge(
            TCompressedBitmap::TSerializedChunk{
                block.GetChunkIdx(),
                block.GetData()
            });
    }

    State->AccessStats().SetLogicalUsedBlocksCount(
        State->GetLogicalUsedBlocks().Count()
    );

    ExecuteTx(ctx, CreateTx<TUpdateLogicalUsedBlocks>(0));
}

bool TPartitionActor::PrepareUpdateLogicalUsedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateLogicalUsedBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteUpdateLogicalUsedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateLogicalUsedBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TPartitionDatabase db(tx.DB);

    args.UpdatedToIdx = Min(
        args.UpdateFromIdx + Config->GetLogicalUsedBlocksUpdateBlockCount(),
        State->GetLogicalUsedBlocks().Capacity());

    auto serializer = State->GetLogicalUsedBlocks().RangeSerializer(
        args.UpdateFromIdx, args.UpdatedToIdx
    );
    TCompressedBitmap::TSerializedChunk sc;
    while (serializer.Next(&sc)) {
        if (!TCompressedBitmap::IsZeroChunk(sc)) {
            db.WriteLogicalUsedBlocks(sc);
        }
    }
}

void TPartitionActor::CompleteUpdateLogicalUsedBlocks(
    const TActorContext& ctx,
    TTxPartition::TUpdateLogicalUsedBlocks& args)
{
    if (args.UpdatedToIdx == State->GetLogicalUsedBlocks().Capacity()) {
        FinalizeLoadState(ctx);
    } else {
        ExecuteTx(ctx, CreateTx<TUpdateLogicalUsedBlocks>(args.UpdatedToIdx));
    }
}

bool TPartitionActor::PrepareLoadCompactionMapChunk(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadCompactionMapChunk& args)
{
    TPartitionDatabase db(tx.DB);
    args.Counters.reserve(args.Counters.size() + args.Range.Size());
    const bool result = db.ReadCompactionMap(
        TBlockRange32::WithLength(
            args.Range.Start * State->GetCompactionMap().GetRangeSize(),
            args.Range.Size() * State->GetCompactionMap().GetRangeSize()),
        args.Counters);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Compaction map chunk %s read, result=%d",
        LogTitle.GetWithTime().c_str(),
        args.Range.Print().c_str(),
        result);

    return result;
}

void TPartitionActor::ExecuteLoadCompactionMapChunk(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadCompactionMapChunk& args)
{
    Y_UNUSED(ctx, tx, args);
}

void TPartitionActor::CompleteLoadCompactionMapChunk(
    const TActorContext& ctx,
    TTxPartition::TLoadCompactionMapChunk& args)
{
    State->GetCompactionMap().Update(args.Counters, &State->GetUsedBlocks());

    if (args.Counters.empty()) {
        CompactionMapLoadState.reset();

        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Compaction map loaded",
            LogTitle.GetWithTime().c_str());

        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Compaction map chunk %s updated",
        LogTitle.GetWithTime().c_str(),
        args.Range.Print().c_str());

    CompactionMapLoadState->OnRangeLoaded(args.Range);
    LoadNextCompactionMapChunk(ctx);
}

void TPartitionActor::LoadNextCompactionMapChunk(
    const NActors::TActorContext& ctx)
{
    TBlockRange32 range = CompactionMapLoadState->LoadNextChunk();
    auto request =
        std::make_unique<TEvPartitionPrivate::TEvLoadCompactionMapChunkRequest>(
            range);
    NCloud::Send(ctx, SelfId(), std::move(request));
}

void TPartitionActor::HandleLoadCompactionMapChunk(
    const TEvPartitionPrivate::TEvLoadCompactionMapChunkRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Loading compaction map chunk %s",
        LogTitle.GetWithTime().c_str(),
        ev->Get()->Range.Print().c_str());

    ExecuteTx<TLoadCompactionMapChunk>(ctx, ev->Get()->Range);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
