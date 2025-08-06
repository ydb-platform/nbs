#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

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
        case NCloud::NProto::STORAGE_MEDIA_SSD:
            return config.GetMaxIORequestsInFlightSSD();
        default:
            return config.GetMaxIORequestsInFlight();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadState& args)
{
    Y_UNUSED(ctx);

    // TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Reading state from local db");

    bool ready = db.ReadMeta(args.Meta);
    ready &= db.ReadGlobalBlobs(args.Blobs);
    ready &= db.ReadGlobalBlobUpdates(args.BlobUpdates);
    ready &= db.ReadGlobalBlobGarbage(args.BlobGarbage);
    ready &= db.ReadCompactionMap(args.CompactionMap);
    ready &= db.ReadGarbageBlobs(args.GarbageBlobs);
    ready &= db.ReadFreshBlockUpdates(args.FreshBlockUpdates);

    if (!ready) {
        return false;
    }

    if (args.Meta.Defined()) {
        ready &= db.ReadAllZoneBlobIds(
            args.ZoneBlobIds,
            args.Meta->GetLastCollectCommitId()
        );
    }

    if (!db.ReadCheckpoints(args.Checkpoints)) {
        return false;
    }

    for (const auto& meta: args.Checkpoints) {
        if (meta.GetDateDeleted()) {
            ready &= db.ReadCheckpointBlobs(
                meta.GetCommitId(),
                args.DeletedCheckpointBlobIds.emplace_back()
            );
        }
    }

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " State read");

    return ready;
}

void TPartitionActor::ExecuteLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadState& args)
{
    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] State data loaded",
        TabletID());

    // TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    if (!args.Meta) {
        // initialize with empty meta
        args.Meta.ConstructInPlace();
    }

    // override config
    args.Meta->MutableConfig()->CopyFrom(PartitionConfig);

    db.WriteMeta(*args.Meta);
}

void TPartitionActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxPartition::TLoadState& args)
{
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

    TIndexCachingConfig indexCachingConfig {
        Config->GetHotZoneRequestCountFactor(),
        Config->GetColdZoneRequestCountFactor(),
        Config->GetBlockListCacheSizePercentage() / 100.,
    };

    auto& config = *args.Meta->MutableConfig();
    if (!config.GetZoneBlockCount()) {
        // XXX for partitions created before NBS-1524
        config.SetZoneBlockCount(32 * MaxBlocksCount);
    }

    ui32 tabletChannelCount = Info()->Channels.size();
    ui32 configChannelCount = args.Meta->GetConfig().ExplicitChannelProfilesSize();

    if (tabletChannelCount != configChannelCount) {
        ReportInvalidTabletConfig(
            TStringBuilder()
            << "[" << TabletID() << "] "
            << "tablet info differs from config: "
            << "tabletChannelCount != configChannelCount ("
            << tabletChannelCount << " != " << configChannelCount << ")");

        // FIXME(NBS-2088): do suicide
        // Suicide(ctx);
        // return;
    }

    State = std::make_unique<TPartitionState>(
        *args.Meta,
        TabletID(),
        Executor()->Generation(),
        Min(tabletChannelCount, configChannelCount),  // channelCount
        Config->GetMaxBlobSize(),
        Config->GetMaxRangesPerBlob(),
        Config->GetOptimizeForShortRanges()
            ? EOptimizationMode::OptimizeForShortRanges
            : EOptimizationMode::OptimizeForLongRanges,
        BuildCompactionPolicy(args.Meta->GetConfig(), *Config, SiblingCount),
        bpConfig,
        fsConfig,
        indexCachingConfig,
        GetMaxIORequestsInFlight(*Config, PartitionConfig),
        Config->GetReassignChannelsPercentageThreshold(),
        Config->GetReassignMixedChannelsPercentageThreshold()
    );

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Initializing State object");


    State->InitCheckpoints(args.Checkpoints, args.DeletedCheckpointBlobIds);
    State->SetFreshBlockUpdates(std::move(args.FreshBlockUpdates));

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " UpdateIndex begin");
    State->UpdateIndex(args.Blobs, args.BlobUpdates, args.BlobGarbage);
    State->GetBlobs().FinishGlobalDataInitialization();
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " UpdateIndex end");
    State->InitCompactionMap(args.CompactionMap);

    {
        TVector<TPartialBlobId> knownBlobIds = std::move(args.ZoneBlobIds);
        for (const auto& blob: args.Blobs) {
            const auto& blobId = blob.BlobId;
            if (!IsDeletionMarker(blobId)
                    && blobId.CommitId() >= args.Meta->GetLastCollectCommitId())
            {
                knownBlobIds.push_back(blobId);
            }
        }
        State->InitGarbage(
            knownBlobIds,
            args.GarbageBlobs
        );
    }

    auto totalBlockCount = State->GetFreshBlockCount()
        + State->GetMergedBlockCount();
    UpdateStorageStats(ctx, totalBlockCount * State->GetBlockSize());

    LoadFreshBlobs(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
