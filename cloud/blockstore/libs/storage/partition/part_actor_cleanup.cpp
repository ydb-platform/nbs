#include "part_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueCleanupIfNeeded(const TActorContext& ctx)
{
    if (State->GetCleanupState().Status != EOperationStatus::Idle) {
        // already enqueued
        return;
    }

    if (State->IsMetadataRebuildStarted() &&
        State->GetMetadataRebuildType() == EMetadataRebuildType::BlockCount)
    {
        return;
    }

    if (State->IsScanDiskStarted()) {
        return;
    }

    auto& scoreHistory = State->GetCleanupScoreHistory();
    const auto now = ctx.Now();
    if (scoreHistory.LastTs() + Config->GetMaxCleanupDelay() <= now) {
        scoreHistory.Register({
            now,
            static_cast<ui32>(State->GetCleanupQueue().GetQueueBytes() / 1_MB)
        });
    }

    ui64 commitId = State->GetCleanupCommitId();

    ui32 pendingBlobs = State->GetBlobCountToCleanup(
        commitId,
        Config->GetCleanupThreshold()
    );

    if (pendingBlobs < Config->GetCleanupThreshold()) {
        // not ready
        return;
    }

    State->GetCleanupState().SetStatus(EOperationStatus::Enqueued);

    auto request = std::make_unique<TEvPartitionPrivate::TEvCleanupRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()));

    const auto throttlingAllowed = State->GetCleanupQueue().GetQueueBytes()
        < Config->GetCleanupQueueBytesLimitForThrottling();

    if (throttlingAllowed && Config->GetMaxCleanupDelay()) {
        // TODO: unify this code and compaction delay-related code
        auto execTime = State->GetCleanupExecTimeForLastSecond(ctx.Now());
        auto delay = Config->GetMinCleanupDelay();
        if (Config->GetMaxCleanupExecTimePerSecond()) {
            auto throttlingFactor = double(execTime.GetValue())
                / Config->GetMaxCleanupExecTimePerSecond().GetValue();
            const auto throttleDelay = (TDuration::Seconds(1) - execTime) * throttlingFactor;

            delay = Max(delay, throttleDelay);
        }

        delay = Min(delay, Config->GetMaxCleanupDelay());
        State->SetCleanupDelay(delay);
    } else {
        State->SetCleanupDelay({});
    }

    if (State->GetCleanupDelay()) {
        ctx.Schedule(State->GetCleanupDelay(), request.release());
    } else {
        NCloud::Send(
            ctx,
            SelfId(),
            std::move(request));
    }
}

void TPartitionActor::HandleCleanup(
    const TEvPartitionPrivate::TEvCleanupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        BackgroundTaskStarted_Partition,
        requestInfo->CallContext->LWOrbit,
        "Cleanup",
        static_cast<ui32>(PartitionConfig.GetStorageMediaKind()),
        requestInfo->CallContext->RequestId,
        PartitionConfig.GetDiskId());

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvPartitionPrivate::TEvCleanupResponse>(
            MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "Cleanup",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetCleanupState().Status == EOperationStatus::Started) {
        replyError(ctx, *requestInfo, E_TRY_AGAIN, "cleanup already started");
        return;
    }

    if (State->IsMetadataRebuildStarted() &&
        State->GetMetadataRebuildType() == EMetadataRebuildType::BlockCount)
    {
        State->GetCleanupState().SetStatus(EOperationStatus::Idle);

        replyError(ctx, *requestInfo, E_TRY_AGAIN, "Metadata rebuild is running");
        return;
    }

    if (State->IsScanDiskStarted()) {
        State->GetCleanupState().SetStatus(EOperationStatus::Idle);

        replyError(ctx, *requestInfo, E_TRY_AGAIN, "Scan disk is running");
        return;
    }

    ui64 commitId = State->GetCleanupCommitId();

    auto cleanupQueue = State->GetCleanupQueue().GetItems(
        commitId,
        Config->GetMaxBlobsToCleanup()
    );

    if (!cleanupQueue) {
        State->GetCleanupState().SetStatus(EOperationStatus::Idle);

        replyError(ctx, *requestInfo, S_ALREADY, "nothing to cleanup");
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Start cleanup @%lu (queue: %u)",
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
        commitId,
        static_cast<ui32>(cleanupQueue.size()));

    State->GetCleanupState().SetStatus(EOperationStatus::Started);

    AddTransaction<TEvPartitionPrivate::TCleanupMethod>(*requestInfo);

    ExecuteTx<TCleanup>(
        ctx,
        requestInfo,
        commitId,
        std::move(cleanupQueue));
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareCleanup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCleanup& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    bool ready = true;

    for (const auto& item: args.CleanupQueue) {
        TMaybe<NProto::TBlobMeta> blobMeta;
        if (db.ReadBlobMeta(item.BlobId, blobMeta)) {
            Y_ABORT_UNLESS(blobMeta.Defined(),
                "Could not read meta data for blob: %s",
                ToString(MakeBlobId(TabletID(), item.BlobId)).data());

            args.BlobsMeta.emplace_back(std::move(blobMeta.GetRef()));
        } else {
            ready = false;
        }
    }

    return ready;
}

void TPartitionActor::ExecuteCleanup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCleanup& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    size_t mixedBlobsCount = 0;
    size_t mergedBlobsCount = 0;


    Y_ABORT_UNLESS(args.CleanupQueue.size() == args.BlobsMeta.size());
    for (size_t i = 0; i < args.CleanupQueue.size(); ++i) {
        const auto& item = args.CleanupQueue[i];
        const auto& blobMeta = args.BlobsMeta[i];

        if (blobMeta.HasMixedBlocks()) {
            const auto& mixedBlocks = blobMeta.GetMixedBlocks();

            if (mixedBlocks.CommitIdsSize() == 0) {
                // every block shares the same commitId
                ui64 commitId = item.BlobId.CommitId();
                for (ui32 blockIndex: mixedBlocks.GetBlocks()) {
                    State->DeleteMixedBlock(db, blockIndex, commitId);
                }
            } else {
                // each block has its own commitId
                Y_ABORT_UNLESS(mixedBlocks.BlocksSize() == mixedBlocks.CommitIdsSize());
                for (size_t j = 0; j < mixedBlocks.BlocksSize(); ++j) {
                    ui32 blockIndex = mixedBlocks.GetBlocks(j);
                    ui64 commitId = mixedBlocks.GetCommitIds(j);
                    State->DeleteMixedBlock(db, blockIndex, commitId);
                }
            }

            ++mixedBlobsCount;
            if (!IsDeletionMarker(item.BlobId)) {
                // Mins for block counts are needed due to some inconsistencies caused by
                // NBS-1422
                State->DecrementMixedBlocksCount(
                    Min(mixedBlocks.BlocksSize(), State->GetMixedBlocksCount()));
            }
        } else if (blobMeta.HasMergedBlocks()) {
            const auto& mergedBlocks = blobMeta.GetMergedBlocks();

            auto blockRange = TBlockRange32::MakeClosedInterval(
                mergedBlocks.GetStart(),
                mergedBlocks.GetEnd());
            db.DeleteMergedBlocks(item.BlobId, blockRange);

            ++mergedBlobsCount;
            if (!IsDeletionMarker(item.BlobId)) {
                // Mins for block counts are needed due to some inconsistencies caused by
                // NBS-1422
                ui64 delta = blockRange.Size() - mergedBlocks.GetSkipped();
                State->DecrementMergedBlocksCount(
                    Min(delta, State->GetMergedBlocksCount()));
            }
        }

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu][d:%s] Delete blob: %s",
            TabletID(),
            PartitionConfig.GetDiskId().c_str(),
            ToString(MakeBlobId(TabletID(), item.BlobId)).data());

        State->RemoveCleanupQueueItem(item);

        db.DeleteBlobMeta(item.BlobId);
        db.DeleteCleanupQueue(item.BlobId, item.CommitId);

        if (!IsDeletionMarker(item.BlobId)) {
            db.WriteGarbageBlob(item.BlobId);
        }
    }

    // Updating counters
    State->DecrementMixedBlobsCount(mixedBlobsCount);
    State->DecrementMergedBlobsCount(mergedBlobsCount);

    db.WriteMeta(State->GetMeta());
}

void TPartitionActor::CompleteCleanup(
    const TActorContext& ctx,
    TTxPartition::TCleanup& args)
{
    TRequestScope timer(*args.RequestInfo);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Complete cleanup @%lu",
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
        args.CommitId);

    auto response = std::make_unique<TEvPartitionPrivate::TEvCleanupResponse>();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "Cleanup",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);

    State->GetCleanupState().SetStatus(EOperationStatus::Idle);

    // Addition to GarbageQueue is postponed till CompleteCleanup
    // to avoid race between Cleanup and CollectGarbage (see NBS-239)
    // This seems to be safe because CollectGarbage only processes
    // blobs added to GarbageQueue.
    for (const auto& item: args.CleanupQueue) {
        if (!IsDeletionMarker(item.BlobId)) {
            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%lu][d:%s] Add garbage blob: %s",
                TabletID(),
                PartitionConfig.GetDiskId().c_str(),
                ToString(MakeBlobId(TabletID(), item.BlobId)).data());

            bool added = State->GetGarbageQueue().AddGarbageBlob(item.BlobId);
            Y_ABORT_UNLESS(added);
        }
    }

    const auto d = CyclesToDurationSafe(args.RequestInfo->GetExecCycles());
    State->SetLastCleanupExecTime(d, ctx.Now());
    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());

    EnqueueCleanupIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);

    auto time = CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.Cleanup.AddRequest(time);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
