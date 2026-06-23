#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition/model/background_ops_throttling.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NProto;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TVerifyResult {
    bool Ready = true;
    NProto::TError Error;
};

TVerifyResult VerifyMixedBlocksMeta(
    TPartitionDatabase& db,
    TPartialBlobId originalBlobId,
    const NProto::TBlobMeta::TMixedBlocks& originalMixedBlocks,
    const NProto::TBlobMeta::TMixedBlocks& recreatedMixedBlocks)
{
    // Check that blocks from recreated blob meta are present in the original
    // blob meta and that their commit ids are the same.
    // Some blocks may be missing in recreated blob meta because we delete some
    // blocks from mixed index on compaction.

    auto getCommitId = [&](const NProto::TBlobMeta::TMixedBlocks& mixedBlocks,
                           size_t i) -> ui64
    {
        return i < mixedBlocks.CommitIdsSize() ? mixedBlocks.GetCommitIds(i)
                                               : originalBlobId.CommitId();
    };

    TVector<TBlock> originalBlocks;
    for (size_t i = 0; i < originalMixedBlocks.BlocksSize(); ++i) {
        originalBlocks.emplace_back(
            originalMixedBlocks.GetBlocks(i),
            getCommitId(originalMixedBlocks, i),
            false);
    }

    TVector<TBlock> recreatedBlocks;
    for (size_t i = 0; i < recreatedMixedBlocks.BlocksSize(); ++i) {
        recreatedBlocks.emplace_back(
            recreatedMixedBlocks.GetBlocks(i),
            getCommitId(recreatedMixedBlocks, i),
            false);
    }

    auto cmp = [](const TBlock& l, const TBlock& r)
    {
        if (l.BlockIndex == r.BlockIndex) {
            return l.CommitId < r.CommitId;
        }
        return l.BlockIndex < r.BlockIndex;
    };

    Sort(originalBlocks, cmp);
    Sort(recreatedBlocks, cmp);

    TVector<TBlock> missedBlocks;

    size_t recreatedIdx = 0, originalIdx = 0;
    while (recreatedIdx < recreatedBlocks.size() &&
           originalIdx < originalBlocks.size())
    {
        if (recreatedBlocks[recreatedIdx] != originalBlocks[originalIdx]) {
            missedBlocks.emplace_back(originalBlocks[originalIdx]);

            ++originalIdx;
            continue;
        }

        ++recreatedIdx;
        ++originalIdx;
    }

    for (; originalIdx < originalBlocks.size(); ++originalIdx) {
        missedBlocks.emplace_back(originalBlocks[originalIdx]);
    }

    if (recreatedIdx < recreatedBlocks.size()) {
        auto error = MakeError(
            E_ARGUMENT,
            "there are blocks that are not present in the original blob "
            "meta");
        return {.Error = std::move(error)};
    }

    struct TVisitor final: public IMixedBlocksIndexVisitor
    {
        TVector<TBlock> LeakedBlocks;

        bool VisitBlock(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset,
            ui8 compactionRangeCount) override
        {
            Y_UNUSED(blobId);
            Y_UNUSED(blobOffset);
            Y_UNUSED(compactionRangeCount);

            LeakedBlocks.emplace_back(blockIndex, commitId, false);

            return true;
        }
    };

    TVisitor visitor;
    bool ready = db.FindMixedBlocks(visitor, missedBlocks);
    if (!ready) {
        return {.Ready = false};
    }

    if (visitor.LeakedBlocks) {
        TStringBuilder sb;
        sb << "Leaked blocks in recreated blob meta: ";
        for (size_t i = 0; i < visitor.LeakedBlocks.size(); ++i) {
            sb << "{ BlockIndex: " << visitor.LeakedBlocks[i].BlockIndex
               << ", CommitId: " << visitor.LeakedBlocks[i].CommitId << " } ";
        }
        auto error = MakeError(E_ARGUMENT, std::move(sb));
        return {.Error = std::move(error)};
    }

    return {};
}

TVerifyResult VerifyMergedBlocksMeta(
    const NProto::TBlobMeta::TMergedBlocks& originalMergedBlocks,
    const NProto::TBlobMeta::TMergedBlocks& recreatedMergedBlocks)
{
    bool ok =
        originalMergedBlocks.GetStart() == recreatedMergedBlocks.GetStart() &&
        originalMergedBlocks.GetEnd() == recreatedMergedBlocks.GetEnd() &&
        originalMergedBlocks.GetSkipped() == recreatedMergedBlocks.GetSkipped();

    if (!ok) {
        auto error = MakeError(E_ARGUMENT, "Mismatched merged blocks");
        return {.Error = std::move(error)};
    }

    return {};
}

TVerifyResult VerifyRecreatedBlobMeta(
    TPartitionDatabase& db,
    TPartialBlobId originalBlobId,
    const NProto::TBlobMeta& blobMeta,
    const NProto::TBlobMeta& recreatedBlobMeta)
{
    if (blobMeta.HasMixedBlocks() != recreatedBlobMeta.HasMixedBlocks() ||
        blobMeta.HasMergedBlocks() != recreatedBlobMeta.HasMergedBlocks())
    {
        auto error = MakeError(E_ARGUMENT, "Mismatched blob meta types");
        return {.Error = std::move(error)};
    }

    if (blobMeta.HasMixedBlocks()) {
        return VerifyMixedBlocksMeta(
            db,
            originalBlobId,
            blobMeta.GetMixedBlocks(),
            recreatedBlobMeta.GetMixedBlocks());
    }

    if (blobMeta.HasMergedBlocks()) {
        return VerifyMergedBlocksMeta(
            blobMeta.GetMergedBlocks(),
            recreatedBlobMeta.GetMergedBlocks());
    }

    return {};
}

}   // namespace

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

    State->GetCleanupState().SetStatus(EOperationStatus::Enqueued, ctx.Now());

    auto request = std::make_unique<TEvPartitionPrivate::TEvCleanupRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()));

    const auto throttlingAllowed = State->GetCleanupQueue().GetQueueBytes()
        < Config->GetCleanupQueueBytesLimitForThrottling();

    if (throttlingAllowed) {
        State->SetCleanupDelay(CalculateBackgroundOpThrottleDelay(
            State->GetCleanupExecTimeForLastSecond(ctx.Now()),
            Config->GetMaxCleanupExecTimePerSecond(),
            Config->GetMinCleanupDelay(),
            Config->GetMaxCleanupDelay()));
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
        State->GetCleanupState().SetStatus(EOperationStatus::Idle, ctx.Now());

        replyError(
            ctx,
            *requestInfo,
            E_TRY_AGAIN,
            "Metadata rebuild is running");
        return;
    }

    if (State->IsScanDiskStarted()) {
        State->GetCleanupState().SetStatus(EOperationStatus::Idle, ctx.Now());

        replyError(ctx, *requestInfo, E_TRY_AGAIN, "Scan disk is running");
        return;
    }

    ui64 commitId = State->GetCleanupCommitId();

    auto cleanupQueue = State->GetCleanupQueue().GetItems(
        commitId,
        Config->GetMaxBlobsToCleanup());

    if (!cleanupQueue) {
        State->GetCleanupState().SetStatus(EOperationStatus::Idle, ctx.Now());

        replyError(ctx, *requestInfo, S_ALREADY, "nothing to cleanup");
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start cleanup @%lu (queue: %u)",
        LogTitle.GetWithTime().c_str(),
        commitId,
        static_cast<ui32>(cleanupQueue.size()));

    State->GetCleanupState().SetStatus(EOperationStatus::Started, ctx.Now());

    AddTransaction<TEvPartitionPrivate::TCleanupMethod>(*requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TCleanup>(requestInfo, commitId, std::move(cleanupQueue)));
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

    THashSet<TPartialBlobId, TPartialBlobIdHash> blobIdsToRemoveFromQueue;
    THashMap<TPartialBlobId, NProto::TBlobMeta, TPartialBlobIdHash> blobMetas;

    bool ready = true;

    for (const auto& item: args.CleanupQueue) {
        // no need to read blob meta for blobs with already known blocks
        const bool hasValidMetaInCleanupQueue =
            item.BlobMeta.HasMixedBlocks() || item.BlobMeta.HasMergedBlocks();

        TMaybe<NProto::TBlobMeta> blobMeta;
        if (db.ReadBlobMeta(item.BlobId, blobMeta)) {
            Y_ABORT_UNLESS(
                blobMeta.Defined(),
                "Could not read meta data for blob: %s",
                ToString(MakeBlobId(TabletID(), item.BlobId)).data());
            auto& meta = blobMetas[item.BlobId];
            meta = std::move(blobMeta.GetRef());

            const bool verifyRecreatedBlobMetasOnCleanup =
                hasValidMetaInCleanupQueue &&
                Config->GetVerifyRecreatedBlobMetasOnCleanup();
            if (!verifyRecreatedBlobMetasOnCleanup) {
                continue;
            }

            auto [ready, error] =
                VerifyRecreatedBlobMeta(db, item.BlobId, meta, item.BlobMeta);
            if (!ready) {
                ready = false;
                continue;
            }

            if (HasError(error)) {
                blobIdsToRemoveFromQueue.insert(item.BlobId);
                ReportCleanupBlobMetaBlocksMismatch(
                    {{"diskId", PartitionConfig.GetDiskId()},
                     {"tabletId", TabletID()},
                     {"blobId", ToString(MakeBlobId(TabletID(), item.BlobId))},
                     {"recreatedBlobMeta",
                      item.BlobMeta.ShortUtf8DebugString()},
                     {"originalBlobMeta", meta.ShortUtf8DebugString()},
                     {"error", FormatError(error)}});
            }
        } else {
            ready = false;
        }
    }

    if (ready) {
        auto itemsToRemove = std::ranges::remove_if(
            args.CleanupQueue,
            [&](const auto& item) -> bool
            { return blobIdsToRemoveFromQueue.contains(item.BlobId); });
        args.CleanupQueue.erase(itemsToRemove.begin(), itemsToRemove.end());

        for (const auto& item: args.CleanupQueue) {
            args.BlobsMeta.push_back(std::move(blobMetas[item.BlobId]));
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

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Delete blob: %s",
            LogTitle.GetWithTime().c_str(),
            ToString(MakeBlobId(TabletID(), item.BlobId)).Quote().c_str());

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

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete Cleanup transaction @%lu",
        LogTitle.GetWithTime().c_str(),
        args.CommitId);

    auto response = std::make_unique<TEvPartitionPrivate::TEvCleanupResponse>();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "Cleanup",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);

    State->GetCleanupState().SetStatus(EOperationStatus::Idle, ctx.Now());

    // Addition to GarbageQueue is postponed till CompleteCleanup
    // to avoid race between Cleanup and CollectGarbage (see NBS-239)
    // This seems to be safe because CollectGarbage only processes
    // blobs added to GarbageQueue.
    for (const auto& item: args.CleanupQueue) {
        if (!IsDeletionMarker(item.BlobId)) {
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s Add garbage blob: %s",
                LogTitle.GetWithTime().c_str(),
                ToString(MakeBlobId(TabletID(), item.BlobId)).Quote().c_str());

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
