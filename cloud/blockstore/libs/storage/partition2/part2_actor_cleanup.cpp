#include "part2_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition2/part2_diagnostics.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

using EMode = TEvPartitionPrivate::ECleanupMode;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueCleanup(
    const TActorContext& ctx,
    TEvPartitionPrivate::ECleanupMode mode)
{
    State->SetCleanupStatus(EOperationStatus::Enqueued);

    auto request = std::make_unique<TEvPartitionPrivate::TEvCleanupRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()),
        mode);

    NCloud::Send(
        ctx,
        SelfId(),
        std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

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
        msg->Mode == TEvPartitionPrivate::ECleanupMode::DirtyBlobCleanup
            ? "DirtyBlobCleanup" : "CheckpointBlobCleanup",
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

    if (State->GetCleanupStatus() == EOperationStatus::Delayed ||
        State->GetCleanupStatus() == EOperationStatus::Started)
    {
        replyError(ctx, *requestInfo, E_TRY_AGAIN, "cleanup already started");
        return;
    }

    auto createTxAndEnqueueIntoCCCQueue = [&] (ui64 commitId, auto onStartProcessing) {
        State->SetCleanupStatus(EOperationStatus::Delayed);

        AddTransaction<TEvPartitionPrivate::TCleanupMethod>(*requestInfo);
        auto tx = CreateTx<TCleanup>(requestInfo, commitId, msg->Mode);

        auto& queue = State->GetCCCRequestQueue();
        queue.push_back({ commitId, std::move(tx), std::move(onStartProcessing) });

        ProcessCCCRequestQueue(ctx);
    };

    const auto limit = static_cast<size_t>(Config->GetMaxBlobsToCleanup());

    if (msg->Mode == EMode::CheckpointBlobCleanup) {
        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Extracting checkpoint blobs for cleanup @%lu",
            TabletID());

        State->ExtractCheckpointBlobsToCleanup(limit);

        // don't wait for inflight fresh to complete
        createTxAndEnqueueIntoCCCQueue(0, [this] (const TActorContext& ctx) {
            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Starting checkpoint blob cleanup @%lu",
                TabletID(),
                State->GetCleanupCheckpointCommitId());

            State->SetCleanupStatus(EOperationStatus::Started);
        });
        return;
    }

    if (State->HasPendingChunkToCleanup()) {
        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Extracting dirty blobs from pending chunk to cleanup",
            TabletID());

        State->ExtractBlobsFromChunkToCleanup(limit);

        // don't wait for inflight fresh to complete
        createTxAndEnqueueIntoCCCQueue(0, [this] (const TActorContext& ctx) {
            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Resuming dirty blob cleanup for pending chunk",
                TabletID());

            State->SetCleanupStatus(EOperationStatus::Started);
        });
        return;
    }

    if (!State->TrySelectZoneToCleanup()) {
        State->SetCleanupStatus(EOperationStatus::Idle);
        replyError(ctx, *requestInfo, S_ALREADY, "nothing to cleanup");
        return;
    }

    const ui64 commitId = State->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        requestInfo->CancelRequest(ctx);
        RebootPartitionOnCommitIdOverflow(ctx, "Cleanup");
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Separating chunk for dirty blob cleanup @%lu",
        TabletID(),
        commitId);

    State->SeparateChunkForCleanup(commitId);

    createTxAndEnqueueIntoCCCQueue(
        commitId, [this, commitId, limit] (const TActorContext& ctx) {
            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Starting dirty blob cleanup @%lu",
                TabletID(),
                commitId);

            State->SetCleanupStatus(EOperationStatus::Started);
            State->ExtractChunkToCleanup();
            State->ExtractBlobsFromChunkToCleanup(limit);
        }
    );
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

    const ui32 zoneId = State->GetPendingCleanupZoneId();

    const auto& index = State->GetBlobs();
    const auto& blobs = args.Mode == EMode::DirtyBlobCleanup
        ? State->GetPendingBlobsToCleanup()
        : State->GetPendingCheckpointBlobsToCleanup();

    // some blobs could be already deleted
    for (const auto& blobId: blobs) {
        if (index.FindBlob(zoneId, blobId).Blob) {
            args.ExistingBlobIds.push_back(blobId);
        }
    }

    bool ready = true;

    for (const auto& blobId: args.ExistingBlobIds) {
        TMaybe<TBlockList> list;
        if (!State->FindBlockList(db, zoneId, blobId, list)) {
            ready = false;
        }

        if (ready) {
            Y_ABORT_UNLESS(list);
            args.BlockLists.emplace_back(list->GetBlocks());
        }
    }

    for (const auto& blocklist: args.BlockLists) {
        const auto blobRange = TBlockRange32::MakeClosedInterval(
            blocklist.front().BlockIndex,
            blocklist.back().BlockIndex);

        ready &= State->InitIndex(db, blobRange);
    }

    return ready;
}

void TPartitionActor::ExecuteCleanup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCleanup& args)
{
    Y_UNUSED(ctx);

    // needed to prevent the blobs updated during this tx from being deleted
    // before this tx completes
    args.CollectBarrierCommitId = State->GetLastCommitId();
    State->AcquireCollectBarrier(args.CollectBarrierCommitId);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    Y_ABORT_UNLESS(args.ExistingBlobIds.size() == args.BlockLists.size());
    size_t i = 0;

    for (const auto& blobId: args.ExistingBlobIds) {
        auto& blocks = args.BlockLists[i++];

        // extra diagnostics
        TDumpBlockCommitIds dumpBlockCommitIds(
            blocks,
            args.BlockCommitIds,
            Config->GetDumpBlockCommitIdsIntoProfileLog());

        bool updated = State->UpdateBlob(
            db,
            blobId,
            args.Mode == EMode::DirtyBlobCleanup,  // fast path allowed
            blocks
        );

        Y_ABORT_UNLESS(updated, "Missing blob detected: %s",
            DumpBlobIds(TabletID(), blobId).data());
    }

    if (args.Mode == EMode::DirtyBlobCleanup) {
        args.BlobUpdates = State->FinishDirtyBlobCleanup(db);
    } else {
        State->FinishCheckpointBlobCleanup(db);
    }

    State->WriteStats(db);
}

void TPartitionActor::CompleteCleanup(
    const TActorContext& ctx,
    TTxPartition::TCleanup& args)
{
    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    State->StopProcessingCCCRequest();

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] %s cleanup completed @%lu",
        TabletID(),
        args.Mode == EMode::DirtyBlobCleanup ? "dirty blob" : "checkpoint blob",
        args.CommitId);

    auto response = std::make_unique<TEvPartitionPrivate::TEvCleanupResponse>();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "Cleanup",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    UpdateCPUUsageStat(ctx, timer.Finish());

    const auto d = CyclesToDurationSafe(args.RequestInfo->GetTotalCycles());
    PartCounters->RequestCounters.Cleanup.AddRequest(d.MicroSeconds());

    State->ReleaseCollectBarrier(args.CollectBarrierCommitId);
    State->SetCleanupStatus(EOperationStatus::Idle);

    EnqueueCompactionIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);

    ProcessCCCRequestQueue(ctx);

    {
        IProfileLog::TSysReadWriteRequest request;
        request.RequestType = ESysRequestType::Cleanup;
        request.Duration = d;

        TVector<ui32> blockIndices;
        ui32 blockCount = 0;
        for (const auto& l: args.BlockLists) {
            Y_ABORT_UNLESS(l.size());
            blockCount += l.size();
        }
        blockIndices.reserve(blockCount);
        for (const auto& l: args.BlockLists) {
            for (const auto& b: l) {
                blockIndices.push_back(b.BlockIndex);
            }
        }
        SortUnique(blockIndices);

        TBlockRange64Builder rangeBuilder(request.Ranges);
        for (const auto b: blockIndices) {
            rangeBuilder.OnBlock(b);
        }

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ctx.Now() - d;
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }

    LogBlockCommitIds(
        ctx,
        ESysRequestType::Cleanup,
        std::move(args.BlockCommitIds),
        args.CommitId);

    if (Config->GetDumpBlobUpdatesIntoProfileLog()) {
        IProfileLog::TCleanupRequestBlobUpdates request;
        TVector<IProfileLog::TBlobUpdate> blobUpdates;
        blobUpdates.reserve(args.BlobUpdates.size());
        for (const auto& blobUpdate: args.BlobUpdates) {
            blobUpdates.push_back({blobUpdate.BlockRange, blobUpdate.CommitId});
        }
        request.BlobUpdates = std::move(blobUpdates);
        request.CleanupCommitId = args.CommitId;

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ctx.Now();
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
