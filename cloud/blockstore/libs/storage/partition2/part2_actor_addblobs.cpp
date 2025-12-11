#include "part2_actor.h"

#include <cloud/blockstore/libs/storage/partition2/part2_diagnostics.h>

#include <util/generic/algorithm.h>

#include <tuple>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NCloud::NStorage;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleAddBlobs(
    const TEvPartitionPrivate::TEvAddBlobsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "AddBlobs",
        requestInfo->CallContext->RequestId);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%lu] Start adding blobs",
        TabletID());

    AddTransaction<TEvPartitionPrivate::TAddBlobsMethod>(*requestInfo);

    ExecuteTx<TAddBlobs>(
        ctx,
        requestInfo,
        msg->Mode,
        std::move(msg->NewBlobs),
        std::move(msg->GarbageInfo),
        std::move(msg->AffectedBlobInfos),
        msg->BlobsSkippedByCompaction,
        msg->BlocksSkippedByCompaction);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareAddBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddBlobs& args)
{
    Y_UNUSED(ctx);

    // writes are usually blind but we still need our index structures to be
    // properly initialized
    bool ready = true;
    TPartitionDatabase db(tx.DB);
    for (const auto& blob: args.NewBlobs) {
        Y_DEBUG_ABORT_UNLESS(IsSorted(blob.Blocks.begin(), blob.Blocks.end()));
        ready &= State->InitIndex(
            db,
            TBlockRange32::MakeClosedInterval(
                blob.Blocks.front().BlockIndex,
                blob.Blocks.back().BlockIndex));
    }

    return ready;
}

void TPartitionActor::ExecuteAddBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddBlobs& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    if (args.Mode == ADD_WRITE_RESULT) {
        // first check if we have enough free commit ids for this operation
        ui32 step;
        std::tie(std::ignore, step) = ParseCommitId(State->GetLastCommitId());
        ui64 futureStep = step + args.NewBlobs.size();
        if (futureStep > Max<ui32>()) {
            args.Success = false;
            return;
        }
    }

    for (auto& blobInfo: args.AffectedBlobInfos) {
        // extra diagnostics
        TDumpBlockCommitIds dumpBlockCommitIds(
            blobInfo.Blocks,
            args.BlockCommitIds,
            Config->GetDumpBlockCommitIdsIntoProfileLog());

        // TODO: re-encode only deleted blocks
        // right now the code produces a correct result
        // but in an inefficient manner
        // see corresponding todo in part2_actor_compaction.cpp
        State->UpdateBlob(
            db,
            blobInfo.BlobId,
            false,   // fastPathAllowed
            blobInfo.Blocks);
        // TODO: update Ranges map as well? delete these blobs from the
        // corresponding ranges?
    }

    for (auto& blob: args.NewBlobs) {
        Y_ABORT_UNLESS(blob.Blocks.size());
        Y_ABORT_UNLESS(IsSorted(blob.Blocks.begin(), blob.Blocks.end()));

        auto blockRange = TBlockRange32::MakeClosedInterval(
            blob.Blocks.front().BlockIndex,
            blob.Blocks.back().BlockIndex);

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%lu] Add blob (blob: %s, range: %s)",
            TabletID(),
            DumpBlobIds(TabletID(), blob.BlobId).data(),
            DescribeRange(blockRange).data());

        // extra diagnostics
        TDumpBlockCommitIds dumpBlockCommitIds(
            blob.Blocks,
            args.BlockCommitIds,
            Config->GetDumpBlockCommitIdsIntoProfileLog());

        if (args.Mode == ADD_WRITE_RESULT || args.Mode == ADD_ZERO_RESULT) {
            // Flush/Compaction just transfers blocks from one place to another,
            // but WriteBlocks and ZeroBlocks are different: we need to generate
            // MinCommitId now
            ui64 commitId = State->GenerateCommitId();

            for (auto& block: blob.Blocks) {
                Y_ABORT_UNLESS(
                    block.MinCommitId == InvalidCommitId &&
                    block.MaxCommitId == InvalidCommitId);
                block.MinCommitId = commitId;
            }

            auto currentRange =
                TBlockRange32::MakeOneBlock(blob.Blocks.front().BlockIndex);

            for (ui32 i = 1; i < blob.Blocks.size(); ++i) {
                const auto& block = blob.Blocks[i];
                if (block.BlockIndex == currentRange.End + 1) {
                    ++currentRange.End;
                } else {
                    // mark overwritten blocks
                    State->AddFreshBlockUpdate(db, {commitId, currentRange});
                    State->MarkMergedBlocksDeleted(db, currentRange, commitId);

                    currentRange =
                        TBlockRange32::MakeOneBlock(block.BlockIndex);
                }
            }

            // mark overwritten blocks
            State->AddFreshBlockUpdate(db, {commitId, currentRange});
            State->MarkMergedBlocksDeleted(db, currentRange, commitId);
        }

        if (args.Mode == ADD_FLUSH_RESULT) {
            // delete fresh blocks
            for (auto& block: blob.Blocks) {
                const auto* freshBlock =
                    State->FindFreshBlock(block.BlockIndex, block.MinCommitId);
                if (freshBlock) {
                    block.MaxCommitId = freshBlock->Meta.MaxCommitId;
                }

                bool deleted = State->DeleteFreshBlock(
                    block.BlockIndex,
                    block.MinCommitId);

                Y_ABORT_UNLESS(deleted);
            }
        }

        if (args.Mode == ADD_COMPACTION_RESULT) {
            if (!args.GarbageInfo.BlobCounters) {
                State->ResetCompactionMap(
                    db,
                    blob.Blocks,
                    args.BlobsSkippedByCompaction,
                    args.BlocksSkippedByCompaction);
            }
        } else {
            State->UpdateCompactionMap(db, blob.Blocks);
        }

        State->WriteBlob(db, blob.BlobId, blob.Blocks);
    }

    // delete source blobs
    // TODO: test that we delete blobs
    if (args.Mode == ADD_COMPACTION_RESULT) {
        for (const auto& x: args.GarbageInfo.BlobCounters) {
            const auto& blobId = x.first;
            bool deleted = State->DeleteBlob(db, blobId);
            // blob could be deleted already
            // Y_ABORT_UNLESS(deleted, "Missing blob detected: %s",
            //     DumpBlobIds(TabletID(), blobId).data());
            Y_UNUSED(deleted);
        }
    }

    if (args.Mode == ADD_FLUSH_RESULT) {
        const ui64 commitId = State->GetFlushContext().CommitId;
        State->SetLastFlushCommitId(commitId);

        State->MoveBlobUpdatesByFreshToDb(db);
        State->TrimFreshBlockUpdates(db);
    }

    State->WriteStats(db);
}

void TPartitionActor::CompleteAddBlobs(
    const TActorContext& ctx,
    TTxPartition::TAddBlobs& args)
{
    if (!args.Success) {
        RebootPartitionOnCommitIdOverflow(ctx, "AddBlobs");
        return;
    }

    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%lu] Complete add blobs",
        TabletID());

    auto response =
        std::make_unique<TEvPartitionPrivate::TEvAddBlobsResponse>();
    response->ExecCycles = args.RequestInfo->GetExecCycles();
    response->BlockCommitIds = std::move(args.BlockCommitIds);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "AddBlobs",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    auto time =
        CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.AddBlobs.AddRequest(time);

    EnqueueCompactionIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
