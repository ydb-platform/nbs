#include "part2_actor.h"

#include "part2_addblobs_logic.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

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

    if (CompactionMapLoadState) {
        const THashSet<ui32> rangeIndices =
            GetRangeIndices(msg->FreshBlobs, msg->MixedBlobs, msg->MergedBlobs);

        const auto ranges =
            CompactionMapLoadState->GetNotLoadedRanges(rangeIndices);

        if (!ranges.empty()) {
            CompactionMapLoadState->EnqueueOutOfOrderRanges(ranges);

            const auto error =
                MakeError(E_REJECTED, "compaction map not loaded yet");
            auto response =
                std::make_unique<TEvPartitionPrivate::TEvAddBlobsResponse>(
                    error);
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "AddBlobs",
        requestInfo->CallContext->RequestId);

    AddTransaction<TEvPartitionPrivate::TAddBlobsMethod>(*requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TAddBlobs>(
            requestInfo,
            msg->CommitId,
            std::move(msg->MixedBlobs),
            std::move(msg->MergedBlobs),
            std::move(msg->FreshBlobs),
            std::move(msg->L0Blobs),
            std::move(msg->L1Blobs),
            msg->Mode,
            std::move(msg->AffectedBlobs),
            std::move(msg->AffectedBlocks),
            std::move(msg->MixedBlobCompactionInfos),
            std::move(msg->MergedBlobCompactionInfos)));
}

bool TPartitionActor::PrepareAddBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddBlobs& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    // we really want to keep the writes blind
    return true;
}

void TPartitionActor::ExecuteAddBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    // NBS-415: we should keep garbage blobs until all readers gone,
    // but Args.CommitId is from past and could not be used for that.
    // ui64 deletionCommitId = args.CommitId;
    args.DeletionCommitId = State->GetLastCommitId();

    // need this barrier to prevent dirty reads from concurrent Cleanup
    State->GetCleanupQueue().AcquireBarrier(args.DeletionCommitId);

    ExecuteAddBlobsTransaction(
        ctx.ActorSystem(),
        LogTitle.GetChild(GetCycleCount()),
        TabletID(),
        PartitionConfig.GetDiskId(),
        args.DeletionCommitId,
        State->GetMaxBlocksInBlob(),
        db,
        args,
        *State);
}

void TPartitionActor::CompleteAddBlobs(
    const TActorContext& ctx,
    TTxPartition::TAddBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);

    auto response = std::make_unique<TEvPartitionPrivate::TEvAddBlobsResponse>();
    response->ExecCycles = args.RequestInfo->GetExecCycles();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "AddBlobs",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);

    State->GetCleanupQueue().ReleaseBarrier(args.DeletionCommitId);

    EnqueueCompactionIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);

    auto time = CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.AddBlobs.AddRequest(time);
}

THashSet<ui32> TPartitionActor::GetRangeIndices(
    const TVector<TAddFreshBlob>& freshBlobs,
    const TVector<TAddMixedBlob>& mixedBlobs,
    const TVector<TAddMergedBlob>& mergedBlobs) const
{
    const auto& compactionMap = State->AccessCompactionMap();

    THashSet<ui32> rangeIndices;

    for (const auto& blob: freshBlobs) {
        if (!blob.Blocks.empty()) {
            rangeIndices.emplace(
                compactionMap.GetRangeIndex(blob.Blocks.front().BlockIndex));
        }
    }

    for (const auto& blob: mixedBlobs) {
        for (const auto& block: blob.Blocks) {
            rangeIndices.emplace(compactionMap.GetRangeIndex(block));
        }
    }

    for (const auto& blob: mergedBlobs) {
        const auto rangeStart =
            compactionMap.GetRangeStart(blob.BlockRange.Start);
        rangeIndices.emplace(compactionMap.GetRangeIndex(rangeStart));
    }

    return rangeIndices;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
