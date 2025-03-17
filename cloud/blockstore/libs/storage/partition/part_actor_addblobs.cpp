#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/tablet/gc_logic.h>

#include <library/cpp/containers/dense_hash/dense_hash.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NCloud::NStorage;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

TString DescribeFreshRange(const TVector<TBlock>& blocks)
{
    if (blocks) {
        return TStringBuilder()
            << "[" << blocks.front().BlockIndex << ".." << blocks.back().BlockIndex << "]";
    }
    return "<none>";
}

bool HasDuplicates(const TVector<ui32>& items)
{
    if (items.size() > 1) {
        for (size_t i = 1; i < items.size(); ++i) {
            Y_DEBUG_ABORT_UNLESS(items[i - 1] <= items[i]);
            if (items[i - 1] == items[i]) {
                return true;
            }
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

class TAddBlobsExecutor
{
private:
    TPartitionState& State;
    TTxPartition::TAddBlobs& Args;

    const ui64 TabletId;
    const ui64 DeletionCommitId;
    const ui32 MaxBlocksInBlob;

    struct TRangeInfo
    {
        TRangeStat Stat;
        ui32 BlobsSkippedByCompaction = 0;
        ui32 BlocksSkippedByCompaction = 0;
    };

    TDenseHash<ui32, TRangeInfo> CompactionCounters { std::numeric_limits<ui32>::max() };
    TDenseHash<ui32, ui64> OverwrittenBlocks { std::numeric_limits<ui32>::max() };

public:
    TAddBlobsExecutor(
            TPartitionState& state,
            TTxPartition::TAddBlobs& args,
            ui64 tabletId,
            ui64 deletionCommitId,
            ui32 maxBlocksInBlob)
        : State(state)
        , Args(args)
        , TabletId(tabletId)
        , DeletionCommitId(deletionCommitId)
        , MaxBlocksInBlob(maxBlocksInBlob)
    {}

    void Execute(const TActorContext& ctx, TPartitionDatabase& db)
    {
        if (Args.Mode == ADD_COMPACTION_RESULT) {
            Y_ABORT_UNLESS(
                Args.MixedBlobs.size() ==
                Args.MixedBlobCompactionInfos.size());
        }

        for (ui32 i = 0; i < Args.MixedBlobs.size(); ++i) {
            const auto& blob = Args.MixedBlobs[i];
            ProcessNewBlob(ctx, db, blob);
            UpdateCompactionCounters(blob);
            if (Args.Mode == EAddBlobMode::ADD_WRITE_RESULT) {
                UpdateUsedBlocks(db, blob);
            }

            if (Args.Mode == ADD_COMPACTION_RESULT) {
                const auto& cm = State.GetCompactionMap();
                const auto blockIndex = cm.GetRangeStart(BlockIndex(blob, 0));
                auto& rangeInfo = CompactionCounters[blockIndex];
                rangeInfo.BlobsSkippedByCompaction =
                    Args.MixedBlobCompactionInfos[i].BlobsSkippedByCompaction;
                rangeInfo.BlocksSkippedByCompaction =
                    Args.MixedBlobCompactionInfos[i].BlocksSkippedByCompaction;
            }
        }

        if (Args.Mode == ADD_COMPACTION_RESULT) {
            Y_ABORT_UNLESS(
                Args.MergedBlobs.size() ==
                Args.MergedBlobCompactionInfos.size());
        }

        for (ui32 i = 0; i < Args.MergedBlobs.size(); ++i) {
            const auto& blob = Args.MergedBlobs[i];
            ProcessNewBlob(ctx, db, blob);
            UpdateCompactionCounters(blob);
            if (Args.Mode == EAddBlobMode::ADD_WRITE_RESULT) {
                UpdateUsedBlocks(db, blob);
            }

            if (Args.Mode == ADD_COMPACTION_RESULT) {
                const auto& cm = State.GetCompactionMap();
                const auto blockIndex = cm.GetRangeStart(blob.BlockRange.Start);
                Y_DEBUG_ABORT_UNLESS(
                    blockIndex == cm.GetRangeStart(blob.BlockRange.End));
                auto& rangeInfo = CompactionCounters[blockIndex];
                rangeInfo.BlobsSkippedByCompaction =
                    Args.MergedBlobCompactionInfos[i].BlobsSkippedByCompaction;
                rangeInfo.BlocksSkippedByCompaction =
                    Args.MergedBlobCompactionInfos[i].BlocksSkippedByCompaction;
            }
        }

        for (const auto& blob: Args.FreshBlobs) {
            ProcessOverwrittenBlocks(blob);
        }
        UpdateUsedFreshBlocks(db);

        for (const auto& blob: Args.FreshBlobs) {
            ProcessNewBlob(ctx, db, blob);
            UpdateCompactionCounters(blob);
        }

        if (Args.Mode == ADD_COMPACTION_RESULT) {
            ProcessAffectedBlobs(db);
            ProcessAffectedBlocks(db);
        }

        UpdateCompactionMap(db);
        State.UpdateTrimFreshLogToCommitIdInMeta();

        db.WriteMeta(State.GetMeta());
    }

private:
    void ProcessNewBlob(
        const TActorContext& ctx,
        TPartitionDatabase& db,
        const TAddMixedBlob& blob)
    {
        Y_DEBUG_ABORT_UNLESS(blob.BlobId.CommitId() == Args.CommitId);
        Y_DEBUG_ABORT_UNLESS(!HasDuplicates(blob.Blocks));

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            IsDeletionMarker(blob.BlobId)
                ? "[%lu] Add MixedBlob (zero blocks) @%lu (blob: %s, range: %s)"
                : "[%lu] Add MixedBlob @%lu (blob: %s, range: %s)",
            TabletId,
            Args.CommitId,
            ToString(MakeBlobId(TabletId, blob.BlobId)).data(),
            DescribeRange(blob.Blocks).data());

        // write blob meta
        NProto::TBlobMeta blobMeta;

        auto& mixedBlocks = *blobMeta.MutableMixedBlocks();
        mixedBlocks.MutableBlocks()->Reserve(blob.Blocks.size());

        for (ui32 blockIndex: blob.Blocks) {
            mixedBlocks.AddBlocks(blockIndex);
        }

        for (ui32 checksum: blob.Checksums) {
            blobMeta.AddBlockChecksums(checksum);
        }

        db.WriteBlobMeta(blob.BlobId, blobMeta);

        if (!IsDeletionMarker(blob.BlobId)) {
            bool added = State.GetGarbageQueue().AddNewBlob(blob.BlobId);
            Y_ABORT_UNLESS(added);
        }

        // write blocks mask
        TBlockMask blockMask;

        for (ui16 blobOffset = blob.Blocks.size();
            blobOffset < MaxBlocksInBlob;
            ++blobOffset)
        {
            blockMask.Set(blobOffset);
        }

        Y_ABORT_UNLESS(!IsBlockMaskFull(blockMask, MaxBlocksInBlob));
        db.WriteBlockMask(blob.BlobId, blockMask);

        // write blocks
        State.WriteMixedBlocks(db, blob.BlobId, blob.Blocks);

        // update counters
        State.IncrementMixedBlobsCount(1);
        if (!IsDeletionMarker(blob.BlobId)) {
            State.IncrementMixedBlocksCount(blob.Blocks.size());
        }
    }

    void ProcessNewBlob(
        const TActorContext& ctx,
        TPartitionDatabase& db,
        const TAddMergedBlob& blob)
    {
        Y_DEBUG_ABORT_UNLESS(blob.BlobId.CommitId() == Args.CommitId);

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            IsDeletionMarker(blob.BlobId)
                ? "[%lu] Add MergedBlob (zero blocks) @%lu (blob: %s, range: %s)"
                : "[%lu] Add MergedBlob @%lu (blob: %s, range: %s)",
            TabletId,
            Args.CommitId,
            ToString(MakeBlobId(TabletId, blob.BlobId)).data(),
            DescribeRange(blob.BlockRange).data());

        const auto skipped = blob.SkipMask.Count();
        Y_ABORT_UNLESS(skipped < blob.BlockRange.Size());

        // write blob meta
        NProto::TBlobMeta blobMeta;

        auto& mergedBlocks = *blobMeta.MutableMergedBlocks();
        mergedBlocks.SetStart(blob.BlockRange.Start);
        mergedBlocks.SetEnd(blob.BlockRange.End);
        mergedBlocks.SetSkipped(skipped);

        for (ui32 checksum: blob.Checksums) {
            blobMeta.AddBlockChecksums(checksum);
        }

        db.WriteBlobMeta(blob.BlobId, blobMeta);

        if (!IsDeletionMarker(blob.BlobId)) {
            bool added = State.GetGarbageQueue().AddNewBlob(blob.BlobId);
            Y_ABORT_UNLESS(added);
        }

        // write blocks mask
        TBlockMask blockMask;

        for (ui16 blobOffset = blob.BlockRange.Size() - skipped;
            blobOffset < MaxBlocksInBlob;
            ++blobOffset)
        {
            blockMask.Set(blobOffset);
        }

        Y_ABORT_UNLESS(!IsBlockMaskFull(blockMask, MaxBlocksInBlob));
        db.WriteBlockMask(blob.BlobId, blockMask);

        // write blocks
        db.WriteMergedBlocks(blob.BlobId, blob.BlockRange, blob.SkipMask);

        // update counters
        State.IncrementMergedBlobsCount(1);
        if (!IsDeletionMarker(blob.BlobId)) {
            State.IncrementMergedBlocksCount(blob.BlockRange.Size() - skipped);
        }

        State.ConfirmedBlobsAdded(db, Args.CommitId);
    }

    void ProcessNewBlob(
        const TActorContext& ctx,
        TPartitionDatabase& db,
        const TAddFreshBlob& blob)
    {
        Y_DEBUG_ABORT_UNLESS(blob.BlobId.CommitId() == Args.CommitId);

        // duplicates are allowed
        Y_DEBUG_ABORT_UNLESS(IsSorted(blob.Blocks.begin(), blob.Blocks.end()));

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            IsDeletionMarker(blob.BlobId)
                ? "[%lu] Add FreshBlob (zero blocks) @%lu (blob: %s, range: %s)"
                : "[%lu] Add FreshBlob @%lu (blob: %s, range: %s)",
            TabletId,
            Args.CommitId,
            ToString(MakeBlobId(TabletId, blob.BlobId)).data(),
            DescribeFreshRange(blob.Blocks).data());

        // write blob meta
        NProto::TBlobMeta blobMeta;

        auto& mixedBlocks = *blobMeta.MutableMixedBlocks();
        mixedBlocks.MutableBlocks()->Reserve(blob.Blocks.size());
        mixedBlocks.MutableCommitIds()->Reserve(blob.Blocks.size());

        for (const auto& block: blob.Blocks) {
            mixedBlocks.AddBlocks(block.BlockIndex);
            mixedBlocks.AddCommitIds(block.CommitId);
        }

        for (ui32 checksum: blob.Checksums) {
            blobMeta.AddBlockChecksums(checksum);
        }

        db.WriteBlobMeta(blob.BlobId, blobMeta);

        if (!IsDeletionMarker(blob.BlobId)) {
            bool added = State.GetGarbageQueue().AddNewBlob(blob.BlobId);
            Y_ABORT_UNLESS(added);
        }

        // write blocks mask
        TBlockMask blockMask;

        for (ui16 blobOffset = blob.Blocks.size();
            blobOffset < MaxBlocksInBlob;
            ++blobOffset)
        {
            blockMask.Set(blobOffset);
        }

        // mask overwritten blocks (there could be multiple block versions)
        ui16 blobOffset = 0;
        for (const auto& block: blob.Blocks) {
            ui64 lastCommitId = OverwrittenBlocks[block.BlockIndex];
            if (lastCommitId > block.CommitId) {
                blockMask.Set(blobOffset);
            }
            ++blobOffset;
        }

        db.WriteBlockMask(blob.BlobId, blockMask);

        if (IsBlockMaskFull(blockMask, MaxBlocksInBlob)) {
            // blob already could be garbage, but we should keep it
            // as there could be active readers (or even checkpoint)
            db.WriteCleanupQueue(blob.BlobId, DeletionCommitId);
            State.GetCleanupQueue().Add({ blob.BlobId, DeletionCommitId });
        }

        // move blocks from FreshBlocks to MixedBlocks
        blobOffset = 0;
        for (const auto& block: blob.Blocks) {
            State.WriteMixedBlock(db, {
                blob.BlobId,
                block.CommitId,
                block.BlockIndex,
                blobOffset++});

            if (block.IsStoredInDb) {
                State.DeleteFreshBlock(db, block.BlockIndex, block.CommitId);
            } else {
                State.DeleteFreshBlock(block.BlockIndex, block.CommitId);
            }
        }

        // update counters
        State.IncrementMixedBlobsCount(1);
        if (!IsDeletionMarker(blob.BlobId)) {
            State.IncrementMixedBlocksCount(blob.Blocks.size());
        }
    }

    void ProcessOverwrittenBlocks(const TAddFreshBlob& blob)
    {
        // blocks in each blob are ordered by BlockIndex and CommitId,
        // but such total order is not guaranteed across set of
        // fresh blobs which could contain both zero and non-zero
        // blobs mixed
        for (const auto& block: blob.Blocks) {
            ui64& lastCommitId = OverwrittenBlocks[block.BlockIndex];
            if (lastCommitId < block.CommitId) {
                lastCommitId = block.CommitId;
            }
        }
    }

    auto& AccessRangeStat(ui32 blockIndex)
    {
        const auto& cm = State.GetCompactionMap();
        auto& rangeInfo = CompactionCounters[blockIndex];

        if (!rangeInfo.Stat.BlobCount && Args.Mode != ADD_COMPACTION_RESULT) {
            rangeInfo.Stat = cm.Get(blockIndex);
        }

        return rangeInfo.Stat;
    };

    void UpdateCompactionCounters(const TAddMergedBlob& blob)
    {
        const auto& cm = State.GetCompactionMap();

        auto range = TBlockRange32::MakeClosedInterval(
            cm.GetRangeStart(blob.BlockRange.Start),
            cm.GetRangeStart(blob.BlockRange.End));

        for (const ui64 blockIndex: xrange(range, cm.GetRangeSize())) {
            auto& rangeStat = AccessRangeStat(blockIndex);

            TCompactionMap::UpdateCompactionCounter(
                rangeStat.BlobCount + 1,
                &rangeStat.BlobCount
            );

            if (IsDeletionMarker(blob.BlobId)) {
                continue;
            }

            const auto firstBlock = Max<ui64>(blockIndex, blob.BlockRange.Start);
            const auto lastBlock =
                Min<ui64>(blockIndex + cm.GetRangeSize() - 1, blob.BlockRange.End);
            ui32 skipped = 0;
            for (ui64 b = firstBlock; b <= lastBlock; ++b) {
                auto pos = b - blob.BlockRange.Start;
                if (blob.SkipMask.Get(pos)) {
                    ++skipped;
                }
            }
            TCompactionMap::UpdateCompactionCounter(
                rangeStat.BlockCount + (lastBlock - firstBlock + 1 - skipped),
                &rangeStat.BlockCount
            );
        }
    }

    static ui32 BlockIndex(const TAddMixedBlob& blob, ui32 i)
    {
        return blob.Blocks[i];
    }

    static ui32 BlockIndex(const TAddFreshBlob& blob, ui32 i)
    {
        return blob.Blocks[i].BlockIndex;
    }

    template <class TAddSparseBlob>
    void UpdateCompactionCounters(const TAddSparseBlob& blob)
    {
        const auto& cm = State.GetCompactionMap();

        ui32 prevBlockIndex = 0;
        TRangeStat* rangeStat = nullptr;

        for (size_t i = 0; i < blob.Blocks.size(); ++i) {
            ui32 blockIndex = cm.GetRangeStart(BlockIndex(blob, i));
            Y_DEBUG_ABORT_UNLESS(prevBlockIndex <= blockIndex);

            if (i == 0 || prevBlockIndex != blockIndex) {
                prevBlockIndex = blockIndex;

                rangeStat = &AccessRangeStat(blockIndex);
                TCompactionMap::UpdateCompactionCounter(
                    rangeStat->BlobCount + 1,
                    &rangeStat->BlobCount
                );
            }

            if (!IsDeletionMarker(blob.BlobId)) {
                TCompactionMap::UpdateCompactionCounter(
                    rangeStat->BlockCount + 1,
                    &rangeStat->BlockCount
                );
            }
        }
    }

    void UpdateCompactionMap(TPartitionDatabase& db)
    {
        for (const auto& kv: CompactionCounters) {
            const auto usedBlockCount = State.GetUsedBlocks().Count(
                kv.first,
                Min(
                    static_cast<ui64>(
                        kv.first + State.GetCompactionMap().GetRangeSize()
                    ),
                    State.GetUsedBlocks().Capacity()
                )
            );
            db.WriteCompactionMap(
                kv.first,
                kv.second.Stat.BlobCount + kv.second.BlobsSkippedByCompaction,
                kv.second.Stat.BlockCount + kv.second.BlocksSkippedByCompaction
            );
            State.GetCompactionMap().Update(
                kv.first,
                kv.second.Stat.BlobCount + kv.second.BlobsSkippedByCompaction,
                kv.second.Stat.BlockCount + kv.second.BlocksSkippedByCompaction,
                usedBlockCount,
                Args.Mode == ADD_COMPACTION_RESULT
            );
        }
    }

    void UpdateUsedFreshBlocks(TPartitionDatabase& db)
    {
        TVector<ui32> setBlocks;
        TVector<ui32> unsetBlocks;

        // TODO(NBS-1976): make used blocks map more consistent in
        // terms of fresh blocks from channel
        for (const auto& blob: Args.FreshBlobs) {
            for (const auto& block: blob.Blocks) {
                if (OverwrittenBlocks[block.BlockIndex] != block.CommitId) {
                    continue;
                }

                if (IsDeletionMarker(blob.BlobId)) {
                    unsetBlocks.push_back(block.BlockIndex);
                } else {
                    setBlocks.push_back(block.BlockIndex);
                }
            }
        }

        State.SetUsedBlocks(db, setBlocks);
        State.UnsetUsedBlocks(db, unsetBlocks);
    }

    void UpdateUsedBlocks(TPartitionDatabase& db, const TAddMixedBlob& blob)
    {
        if (IsDeletionMarker(blob.BlobId)) {
            State.UnsetUsedBlocks(db, blob.Blocks);
        } else {
            State.SetUsedBlocks(db, blob.Blocks);
        }
    }

    void UpdateUsedBlocks(TPartitionDatabase& db, const TAddMergedBlob& blob)
    {
        if (IsDeletionMarker(blob.BlobId)) {
            State.UnsetUsedBlocks(db, blob.BlockRange);
        } else {
            State.SetUsedBlocks(db, blob.BlockRange, blob.SkipMask.Count());
        }
    }

    void ProcessAffectedBlobs(TPartitionDatabase& db)
    {
        for (const auto& kv: Args.AffectedBlobs) {
            const auto& blockMask = kv.second.BlockMask.GetRef();
            db.WriteBlockMask(kv.first, blockMask);

            if (IsBlockMaskFull(blockMask, MaxBlocksInBlob)) {
                db.WriteCleanupQueue(kv.first, DeletionCommitId);
                State.GetCleanupQueue().Add({ kv.first, DeletionCommitId });
            }
        }
    }

    // NBS-301: remove blocks from index as soon as possible
    void ProcessAffectedBlocks(TPartitionDatabase& db)
    {
        if (!Args.AffectedBlocks) {
            return;
        }

        TVector<ui64> checkpoints;
        State.GetCheckpoints().GetCommitIds(checkpoints);
        // XXX affected blocks are only supplied via the requests sent by
        // compaction and compaction takes care of building correct affected
        // block lists itself: blocks not overwritten by compaction should not
        // be added to AffectedBlocks
        // But at the same time compaction acquires a cleanup barrier => most of
        // the affected blocks won't be deleted because of this barrier
        State.GetCleanupQueue().GetCommitIds(checkpoints);

        if (!checkpoints) {
            // fast path
            for (const auto& block: Args.AffectedBlocks) {
                State.DeleteMixedBlock(db, block.BlockIndex, block.CommitId);
            }
            return;
        }

        SortUnique(checkpoints, TGreater<ui64>());

        ui32 blockIndex = 0;
        TVector<ui64> commitIds;
        TVector<ui64> garbage;

        auto processGroup = [&] {
            FindGarbageVersions(checkpoints, commitIds, garbage);

            for (ui64 commitId: garbage) {
                State.DeleteMixedBlock(db, blockIndex, commitId);
            }

            commitIds.clear();
            garbage.clear();
        };

        for (const auto& block: Args.AffectedBlocks) {
            if (blockIndex != block.BlockIndex) {
                if (commitIds) {
                    processGroup();
                }
                blockIndex = block.BlockIndex;
            }

            commitIds.push_back(block.CommitId);
        }

        if (commitIds) {
            processGroup();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleAddBlobs(
    const TEvPartitionPrivate::TEvAddBlobsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

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

    ExecuteTx<TAddBlobs>(
        ctx,
        requestInfo,
        msg->CommitId,
        std::move(msg->MixedBlobs),
        std::move(msg->MergedBlobs),
        std::move(msg->FreshBlobs),
        msg->Mode,
        std::move(msg->AffectedBlobs),
        std::move(msg->AffectedBlocks),
        std::move(msg->MixedBlobCompactionInfos),
        std::move(msg->MergedBlobCompactionInfos));
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

    TAddBlobsExecutor executor(
        *State,
        args,
        TabletID(),
        args.DeletionCommitId,
        State->GetMaxBlocksInBlob()
    );
    executor.Execute(ctx, db);
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

}   // namespace NCloud::NBlockStore::NStorage::NPartition
