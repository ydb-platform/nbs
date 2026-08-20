#include "part2_addblobs_logic.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/tablet/gc_logic.h>

#include <library/cpp/containers/dense_hash/dense_hash.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <array>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

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
        return TStringBuilder() << "[" << blocks.front().BlockIndex << ".."
                                << blocks.back().BlockIndex << "]";
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
    const TString DiskId;
    const ui64 DeletionCommitId;
    const ui32 MaxBlocksInBlob;
    TChildLogTitle LogTitle;

    struct TRangeInfo
    {
        TRangeStat Stat;
        ui32 BlobsSkippedByCompaction = 0;
        ui32 BlocksSkippedByCompaction = 0;
    };

    TDenseHash<ui32, TRangeInfo> CompactionCounters{
        std::numeric_limits<ui32>::max()};
    TDenseHash<ui32, ui64> OverwrittenBlocks{std::numeric_limits<ui32>::max()};

    std::array<THashSet<ui32>, 2> ChangedLevelIndexRanges;
    std::array<THashSet<ui32>, 2> ChangedLevelIndexFilterChunks;

public:
    TAddBlobsExecutor(
            TPartitionState& state,
            TTxPartition::TAddBlobs& args,
            ui64 tabletId,
            TString diskId,
            ui64 deletionCommitId,
            ui32 maxBlocksInBlob,
            TChildLogTitle logTitle)
        : State(state)
        , Args(args)
        , TabletId(tabletId)
        , DiskId(std::move(diskId))
        , DeletionCommitId(deletionCommitId)
        , MaxBlocksInBlob(maxBlocksInBlob)
        , LogTitle(std::move(logTitle))
    {}

    void Execute(const TActorSystem* actorSystem, TPartitionDatabase& db)
    {
        if (Args.Mode == ADD_COMPACTION_RESULT) {
            Y_ABORT_UNLESS(
                Args.MixedBlobs.size() == Args.MixedBlobCompactionInfos.size());
        }

        for (ui32 i = 0; i < Args.MixedBlobs.size(); ++i) {
            const auto& blob = Args.MixedBlobs[i];
            ProcessNewBlob(actorSystem, db, blob);
            UpdateCompactionCounters(blob);
            if (Args.Mode == EAddBlobMode::ADD_WRITE_RESULT) {
                UpdateUsedBlocks(db, blob);
            }

            if (Args.Mode == ADD_COMPACTION_RESULT) {
                const auto& cm = State.AccessCompactionMap();
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
            ProcessNewBlob(actorSystem, db, blob);
            UpdateCompactionCounters(blob);
            if (Args.Mode == EAddBlobMode::ADD_WRITE_RESULT) {
                UpdateUsedBlocks(db, blob);
            }

            if (Args.Mode == ADD_COMPACTION_RESULT) {
                const auto& cm = State.AccessCompactionMap();
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
            ProcessNewBlob(actorSystem, db, blob);
            UpdateCompactionCounters(blob);
        }

        for (const auto& blob: Args.L0Blobs) {
            ProcessNewBlob</*TLevel=*/0>(actorSystem, db, blob);

            auto& cm = State.GetCompactionMapL0();
            cm.BlobAdded(blob.BlockIndices, blob.CommitIds, Args.CommitId);
            RegisterLevelIndexBlob(ELevelIndex::L0, cm, blob);
        }

        for (const auto& blob: Args.L1Blobs) {
            ProcessNewBlob</*TLevel=*/1>(actorSystem, db, blob);

            auto& cm = State.GetCompactionMapL1();
            cm.BlobAdded(blob.BlockIndices, blob.CommitIds, Args.CommitId);
            RegisterLevelIndexBlob(ELevelIndex::L1, cm, blob);
        }

        if (Args.Mode == ADD_COMPACTION_RESULT) {
            ProcessAffectedBlobs(db);
            ProcessAffectedBlocks(db);
        }

        if (Args.Mode == ADD_PROMOTE_COMPACTION_RESULT) {
            Y_ABORT_UNLESS(Args.PromoteCompactionSource.Defined());

            for (auto& [blobId, affectedBlob]: Args.AffectedBlobs) {
                Y_ABORT_UNLESS(affectedBlob.BlobMeta.Defined());

                bool inserted = State.GetCleanupQueue().Add(
                    TCleanupQueueItem{
                        blobId,
                        DeletionCommitId,
                        std::move(*affectedBlob.BlobMeta)});

                STORAGE_VERIFY_DEBUG_C(
                    inserted,
                    TWellKnownEntityTypes::TABLET,
                    TabletId,
                    "Cleanup queue: blob already in cleanup queue");
                if (inserted) {
                    db.WriteCleanupQueue(blobId, DeletionCommitId);
                }
            }

            const ELevelIndex source =
                *Args.PromoteCompactionSource == EPromoteCompactionSource::L0
                    ? ELevelIndex::L0
                    : ELevelIndex::L1;
            auto& compactionMap = GetLevelIndexCompactionMap(source);
            const auto rangeIndices = compactionMap.CompactionFinished();
            RegisterFinishedLevelIndexRanges(
                source,
                compactionMap,
                rangeIndices);
        }

        PersistLevelIndexState(db, ELevelIndex::L0);
        PersistLevelIndexState(db, ELevelIndex::L1);
        UpdateCompactionMap(db);

        if (Args.Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
            auto trimFreshLogToCommitId =
                State.AccessMeta().GetTrimFreshLogToCommitId();
            State.AccessMeta().SetTrimFreshLogToCommitId(
                Max(trimFreshLogToCommitId, Args.CommitId));
        } else {
            State.UpdateTrimFreshLogToCommitIdInMeta();
        }

        db.WriteMeta(State.GetMeta());
    }

private:
    static size_t LevelIndex(ELevelIndex level)
    {
        return static_cast<size_t>(level);
    }

    TLevelIndexCompactionMap& GetLevelIndexCompactionMap(ELevelIndex level)
    {
        switch (level) {
            case ELevelIndex::L0:
                return State.GetCompactionMapL0();
            case ELevelIndex::L1:
                return State.GetCompactionMapL1();
        }

        Y_ABORT("Unexpected level index");
    }

    TBlocksFilter& GetLevelIndexBlocksFilter(ELevelIndex level)
    {
        switch (level) {
            case ELevelIndex::L0:
                return State.GetBlocksFilterL0();
            case ELevelIndex::L1:
                return State.GetBlocksFilterL1();
        }

        Y_ABORT("Unexpected level index");
    }

    void RegisterLevelIndexBlob(
        ELevelIndex level,
        const TLevelIndexCompactionMap& compactionMap,
        const TAddLevelIndexBlob& blob)
    {
        Y_ABORT_UNLESS(!blob.BlockIndices.empty());

        auto& changedRanges = ChangedLevelIndexRanges[LevelIndex(level)];
        changedRanges.insert(
            compactionMap.GetCompactionMap().GetRangeIndex(
                blob.BlockIndices.front()));

        auto& changedChunks =
            ChangedLevelIndexFilterChunks[LevelIndex(level)];
        for (ui32 blockIndex: blob.BlockIndices) {
            changedChunks.insert(
                blockIndex / TCompressedBitmap::CHUNK_SIZE);
        }
    }

    void RegisterFinishedLevelIndexRanges(
        ELevelIndex level,
        const TLevelIndexCompactionMap& compactionMap,
        const TVector<ui32>& rangeIndices)
    {
        auto& changedRanges = ChangedLevelIndexRanges[LevelIndex(level)];
        auto& changedChunks = ChangedLevelIndexFilterChunks[LevelIndex(level)];

        for (ui32 rangeIndex: rangeIndices) {
            changedRanges.insert(rangeIndex);

            const ui64 rangeStart =
                static_cast<ui64>(rangeIndex) *
                compactionMap.GetCompactionMap().GetRangeSize();
            const ui64 rangeEnd = Min(
                rangeStart + compactionMap.GetCompactionMap().GetRangeSize(),
                State.GetBlocksCount());
            const auto [firstChunk, lastChunk] =
                TCompressedBitmap::ChunkRange(rangeStart, rangeEnd);
            for (ui32 chunkIndex = firstChunk; chunkIndex <= lastChunk;
                 ++chunkIndex)
            {
                changedChunks.insert(chunkIndex);
            }
        }
    }

    void PersistLevelIndexState(TPartitionDatabase& db, ELevelIndex level)
    {
        const auto& compactionMap =
            GetLevelIndexCompactionMap(level).GetCompactionMap();
        const auto& blocksFilter = GetLevelIndexBlocksFilter(level);

        for (ui32 rangeIndex: ChangedLevelIndexRanges[LevelIndex(level)]) {
            const ui64 blockIndex =
                static_cast<ui64>(rangeIndex) * compactionMap.GetRangeSize();
            STORAGE_VERIFY(
                blockIndex <= Max<ui32>(),
                TWellKnownEntityTypes::TABLET,
                TabletId);

            const auto stat = compactionMap.Get(static_cast<ui32>(blockIndex));
            db.WriteLevelIndexRange(
                level,
                rangeIndex,
                stat.BlobCount,
                stat.BlockCount,
                blocksFilter.GetRangeBaselineCommitId(rangeIndex));
        }

        for (ui32 chunkIndex: ChangedLevelIndexFilterChunks[LevelIndex(level)])
        {
            const ui64 rangeStart =
                static_cast<ui64>(chunkIndex) * TCompressedBitmap::CHUNK_SIZE;
            const ui64 rangeEnd =
                Min(rangeStart + TCompressedBitmap::CHUNK_SIZE,
                    State.GetBlocksCount());
            STORAGE_VERIFY(
                rangeStart < rangeEnd,
                TWellKnownEntityTypes::TABLET,
                TabletId);

            auto serializer =
                blocksFilter.RangeSerializer(rangeStart, rangeEnd);

            TCompressedBitmap::TSerializedChunk chunk;

            auto hasChunk = serializer.Next(&chunk);
            if (!hasChunk) {
                db.DeleteLevelIndexBlocksFilter(level, chunkIndex);
                continue;
            }

            STORAGE_VERIFY(
                chunk.ChunkIdx == chunkIndex,
                TWellKnownEntityTypes::TABLET,
                TabletId);

            if (TCompressedBitmap::IsZeroChunk(chunk)) {
                db.DeleteLevelIndexBlocksFilter(level, chunkIndex);
            } else {
                db.WriteLevelIndexBlocksFilter(level, chunk);
            }
        }
    }

    void ProcessNewBlob(
        const TActorSystem* actorSystem,
        TPartitionDatabase& db,
        const TAddMixedBlob& blob)
    {
        Y_DEBUG_ABORT_UNLESS(blob.BlobId.CommitId() == Args.CommitId);
        Y_DEBUG_ABORT_UNLESS(!HasDuplicates(blob.Blocks));

        if (actorSystem) {
            LOG_DEBUG(
                *actorSystem,
                TBlockStoreComponents::PARTITION,
                IsDeletionMarker(blob.BlobId)
                    ? "%s Add MixedBlob (zero blocks) @%lu (blob: %s, range: "
                      "%s)"
                    : "%s Add MixedBlob @%lu (blob: %s, range: %s)",
                LogTitle.GetWithTime().c_str(),
                Args.CommitId,
                ToString(MakeBlobId(TabletId, blob.BlobId)).c_str(),
                DescribeRange(blob.Blocks).c_str());
        }

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

        for (ui16 blobOffset = blob.Blocks.size(); blobOffset < MaxBlocksInBlob;
             ++blobOffset)
        {
            blockMask.Set(blobOffset);
        }

        Y_ABORT_UNLESS(!IsBlockMaskFull(blockMask, MaxBlocksInBlob));
        db.WriteBlockMask(blob.BlobId, blockMask);

        // write blocks
        State.WriteMixedBlocks(
            db,
            blob.BlobId,
            blob.Blocks,
            blob.CompactionRangeCount);

        // update counters
        State.IncrementMixedBlobsCount(1);
        if (!IsDeletionMarker(blob.BlobId)) {
            State.IncrementMixedBlocksCount(blob.Blocks.size());
        }
    }

    void ProcessNewBlob(
        const TActorSystem* actorSystem,
        TPartitionDatabase& db,
        const TAddMergedBlob& blob)
    {
        Y_DEBUG_ABORT_UNLESS(blob.BlobId.CommitId() == Args.CommitId);

        if (actorSystem) {
            LOG_DEBUG(
                *actorSystem,
                TBlockStoreComponents::PARTITION,
                IsDeletionMarker(blob.BlobId)
                    ? "%s Add MergedBlob (zero blocks) @%lu (blob: %s, range: "
                      "%s)"
                    : "%s Add MergedBlob @%lu (blob: %s, range: %s)",
                LogTitle.GetWithTime().c_str(),
                Args.CommitId,
                ToString(MakeBlobId(TabletId, blob.BlobId)).c_str(),
                DescribeRange(blob.BlockRange).c_str());
        }

        const auto skipped = blob.SkipMask.Count();
        Y_ABORT_UNLESS(skipped < blob.BlockRange.Size());

        // write blob meta
        NProto::TBlobMeta blobMeta;

        auto& mergedBlocks = *blobMeta.MutableMergedBlocks();
        mergedBlocks.SetStart(blob.BlockRange.Start);
        mergedBlocks.SetEnd(blob.BlockRange.End);
        mergedBlocks.SetSkipped(skipped);
        mergedBlocks.SetCommitId(blob.CommitId);

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
        db.WriteMergedBlocks(
            blob.BlobId,
            blob.BlockRange,
            blob.SkipMask,
            blob.CommitId);

        // update counters
        State.IncrementMergedBlobsCount(1);
        if (!IsDeletionMarker(blob.BlobId)) {
            State.IncrementMergedBlocksCount(blob.BlockRange.Size() - skipped);
        }

        State.ConfirmedBlobsAdded(db, Args.CommitId);
    }

    void ProcessNewBlob(
        const TActorSystem* actorSystem,
        TPartitionDatabase& db,
        const TAddFreshBlob& blob)
    {
        Y_DEBUG_ABORT_UNLESS(blob.BlobId.CommitId() == Args.CommitId);

        // duplicates are allowed
        Y_DEBUG_ABORT_UNLESS(IsSorted(blob.Blocks.begin(), blob.Blocks.end()));

        if (actorSystem) {
            LOG_DEBUG(
                *actorSystem,
                TBlockStoreComponents::PARTITION,
                IsDeletionMarker(blob.BlobId)
                    ? "%s Add FreshBlob (zero blocks) @%lu (blob: %s, range: "
                      "%s)"
                    : "%s Add FreshBlob @%lu (blob: %s, range: %s)",
                LogTitle.GetWithTime().c_str(),
                Args.CommitId,
                ToString(MakeBlobId(TabletId, blob.BlobId)).c_str(),
                DescribeFreshRange(blob.Blocks).c_str());
        }

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

        for (ui16 blobOffset = blob.Blocks.size(); blobOffset < MaxBlocksInBlob;
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
            State.GetCleanupQueue().Add(
                {blob.BlobId, DeletionCommitId, blobMeta});
        }

        // move blocks from FreshBlocks to MixedBlocks
        blobOffset = 0;
        for (const auto& block: blob.Blocks) {
            State.WriteMixedBlock(
                db,
                {blob.BlobId,
                 block.CommitId,
                 block.BlockIndex,
                 blobOffset++,
                 blob.CompactionRangeCount});

            if (block.IsStoredInDb) {
                State.DeleteFreshBlockFromDb(
                    db,
                    block.BlockIndex,
                    block.CommitId);
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

    template <int TLevel>
    void ProcessNewBlob(
        const TActorSystem* actorSystem,
        TPartitionDatabase& db,
        const TAddLevelIndexBlob& blob)
    {
        static_assert(TLevel == 0 || TLevel == 1, "Invalid level");

        NProto::TBlobMeta blobMeta;

        NProto::TBlobMeta::TMixedBlocks* levelBlocks;
        if constexpr (TLevel == 0) {
            levelBlocks = blobMeta.MutableL0Blocks();
        } else {
            levelBlocks = blobMeta.MutableL1Blocks();
        }

        STORAGE_VERIFY(
            blob.BlockIndices.size() == blob.CommitIds.size() ||
                blob.BlockIndices.size() == 0,
            TWellKnownEntityTypes::TABLET,
            TabletId);

        levelBlocks->MutableBlocks()->Assign(
            blob.BlockIndices.begin(),
            blob.BlockIndices.end());
        levelBlocks->MutableCommitIds()->Assign(
            blob.CommitIds.begin(),
            blob.CommitIds.end());

        for (ui32 checksum: blob.Checksums) {
            blobMeta.AddBlockChecksums(checksum);
        }

        TBlockRange32 blockRange = TBlockRange32::MakeClosedInterval(
            blob.BlockIndices.front(),
            blob.BlockIndices.back());

        if (actorSystem) {
            LOG_DEBUG(
                *actorSystem,
                TBlockStoreComponents::PARTITION,
                "Add L0Blob @%lu (blob: %s, range: %s)",
                Args.CommitId,
                ToString(MakeBlobId(TabletId, blob.BlobId)).c_str(),
                DescribeRange(blockRange).c_str());
        }

        if constexpr (TLevel == 0) {
            db.WriteL0Blob(blob.BlobId, blockRange, blobMeta);
        } else {
            db.WriteL1Blob(blob.BlobId, blockRange, blobMeta);
        }

        db.WriteBlobMeta(blob.BlobId, blobMeta);

        if (!IsDeletionMarker(blob.BlobId)) {
            bool added = State.GetGarbageQueue().AddNewBlob(blob.BlobId);
            Y_ABORT_UNLESS(added);
        }

        if constexpr (TLevel == 0) {
            for (size_t i = 0; i < blob.BlockIndices.size(); ++i) {
                State.DeleteFreshBlock(blob.BlockIndices[i], blob.CommitIds[i]);
            }
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
        const auto& cm = State.AccessCompactionMap();
        auto& rangeInfo = CompactionCounters[blockIndex];

        if (!rangeInfo.Stat.BlobCount && Args.Mode != ADD_COMPACTION_RESULT) {
            rangeInfo.Stat = cm.Get(blockIndex);
        }

        return rangeInfo.Stat;
    }

    void UpdateCompactionCounters(const TAddMergedBlob& blob)
    {
        const auto& cm = State.AccessCompactionMap();

        auto range = TBlockRange32::MakeClosedInterval(
            cm.GetRangeStart(blob.BlockRange.Start),
            cm.GetRangeStart(blob.BlockRange.End));

        for (const ui64 blockIndex: xrange(range, cm.GetRangeSize())) {
            auto& rangeStat = AccessRangeStat(blockIndex);

            TCompactionMap::UpdateCompactionCounter(
                rangeStat.BlobCount + 1,
                &rangeStat.BlobCount);

            if (IsDeletionMarker(blob.BlobId)) {
                continue;
            }

            const auto firstBlock =
                Max<ui64>(blockIndex, blob.BlockRange.Start);
            const auto lastBlock = Min<ui64>(
                blockIndex + cm.GetRangeSize() - 1,
                blob.BlockRange.End);
            ui32 skipped = 0;
            for (ui64 b = firstBlock; b <= lastBlock; ++b) {
                auto pos = b - blob.BlockRange.Start;
                if (blob.SkipMask.Get(pos)) {
                    ++skipped;
                }
            }
            TCompactionMap::UpdateCompactionCounter(
                rangeStat.BlockCount + (lastBlock - firstBlock + 1 - skipped),
                &rangeStat.BlockCount);
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
        const auto& cm = State.AccessCompactionMap();

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
                    &rangeStat->BlobCount);
            }

            if (!IsDeletionMarker(blob.BlobId)) {
                TCompactionMap::UpdateCompactionCounter(
                    rangeStat->BlockCount + 1,
                    &rangeStat->BlockCount);
            }
        }
    }

    void UpdateCompactionMap(TPartitionDatabase& db)
    {
        for (const auto& kv: CompactionCounters) {
            const auto usedBlockCount = State.GetUsedBlocks().Count(
                kv.first,
                Min(static_cast<ui64>(
                        kv.first + State.AccessCompactionMap().GetRangeSize()),
                    State.GetUsedBlocks().Capacity()));

            ui32 newlyZeroedBlocks = 0;

            if (Args.Mode != ADD_COMPACTION_RESULT) {
                newlyZeroedBlocks =
                    State.CalculateNewlyZeroedBlocks(kv.first, usedBlockCount);
            }

            const ui32 prevNewlyZeroedBlocks =
                State.AccessCompactionMap().Get(kv.first).NewlyZeroedBlocks;
            const i64 newlyZeroedBlocksDiff =
                static_cast<i64>(newlyZeroedBlocks) - prevNewlyZeroedBlocks;
            State.SetNewlyZeroedBlocks(
                static_cast<ui32>(std::max(
                    static_cast<i64>(State.GetNewlyZeroedBlocks()) +
                        newlyZeroedBlocksDiff,
                    0L)));

            db.WriteCompactionMap(
                kv.first,
                kv.second.Stat.BlobCount + kv.second.BlobsSkippedByCompaction,
                kv.second.Stat.BlockCount +
                    kv.second.BlocksSkippedByCompaction);
            State.AccessCompactionMap().Update(
                kv.first,
                kv.second.Stat.BlobCount + kv.second.BlobsSkippedByCompaction,
                kv.second.Stat.BlockCount + kv.second.BlocksSkippedByCompaction,
                usedBlockCount,
                newlyZeroedBlocks,
                Args.Mode == ADD_COMPACTION_RESULT);
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
            STORAGE_VERIFY_C(
                kv.second.BlockMask.Defined(),
                TWellKnownEntityTypes::TABLET,
                TabletId,
                "unknown block mask for blob "
                    << MakeBlobId(TabletId, kv.first));

            const auto& blockMask = kv.second.BlockMask.GetRef();
            db.WriteBlockMask(kv.first, blockMask);

            if (IsBlockMaskFull(blockMask, MaxBlocksInBlob)) {
                NProto::TBlobMeta blobMeta;
                if (kv.second.BlobMeta) {
                    blobMeta = kv.second.BlobMeta.GetRef();
                } else if (kv.second.RecreatedBlobMeta) {
                    blobMeta = kv.second.RecreatedBlobMeta.GetRef();
                }

                bool inserted = State.GetCleanupQueue().Add(
                    {kv.first, DeletionCommitId, std::move(blobMeta)});

                STORAGE_VERIFY_DEBUG_C(
                    inserted,
                    TWellKnownEntityTypes::TABLET,
                    TabletId,
                    "Cleanup queue: blob already in cleanup queue");
                if (inserted) {
                    db.WriteCleanupQueue(kv.first, DeletionCommitId);
                }
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

        auto processGroup = [&]
        {
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

void ExecuteAddBlobsTransaction(
    const TActorSystem* actorSystem,
    TChildLogTitle logTitle,
    ui64 tabletId,
    TString diskId,
    ui64 deletionCommitId,
    ui32 maxBlocksInBlob,
    TPartitionDatabase& db,
    TTxPartition::TAddBlobs& args,
    TPartitionState& state)
{
    TAddBlobsExecutor executor(
        state,
        args,
        tabletId,
        std::move(diskId),
        deletionCommitId,
        maxBlocksInBlob,
        std::move(logTitle));
    executor.Execute(actorSystem, db);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
