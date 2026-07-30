#include "mixed_index_blocks_filter.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/algorithm.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TMixedBlocksFilter::TMixedBlocksFilter(
    ui64 tabletId,
    ui64 blocksPerRange,
    size_t blockCount)
    : TabletId(tabletId)
    , BlocksPerRange(blocksPerRange)
    , BlockCount(blockCount)
    , BlocksFilter(blockCount)
    , CompactionRangeCommitIds(
          CeilDiv(blockCount, blocksPerRange),
          std::nullopt)
{}

bool TMixedBlocksFilter::MayHaveBlocksInMixedIndex(
    TBlockRange32 blockRange,
    ui64 commitId) const
{
    STORAGE_VERIFY(
        blockRange.End < BlockCount,
        TWellKnownEntityTypes::TABLET,
        TabletId);

    for (size_t blockIndex = blockRange.Start; blockIndex <= blockRange.End;
         ++blockIndex)
    {
        const size_t compactionRangeIndex = blockIndex / BlocksPerRange;
        STORAGE_VERIFY(
            compactionRangeIndex < CompactionRangeCommitIds.size(),
            TWellKnownEntityTypes::TABLET,
            TabletId);

        const bool hasBlocksInMixedIndex = BlocksFilter.Test(blockIndex);
        const auto compactionRangeCommitId =
            CompactionRangeCommitIds[compactionRangeIndex];
        if (!compactionRangeCommitId) {
            return true;
        }

        // We don't know anything about the mixed blocks with commitId older
        // than the compaction range commitId. So in this case we assume that
        // the mixed blocks are present.
        if (hasBlocksInMixedIndex || *compactionRangeCommitId > commitId) {
            return true;
        }
    }

    return false;
}

void TMixedBlocksFilter::BlocksAddedToMixedIndex(
    TBlockRange32 blockRange,
    ui64 commitId)
{
    STORAGE_VERIFY(
        blockRange.End < BlockCount,
        TWellKnownEntityTypes::TABLET,
        TabletId);

    const ui64 endBlockIndex = static_cast<ui64>(blockRange.End) + 1;
    for (ui64 rangeStart = blockRange.Start; rangeStart < endBlockIndex;) {
        const ui32 compactionRangeIndex = rangeStart / BlocksPerRange;
        const ui64 rangeEnd =
            Min(endBlockIndex, (compactionRangeIndex + 1) * BlocksPerRange);
        STORAGE_VERIFY(
            compactionRangeIndex < CompactionRangeCommitIds.size(),
            TWellKnownEntityTypes::TABLET,
            TabletId);

        const auto compactionRangeCommitId =
            CompactionRangeCommitIds[compactionRangeIndex];

        // Blocks older than the compaction baseline are not visible at or
        // after that baseline. Tracking them would only introduce false
        // positives in MayHaveBlocksInMixedIndex.
        if (!compactionRangeCommitId || *compactionRangeCommitId <= commitId) {
            BlocksFilter.Set(rangeStart, rangeEnd);
        }

        rangeStart = rangeEnd;
    }

    for (auto& compaction: Compactions) {
        // Compactions are sorted by CommitId in ascending order.
        if (compaction.CommitId > commitId) {
            break;
        }

        for (ui64 rangeStart = blockRange.Start; rangeStart < endBlockIndex;) {
            const ui32 compactionRangeIndex = rangeStart / BlocksPerRange;
            const ui64 rangeEnd =
                Min(endBlockIndex, (compactionRangeIndex + 1) * BlocksPerRange);
            const bool hasRangeIndex = BinarySearch(
                compaction.RangeIndices.begin(),
                compaction.RangeIndices.end(),
                compactionRangeIndex);

            if (hasRangeIndex) {
                for (ui64 i = rangeStart; i < rangeEnd; ++i) {
                    compaction.MixedBlocksAddedDuringCompaction.insert(
                        static_cast<ui32>(i));
                }
            }

            rangeStart = rangeEnd;
        }
    }
}

void TMixedBlocksFilter::CompactionStarted(
    TVector<ui32> rangeIndices,
    ui64 commitId)
{
    STORAGE_VERIFY(
        Compactions.empty() || Compactions.back().CommitId < commitId,
        TWellKnownEntityTypes::TABLET,
        TabletId);

    Sort(rangeIndices);
    for (ui32 compactionRangeIndex: rangeIndices) {
        STORAGE_VERIFY(
            compactionRangeIndex < CompactionRangeCommitIds.size(),
            TWellKnownEntityTypes::TABLET,
            TabletId);
    }

    Compactions.push_back(
        {.RangeIndices = std::move(rangeIndices),
         .CommitId = commitId,
         .MixedBlocksAddedDuringCompaction = {}});
}

void TMixedBlocksFilter::CompactionFinished()
{
    STORAGE_VERIFY(
        !Compactions.empty(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    auto& compaction = Compactions.front();
    for (ui32 compactionRangeIndex: compaction.RangeIndices) {
        STORAGE_VERIFY(
            compactionRangeIndex < CompactionRangeCommitIds.size(),
            TWellKnownEntityTypes::TABLET,
            TabletId);

        CompactionRangeCommitIds[compactionRangeIndex] = compaction.CommitId;
        // All mixed blocks in the compaction range should be compacted. So we
        // can clear its filter and restore only blocks added at or after the
        // compaction commit ID.
        BlocksFilter.Unset(
            compactionRangeIndex * BlocksPerRange,
            Min((compactionRangeIndex + 1) * BlocksPerRange, BlockCount));
    }

    for (ui32 blockIndex: compaction.MixedBlocksAddedDuringCompaction) {
        BlocksFilter.Set(blockIndex, blockIndex + 1);
    }

    Compactions.pop_front();
}

void TMixedBlocksFilter::CompactionFailed()
{
    STORAGE_VERIFY(
        !Compactions.empty(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    Compactions.pop_front();
}

ui64 TMixedBlocksFilter::GetMemoryUsage() const
{
    return BlocksFilter.MemSize() +
           (CompactionRangeCommitIds.capacity() * sizeof(std::optional<ui64>));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
