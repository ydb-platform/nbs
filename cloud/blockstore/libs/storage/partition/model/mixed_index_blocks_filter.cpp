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
    , Blocks(blockCount)
    , CommitIdsPerRange(CeilDiv(blockCount, blocksPerRange), std::nullopt)
{}

bool TMixedBlocksFilter::MayHaveBlocksInMixedIndex(
    TBlockRange32 range,
    ui64 commitId) const
{
    STORAGE_VERIFY(
        range.End < BlockCount,
        TWellKnownEntityTypes::TABLET,
        TabletId);

    for (size_t blockIndex = range.Start; blockIndex <= range.End; ++blockIndex)
    {
        const size_t rangeIndex = blockIndex / BlocksPerRange;
        const bool hasBlocksInMixedIndex = Blocks.Test(blockIndex);
        const auto rangeCommitId = CommitIdsPerRange[rangeIndex];
        if (!rangeCommitId) {
            return true;
        }

        if (hasBlocksInMixedIndex || *rangeCommitId > commitId) {
            return true;
        }
    }

    return false;
}

void TMixedBlocksFilter::BlocksAddedToMixedIndex(
    ui32 blockIndex,
    ui32 blockCount,
    ui64 commitId)
{
    STORAGE_VERIFY(
        static_cast<ui64>(blockIndex) + blockCount <= BlockCount,
        TWellKnownEntityTypes::TABLET,
        TabletId);

    const ui64 endBlockIndex = static_cast<ui64>(blockIndex) + blockCount;
    for (ui64 rangeStart = blockIndex; rangeStart < endBlockIndex;) {
        const ui32 rangeIndex = rangeStart / BlocksPerRange;
        const ui64 rangeEnd =
            Min(endBlockIndex, (rangeIndex + 1) * BlocksPerRange);
        const auto rangeCommitId = CommitIdsPerRange[rangeIndex];

        if (!rangeCommitId || *rangeCommitId <= commitId) {
            Blocks.Set(rangeStart, rangeEnd);
        }

        rangeStart = rangeEnd;
    }

    for (auto& compaction: Compactions) {
        // Compactions are sorted by CommitId in ascending order.
        if (compaction.CommitId > commitId) {
            break;
        }

        for (ui64 rangeStart = blockIndex; rangeStart < endBlockIndex;) {
            const ui32 rangeIndex = rangeStart / BlocksPerRange;
            const ui64 rangeEnd =
                Min(endBlockIndex, (rangeIndex + 1) * BlocksPerRange);
            const bool hasRangeIndex = BinarySearch(
                compaction.RangeIndices.begin(),
                compaction.RangeIndices.end(),
                rangeIndex);

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
    for (ui32 rangeIndex: rangeIndices) {
        STORAGE_VERIFY(
            rangeIndex < CommitIdsPerRange.size(),
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
    for (ui32 rangeIndex: compaction.RangeIndices) {
        CommitIdsPerRange[rangeIndex] = compaction.CommitId;
        Blocks.Unset(
            rangeIndex * BlocksPerRange,
            Min((rangeIndex + 1) * BlocksPerRange, BlockCount));
    }

    for (ui32 blockIndex: compaction.MixedBlocksAddedDuringCompaction) {
        Blocks.Set(blockIndex, blockIndex + 1);
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
    return Blocks.MemSize() +
           (CommitIdsPerRange.capacity() * sizeof(std::optional<ui64>));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
