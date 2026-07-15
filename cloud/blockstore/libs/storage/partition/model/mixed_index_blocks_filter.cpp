#include "mixed_index_blocks_filter.h"
#include "util/generic/algorithm.h"

#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

TMixedBlocksFilter::TMixedBlocksFilter(ui64 blocksPerRange, size_t blockCount)
    : Blocks(blockCount)
    , CommitIdsPerRange(CeilDiv(blockCount, blocksPerRange), std::nullopt)
    , BlocksPerRange(blocksPerRange)
{}

bool TMixedBlocksFilter::MayHaveBlocksInMixedIndex(
    TBlockRange32 range,
    ui64 commitId) const
{
    for (size_t blockIndex = range.Start; blockIndex <= range.End; ++blockIndex)
    {
        size_t rangeIndex = blockIndex / BlocksPerRange;
        bool hasBlocksInMixedIndex = Blocks.Test(blockIndex);
        auto rangeCommitId = CommitIdsPerRange[rangeIndex];
        if (!rangeCommitId) {
            return true;
        }

        if (hasBlocksInMixedIndex || *rangeCommitId > commitId) {
            return true;
        }
    }

    return false;
}

void TMixedBlocksFilter::AddBlocksToMixedIndex(ui32 blockIndex, ui64 commitId)
{
    ui32 rangeIndex = blockIndex / BlocksPerRange;

    auto startCommitId = CommitIdsPerRange[rangeIndex];
    if (!startCommitId || *startCommitId <= commitId) {
        Blocks.Set(blockIndex, blockIndex + 1);
    }

    for (auto& compaction: Compactions) {
        // Compactions are sorted by CommitId in ascending order.
        if (compaction.CommitId > commitId) {
            break;
        }

        auto hasRangeIndex = BinarySearch(
            compaction.RangesForCompaction.begin(),
            compaction.RangesForCompaction.end(),
            rangeIndex);

        if (hasRangeIndex) {
            compaction.MixedBlocksWrittenAfterCompaction.insert(blockIndex);
        }
    }
}

void TMixedBlocksFilter::StartCompaction(
    TVector<ui32> rangeIndices,
    ui64 commitId)
{
    Y_ABORT_UNLESS(
        Compactions.empty() || Compactions.back().CommitId < commitId);
    Sort(rangeIndices);
    Compactions.push_back(
        {.RangesForCompaction = std::move(rangeIndices),
         .CommitId = commitId,
         .MixedBlocksWrittenAfterCompaction = {}});
}

void TMixedBlocksFilter::CompactionFinished()
{
    Y_ABORT_UNLESS(!Compactions.empty());
    auto& compaction = Compactions.front();
    for (auto& rangeIndex: compaction.RangesForCompaction) {
        CommitIdsPerRange[rangeIndex] = compaction.CommitId;
        Blocks.Unset(
            rangeIndex * BlocksPerRange,
            (rangeIndex + 1) * BlocksPerRange);
    }

    for (size_t blockIndex: compaction.MixedBlocksWrittenAfterCompaction) {
        Blocks.Set(blockIndex, blockIndex + 1);
    }

    Compactions.pop_front();
}

void TMixedBlocksFilter::CompactionFailed()
{
    Y_ABORT_UNLESS(!Compactions.empty());
    Compactions.pop_front();
}

void TMixedBlocksFilter::UpdateChunk(TCompressedBitmap::TSerializedChunk chunk)
{
    Blocks.Update(chunk);
}

void TMixedBlocksFilter::UpdateRangeCommitId(ui32 rangeIndex, ui64 commitId)
{
    CommitIdsPerRange[rangeIndex] = commitId;
}

ui64 TMixedBlocksFilter::GetMemoryUsage() const
{
    // RangeIndexToCompactionRangeInfos is not included in the memory usage
    // because it usualy takes small amount of memory, because maximum number of
    // ranges during compaction is bounded.
    return Blocks.MemSize() +
           (CommitIdsPerRange.size() * sizeof(std::optional<ui64>));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
