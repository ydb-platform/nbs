#include "mixed_index_blocks_filter.h"

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

    auto* compactions = RangeIndexToCompactionRangeInfos.FindPtr(rangeIndex);
    if (!compactions) {
        return;
    }

    for (auto& compaction: *compactions) {
        if (compaction.CommitId > commitId) {
            break;
        }

        compaction.MixedBlocksWrittenAfterCompaction.insert(blockIndex);
    }
}

void TMixedBlocksFilter::StartCompactionRange(ui32 rangeIndex, ui64 commitId)
{
    auto& compactions = RangeIndexToCompactionRangeInfos[rangeIndex];

    Y_ABORT_UNLESS(
        compactions.empty() || compactions.back().CommitId < commitId);

    compactions.push_back({.CommitId = commitId});
}

void TMixedBlocksFilter::CompactionRangeFinished(ui32 rangeIndex)
{
    auto* compactions = RangeIndexToCompactionRangeInfos.FindPtr(rangeIndex);
    Y_ABORT_UNLESS(compactions);

    CommitIdsPerRange[rangeIndex] = compactions->front().CommitId;
    Blocks.Unset(
        rangeIndex * BlocksPerRange,
        (rangeIndex + 1) * BlocksPerRange);

    for (size_t blockIndex:
         compactions->front().MixedBlocksWrittenAfterCompaction)
    {
        Blocks.Set(blockIndex, blockIndex + 1);
    }

    compactions->pop_front();
    if (compactions->empty()) {
        RangeIndexToCompactionRangeInfos.erase(rangeIndex);
    }
}

void TMixedBlocksFilter::CompactionRangeFailed(ui32 rangeIndex)
{
    auto* compactions = RangeIndexToCompactionRangeInfos.FindPtr(rangeIndex);
    Y_ABORT_UNLESS(compactions);

    compactions->pop_front();
    if (compactions->empty()) {
        RangeIndexToCompactionRangeInfos.erase(rangeIndex);
    }
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
