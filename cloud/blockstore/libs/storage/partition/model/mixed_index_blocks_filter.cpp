#include "mixed_index_blocks_filter.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

TMixedBlocksFilter::TMixedBlocksFilter(ui64 blocksPerRange, size_t blockCount)
    : Blocks(blockCount)
    , StartCommitIdsPerRange(blockCount / blocksPerRange, std::nullopt)
    , BlocksPerRange(blocksPerRange)
{}

bool TMixedBlocksFilter::MayHaveBlocksInMixedIndex(
    TBlockRange32 range,
    ui64 commitId) const
{
    for (ui32 blockIndex = range.Start; blockIndex <= range.End; ++blockIndex) {
        ui32 rangeIndex = blockIndex / BlocksPerRange;
        bool hasBlocksInMixedIndex = Blocks.Test(blockIndex);
        auto startCommitId = StartCommitIdsPerRange[rangeIndex];
        if (!startCommitId) {
            return true;
        }

        if (hasBlocksInMixedIndex || *startCommitId > commitId) {
            return true;
        }
    }

    return false;
}

void TMixedBlocksFilter::AddBlocksToMixedIndex(ui32 blockIndex, ui64 commitId)
{
    ui32 rangeIndex = blockIndex / BlocksPerRange;

    auto startCommitId = StartCommitIdsPerRange[rangeIndex];
    if (!startCommitId || *startCommitId <= commitId) {
        Blocks.Set(blockIndex, blockIndex + 1);
    }

    size_t range = blockIndex / BlocksPerRange;
    auto* compactions = RangeIndexToCompactionRangeInfos.FindPtr(range);
    if (!compactions) {
        return;
    }

    size_t blockIndexInRange = blockIndex % BlocksPerRange;
    for (auto& compaction: *compactions) {
        if (compaction.CommitId > commitId) {
            break;
        }

        compaction.FilterAfterCompaction.Set(
            blockIndexInRange,
            blockIndexInRange + 1);
    }
}

void TMixedBlocksFilter::StartCompactionRange(ui32 rangeIndex, ui64 commitId)
{
    auto& compactions = RangeIndexToCompactionRangeInfos[rangeIndex];

    Y_ABORT_UNLESS(
        compactions.empty() || compactions.back().CommitId < commitId);

    compactions.push_back(
        {.CommitId = commitId,
         .FilterAfterCompaction = TCompressedBitmap(BlocksPerRange)});
}

void TMixedBlocksFilter::CompactionRangeFinished(ui32 rangeIndex)
{
    auto* compactions = RangeIndexToCompactionRangeInfos.FindPtr(rangeIndex);
    Y_ABORT_UNLESS(compactions);

    StartCommitIdsPerRange[rangeIndex] = compactions->front().CommitId;
    Blocks.Unset(
        rangeIndex * BlocksPerRange,
        (rangeIndex + 1) * BlocksPerRange);
    Blocks.Update(
        compactions->front().FilterAfterCompaction,
        rangeIndex * BlocksPerRange);

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
    StartCommitIdsPerRange[rangeIndex] = commitId;
}

ui64 TMixedBlocksFilter::GetMemoryUsage() const
{
    return Blocks.MemSize() +
           (StartCommitIdsPerRange.size() * sizeof(std::optional<ui64>));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
