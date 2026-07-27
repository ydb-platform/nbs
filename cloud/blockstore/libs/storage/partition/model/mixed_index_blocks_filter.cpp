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
    , Blocks(blockCount)
    , CommitIdsPerRange(CeilDiv(blockCount, blocksPerRange), std::nullopt)
{}

bool TMixedBlocksFilter::MayHaveBlocksInMixedIndex(
    TBlockRange32 range,
    ui64 commitId) const
{
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

void TMixedBlocksFilter::AddBlocksToMixedIndex(ui32 blockIndex, ui64 commitId)
{
    const ui32 rangeIndex = blockIndex / BlocksPerRange;

    STORAGE_VERIFY(
        rangeIndex < CommitIdsPerRange.size(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    const auto startCommitId = CommitIdsPerRange[rangeIndex];
    if (!startCommitId || *startCommitId <= commitId) {
        Blocks.Set(blockIndex, blockIndex + 1);
    }

    for (auto& compaction: Compactions) {
        // Compactions are sorted by CommitId in ascending order.
        if (compaction.CommitId > commitId) {
            break;
        }

        const bool hasRangeIndex = BinarySearch(
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
    STORAGE_VERIFY(
        Compactions.empty() || Compactions.back().CommitId < commitId,
        TWellKnownEntityTypes::TABLET,
        TabletId);

    Sort(rangeIndices);
    Compactions.push_back(
        {.RangesForCompaction = std::move(rangeIndices),
         .CommitId = commitId,
         .MixedBlocksWrittenAfterCompaction = {}});
}

void TMixedBlocksFilter::CompactionFinished()
{
    STORAGE_VERIFY(
        !Compactions.empty(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

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
    STORAGE_VERIFY(
        !Compactions.empty(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    Compactions.pop_front();
}

void TMixedBlocksFilter::UpdateChunk(TCompressedBitmap::TSerializedChunk chunk)
{
    Blocks.Update(chunk);
}

void TMixedBlocksFilter::UpdateRangeCommitId(ui32 rangeIndex, ui64 commitId)
{
    STORAGE_VERIFY(
        rangeIndex < CommitIdsPerRange.size(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

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
