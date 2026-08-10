#include "blocks_filter.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/algorithm.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

size_t GetRangeCount(ui64 blocksPerRange, size_t blockCount)
{
    Y_ABORT_UNLESS(blocksPerRange);
    return CeilDiv(blockCount, blocksPerRange);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlocksFilter::TBlocksFilter(ui64 tabletId, ui64 blocksPerRange,
                             size_t blockCount)
    : TabletId(tabletId)
    , BlocksPerRange(blocksPerRange)
    , BlockCount(blockCount)
    , BlocksFilter(blockCount)
    , CompactionRangeCommitIds(GetRangeCount(blocksPerRange, blockCount),
                               std::nullopt)
{}

bool TBlocksFilter::MayHaveBlocksInMixedIndex(TBlockRange32 blockRange,
                                              ui64 commitId) const
{
    STORAGE_VERIFY(blockRange.End < BlockCount, TWellKnownEntityTypes::TABLET,
                   TabletId);

    for (size_t blockIndex = blockRange.Start; blockIndex <= blockRange.End;
         ++blockIndex)
    {
        const size_t compactionRangeIndex = blockIndex / BlocksPerRange;
        STORAGE_VERIFY(compactionRangeIndex < CompactionRangeCommitIds.size(),
                       TWellKnownEntityTypes::TABLET, TabletId);

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

bool TBlocksFilter::BlocksAddedToMixedIndex(ui64 blockIndex, ui64 commitId)
{
    STORAGE_VERIFY(blockIndex < BlockCount, TWellKnownEntityTypes::TABLET,
                   TabletId);

    const ui32 compactionRangeIndex = blockIndex / BlocksPerRange;
    STORAGE_VERIFY(compactionRangeIndex < CompactionRangeCommitIds.size(),
                   TWellKnownEntityTypes::TABLET, TabletId);

    const auto compactionRangeCommitId =
        CompactionRangeCommitIds[compactionRangeIndex];

    // Blocks older than the compaction baseline are not visible at or
    // after that baseline. Tracking them would only introduce false
    // positives in MayHaveBlocksInMixedIndex.
    const bool added =
        (!compactionRangeCommitId || *compactionRangeCommitId <= commitId) &&
        BlocksFilter.Set(blockIndex, blockIndex + 1);

    for (auto& compaction: Compactions) {
        // Compactions are sorted by CommitId in ascending order.
        if (compaction.CommitId > commitId) {
            break;
        }

        const bool hasRangeIndex =
            BinarySearch(compaction.RangeIndices.begin(),
                         compaction.RangeIndices.end(), compactionRangeIndex);

        if (hasRangeIndex) {
            compaction.MixedBlocksAddedDuringCompaction.insert(blockIndex);
        }
    }

    return added;
}

ui64 TBlocksFilter::GetBlocksCount(ui32 rangeIndex) const
{
    STORAGE_VERIFY(rangeIndex < CompactionRangeCommitIds.size(),
                   TWellKnownEntityTypes::TABLET, TabletId);

    const ui64 rangeStart = rangeIndex * BlocksPerRange;
    const ui64 rangeEnd = Min(rangeStart + BlocksPerRange, BlockCount);
    return BlocksFilter.Count(rangeStart, rangeEnd);
}

void TBlocksFilter::CompactionStarted(TVector<ui32> rangeIndices, ui64 commitId)
{
    STORAGE_VERIFY(
        Compactions.empty() || Compactions.back().CommitId < commitId,
        TWellKnownEntityTypes::TABLET, TabletId);

    Sort(rangeIndices);
    for (ui32 compactionRangeIndex: rangeIndices) {
        STORAGE_VERIFY(compactionRangeIndex < CompactionRangeCommitIds.size(),
                       TWellKnownEntityTypes::TABLET, TabletId);
    }

    Compactions.push_back({.RangeIndices = std::move(rangeIndices),
                           .CommitId = commitId,
                           .MixedBlocksAddedDuringCompaction = {}});
}

void TBlocksFilter::CompactionFinished()
{
    STORAGE_VERIFY(!Compactions.empty(), TWellKnownEntityTypes::TABLET,
                   TabletId);

    auto& compaction = Compactions.front();
    for (ui32 compactionRangeIndex: compaction.RangeIndices) {
        STORAGE_VERIFY(compactionRangeIndex < CompactionRangeCommitIds.size(),
                       TWellKnownEntityTypes::TABLET, TabletId);

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

void TBlocksFilter::CompactionFailed()
{
    STORAGE_VERIFY(!Compactions.empty(), TWellKnownEntityTypes::TABLET,
                   TabletId);

    Compactions.pop_front();
}

ui64 TBlocksFilter::GetMemoryUsage() const
{
    return BlocksFilter.MemSize() +
           (CompactionRangeCommitIds.capacity() * sizeof(std::optional<ui64>));
}

}   // namespace NCloud::NBlockStore::NStorage
