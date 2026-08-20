#include "level_index_compaction_map.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/algorithm.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

TLevelIndexCompactionMap::TLevelIndexCompactionMap(
    ui64 tabletId,
    ui32 blocksPerRange,
    TBlocksFilter& blocksFilter,
    ICompactionPolicyPtr compactionPolicy)
    : TabletId(tabletId)
    , BlocksPerRange(blocksPerRange)
    , BlocksFilter(blocksFilter)
    , CompactionMap(blocksPerRange, std::move(compactionPolicy))
{
    STORAGE_VERIFY(BlocksPerRange, TWellKnownEntityTypes::TABLET, TabletId);
}

void TLevelIndexCompactionMap::BlobAdded(
    const TVector<ui32>& blockIndices,
    const TVector<ui64>& commitIds,
    ui64 commitId)
{
    STORAGE_VERIFY(!blockIndices.empty() && commitIds.size() == blockIndices.size(), TWellKnownEntityTypes::TABLET, TabletId);
    STORAGE_VERIFY(
        IsSorted(blockIndices.begin(), blockIndices.end()),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    const ui32 rangeIndex =
        CompactionMap.GetRangeIndex(blockIndices.front());
    STORAGE_VERIFY(
        rangeIndex == CompactionMap.GetRangeIndex(blockIndices.back()),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    auto stat = CompactionMap.Get(blockIndices.front());
    TCompactionMap::UpdateCompactionCounter(
        stat.BlobCount + 1,
        &stat.BlobCount);
    TCompactionMap::UpdateCompactionCounter(
        stat.BlockCount + blockIndices.size(),
        &stat.BlockCount);

    for (size_t i = 0; i < blockIndices.size(); ++i) {
        if (BlocksFilter.BlocksAddedToMixedIndex(blockIndices[i], commitIds[i])) {
            TCompactionMap::UpdateCompactionCounter(
                stat.UsedBlockCount + 1,
                &stat.UsedBlockCount);
        }
    }

    CompactionMap.Update(
        blockIndices.front(),
        stat.BlobCount,
        stat.BlockCount,
        stat.UsedBlockCount,
        0,        // newlyZeroedBlocks
        false);   // compacted

    for (auto& compaction: Compactions) {
        if (compaction.CommitId > commitId) {
            break;
        }

        if (BinarySearch(
                compaction.RangeIndices.begin(),
                compaction.RangeIndices.end(),
                rangeIndex))
        {
            auto& concurrentStat = compaction.ConcurrentRangeStats[rangeIndex];
            TCompactionMap::UpdateCompactionCounter(
                concurrentStat.BlobCount + 1,
                &concurrentStat.BlobCount);
            TCompactionMap::UpdateCompactionCounter(
                concurrentStat.BlockCount + blockIndices.size(),
                &concurrentStat.BlockCount);
        }
    }
}

void TLevelIndexCompactionMap::CompactionStarted(
    TVector<ui32> rangeIndices,
    ui64 commitId)
{
    STORAGE_VERIFY(
        Compactions.empty() || Compactions.back().CommitId < commitId,
        TWellKnownEntityTypes::TABLET,
        TabletId);

    Sort(rangeIndices);
    STORAGE_VERIFY(
        std::adjacent_find(rangeIndices.begin(), rangeIndices.end()) ==
            rangeIndices.end(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    // TBlocksFilter validates every range index.
    BlocksFilter.CompactionStarted(rangeIndices, commitId);
    Compactions.push_back(
        {.RangeIndices = std::move(rangeIndices),
         .CommitId = commitId,
         .ConcurrentRangeStats = {}});
}

TVector<ui32> TLevelIndexCompactionMap::CompactionFinished()
{
    STORAGE_VERIFY(
        !Compactions.empty(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    BlocksFilter.CompactionFinished();

    auto& compaction = Compactions.front();
    for (ui32 rangeIndex: compaction.RangeIndices) {
        TConcurrentRangeStat concurrentStat;
        if (const auto* stat =
                compaction.ConcurrentRangeStats.FindPtr(rangeIndex))
        {
            concurrentStat = *stat;
        }

        UpdateRange(
            rangeIndex,
            concurrentStat.BlobCount,
            concurrentStat.BlockCount,
            true);   // compacted
    }

    auto rangeIndices = std::move(compaction.RangeIndices);
    Compactions.pop_front();
    return rangeIndices;
}

void TLevelIndexCompactionMap::CompactionFailed()
{
    STORAGE_VERIFY(
        !Compactions.empty(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    BlocksFilter.CompactionFailed();
    Compactions.pop_front();
}

void TLevelIndexCompactionMap::LoadRange(
    ui32 rangeIndex,
    ui32 blobCount,
    ui32 blockCount)
{
    UpdateRange(rangeIndex, blobCount, blockCount, false);   // compacted
}

void TLevelIndexCompactionMap::UpdateRange(
    ui32 rangeIndex,
    ui32 blobCount,
    ui32 blockCount,
    bool compacted)
{
    const ui64 blockIndex = static_cast<ui64>(rangeIndex) * BlocksPerRange;
    STORAGE_VERIFY(
        blockIndex <= Max<ui32>(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    CompactionMap.Update(
        blockIndex,
        blobCount,
        blockCount,
        BlocksFilter.GetBlocksCount(rangeIndex),
        0,   // newlyZeroedBlocks
        compacted);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
