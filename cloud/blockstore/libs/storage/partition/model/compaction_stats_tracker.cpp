#include "compaction_stats_tracker.h"

#include "cloud/storage/core/libs/common/verify.h"
#include "util/generic/algorithm.h"
#include "util/string/builder.h"

#include <algorithm>

namespace NCloud::NBlockStore::NStorage::NPartition {

TCompactionStatsTracker::TCompactionStatsTracker(
        ui64 tabletId,
        TCompactionMap& compactionMap,
        TCompressedBitmap& usedBlocks)
    : TabletId(tabletId)
    , CompactionMap(compactionMap)
    , UsedBlocks(usedBlocks)
{}

TCompactionCounter* TCompactionStatsTracker::AccessCompactionCounter(
    ui32 rangeIdx)
{
    if (!HasCompaction()) {
        return nullptr;
    }

    return FindCounterForRange(*CurrentCompaction, rangeIdx);
}

bool TCompactionStatsTracker::HasCompaction() const
{
    return CurrentCompaction.has_value();
}

void TCompactionStatsTracker::StartCompaction(
    ui64 commitId,
    TVector<ui32> rangeIndices)
{
    Sort(rangeIndices);

    if (CurrentCompaction && CurrentCompaction->CommitId == commitId) {
        STORAGE_VERIFY(
            CurrentCompaction->RangeIndices == rangeIndices,
            TWellKnownEntityTypes::TABLET,
            TabletId);
        return;
    }

    // Only one compaction can be active at a time
    STORAGE_VERIFY(!CurrentCompaction, TWellKnownEntityTypes::TABLET, TabletId);

    TVector<TCompactionCounter> countersForRangeIndices;
    countersForRangeIndices.reserve(rangeIndices.size());
    for (const ui32 rangeIdx: rangeIndices) {
        countersForRangeIndices.emplace_back(
            rangeIdx * CompactionMap.GetRangeSize(),
            TRangeStat{});
    }

    CurrentCompaction = TCompaction{
        .CommitId = commitId,
        .RangeIndices = std::move(rangeIndices),
        .CountersForRangeIndices = std::move(countersForRangeIndices),
    };
}

void TCompactionStatsTracker::ResetCompaction()
{
    VerifyCompactionIsActive();
    for (auto& counter: CurrentCompaction->CountersForRangeIndices) {
        counter.Stat = TRangeStat();
    }
}

TVector<ui32> TCompactionStatsTracker::FinishCompaction()
{
    VerifyCompactionIsActive();

    for (auto& counter: CurrentCompaction->CountersForRangeIndices) {
        ui32 usedBlockCount = UsedBlocks.Count(
            counter.BlockIndex,
            Min(static_cast<ui64>(
                    counter.BlockIndex + CompactionMap.GetRangeSize()),
                UsedBlocks.Capacity()));

        CompactionMap.Update(
            counter.BlockIndex,
            counter.Stat.BlobCount,
            counter.Stat.BlockCount,
            usedBlockCount,
            counter.Stat.NewlyZeroedBlocks,
            counter.Stat.MixedBlockCount,
            true);   // compacted
    }

    TVector<ui32> rangeIndices = std::move(CurrentCompaction->RangeIndices);

    CurrentCompaction.reset();

    return rangeIndices;
}

void TCompactionStatsTracker::AbortCompaction()
{
    VerifyCompactionIsActive();
    CurrentCompaction.reset();
}

void TCompactionStatsTracker::VerifyCompactionIsActive() const
{
    STORAGE_VERIFY(HasCompaction(), TWellKnownEntityTypes::TABLET, TabletId);
}

TCompactionCounter* TCompactionStatsTracker::FindCounterForRange(
    TCompaction& compaction,
    ui32 rangeIdx)
{
    auto* it = std::ranges::lower_bound(compaction.RangeIndices, rangeIdx);
    if (it == compaction.RangeIndices.end() || *it != rangeIdx) {
        return nullptr;
    }
    auto indexInCountersArray =
        std::distance(compaction.RangeIndices.begin(), it);
    return &compaction.CountersForRangeIndices[indexInCountersArray];
}

};   // namespace NCloud::NBlockStore::NStorage::NPartition
