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
    ui64 commitId,
    ui32 rangeIdx)
{
    auto* compaction = CommitIdToCompaction.FindPtr(commitId);

    STORAGE_VERIFY_C(
        compaction,
        TWellKnownEntityTypes::TABLET,
        TabletId,
        TStringBuilder() << "Compaction with commit id " << commitId
                         << " not found");

    auto* counter = FindCounterForRange(*compaction, rangeIdx);
    STORAGE_VERIFY_C(
        counter,
        TWellKnownEntityTypes::TABLET,
        TabletId,
        TStringBuilder() << "Counter for range index " << rangeIdx
                         << " not found in compaction with commit id "
                         << commitId);
    return counter;
}

TVector<TCompactionCounter*> TCompactionStatsTracker::AccessCompactionCounters(
    ui32 rangeIdx)
{
    TVector<TCompactionCounter*> counters;
    for (auto& [commitId, compaction]: CommitIdToCompaction) {
        Y_UNUSED(commitId);

        auto* counter = FindCounterForRange(compaction, rangeIdx);
        if (counter) {
            counters.push_back(counter);
        }
    }
    return counters;
}

bool TCompactionStatsTracker::HasCompaction() const
{
    return !CommitIdToCompaction.empty();
}

void TCompactionStatsTracker::CompactionStarted(
    ui64 commitId,
    TVector<ui32> rangeIndices)
{
    if (!IsSorted(rangeIndices.begin(), rangeIndices.end())) {
        Sort(rangeIndices);
    }
    TVector<TCompactionCounter> countersForRangeIndices;
    countersForRangeIndices.reserve(rangeIndices.size());
    for (const ui32 rangeIdx: rangeIndices) {
        countersForRangeIndices.emplace_back(
            rangeIdx * CompactionMap.GetRangeSize(),
            TRangeStat{});
    }

    TCompaction compaction{
        .RangeIndices = std::move(rangeIndices),
        .CountersForRangeIndices = std::move(countersForRangeIndices),
    };

    auto [it, inserted] =
        CommitIdToCompaction.insert({commitId, std::move(compaction)});
    STORAGE_VERIFY_C(
        inserted,
        TWellKnownEntityTypes::TABLET,
        TabletId,
        TStringBuilder() << "Compaction with commit id " << commitId
                         << " already exists");
}

void TCompactionStatsTracker::ClearCountersForCompaction(ui64 commitId)
{
    auto& compaction = AccessCompaction(commitId);
    for (auto& counter: compaction.CountersForRangeIndices) {
        counter.Stat = TRangeStat();
    }
}

TVector<ui32> TCompactionStatsTracker::FinishRangeCompaction(ui64 commitId)
{
    auto& compaction = AccessCompaction(commitId);

    for (auto& counter: compaction.CountersForRangeIndices) {
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

    TVector<ui32> rangeIndices = std::move(compaction.RangeIndices);

    CommitIdToCompaction.erase(commitId);

    return rangeIndices;
}

void TCompactionStatsTracker::CompactionFailed(ui64 commitId)
{
    size_t erased = CommitIdToCompaction.erase(commitId);
    STORAGE_VERIFY_C(
        erased == 1,
        TWellKnownEntityTypes::TABLET,
        TabletId,
        TStringBuilder() << "Compaction with commit id " << commitId
                         << " not found");
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

auto TCompactionStatsTracker::AccessCompaction(ui64 commitId) -> TCompaction&
{
    auto it = CommitIdToCompaction.find(commitId);
    STORAGE_VERIFY_C(
        it != CommitIdToCompaction.end(),
        TWellKnownEntityTypes::TABLET,
        TabletId,
        TStringBuilder() << "Compaction with commit id " << commitId
                         << " not found");
    return it->second;
}

};   // namespace NCloud::NBlockStore::NStorage::NPartition
