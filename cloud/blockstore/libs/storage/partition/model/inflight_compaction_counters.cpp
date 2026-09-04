#include "inflight_compaction_counters.h"

#include "cloud/storage/core/libs/common/verify.h"
#include "util/generic/algorithm.h"
#include "util/string/builder.h"

#include <algorithm>

namespace NCloud::NBlockStore::NStorage::NPartition {

TInflightCompactionCounters::TInflightCompactionCounters(
    ui64 tabletId, TCompactionMap& compactionMap, TCompressedBitmap& usedBlocks)
    : TabletId(tabletId)
    , CompactionMap(compactionMap)
    , UsedBlocks(usedBlocks)
{}

TVector<TCompactionCounter*> TInflightCompactionCounters::GetCompactionCounters(
    ui32 rangeIdx)
{
    TVector<TCompactionCounter*> counters;
    for (auto& [commitId, compaction]: CommitIdToCompaction) {
        Y_UNUSED(commitId);

        auto it = std::ranges::lower_bound(compaction.RangeIndices, rangeIdx);
        if (it == compaction.RangeIndices.end() || *it != rangeIdx) {
            continue;
        }
        auto indexInCountersArray =
            std::distance(compaction.RangeIndices.begin(), it);
        counters.push_back(
            &compaction.CountersForRangeIndices[indexInCountersArray]);
    }
    return counters;
}

void TInflightCompactionCounters::CompactionStarted(
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

void TInflightCompactionCounters::ClearCountersForCompaction(ui64 commitId)
{
    auto it = CommitIdToCompaction.find(commitId);
    STORAGE_VERIFY_C(
        it != CommitIdToCompaction.end(),
        TWellKnownEntityTypes::TABLET,
        TabletId,
        TStringBuilder() << "Compaction with commit id " << commitId
                         << " not found");

    auto& compaction = it->second;
    for (auto& counter: compaction.CountersForRangeIndices) {
        counter.Stat = TRangeStat();
    }
}

TVector<ui32> TInflightCompactionCounters::FinishRangeCompaction(ui64 commitId)
{
    auto it = CommitIdToCompaction.find(commitId);
    STORAGE_VERIFY_C(
        it != CommitIdToCompaction.end(),
        TWellKnownEntityTypes::TABLET,
        TabletId,
        TStringBuilder() << "Compaction with commit id " << commitId
                         << " not found");

    auto& compaction = it->second;
    for (auto& counter: compaction.CountersForRangeIndices) {
        counter.Stat.Compacted = true;
    }
    CompactionMap.Update(compaction.CountersForRangeIndices, &UsedBlocks);

    TVector<ui32> rangeIndices = std::move(compaction.RangeIndices);

    CommitIdToCompaction.erase(it);

    return rangeIndices;
}

void TInflightCompactionCounters::CompactionFailed(ui64 commitId)
{
    auto it = CommitIdToCompaction.find(commitId);
    STORAGE_VERIFY_C(
        it != CommitIdToCompaction.end(),
        TWellKnownEntityTypes::TABLET,
        TabletId,
        TStringBuilder() << "Compaction with commit id " << commitId
                         << " not found");
    CommitIdToCompaction.erase(it);
}

// class TInflightCompactionCounters
// {
//     struct TCompaction
//     {
//         ui64 CommitId;
//         TVector<std::pair<ui32, TCompactionCounter>> RangeIndexToCounter;
//     };

// private:

//     TCompactionMap& CompactionMap;
//     THashMap<ui64, TCompaction> CommitIdToCompaction;

// public:
//     explicit TInflightCompactionCounters(TCompactionMap& compactionMap);

//     [[nodiscard]] TVector<TCompactionCounter*> GetCompactionCounters(
//         ui64 commitId) const;

//     void StartRangeCompaction(ui64 commitId, TVector<ui32> rangeIndices);
//     void FinishRangeCompaction(ui64 commitId);
//     void FailRangeCompaction(ui64 commitId);
// };

};   // namespace NCloud::NBlockStore::NStorage::NPartition
