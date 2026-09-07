

#pragma once

#include <cloud/blockstore/libs/storage/core/compaction_map.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TCompactionStatsTracker
{
    struct TCompaction
    {
        // Sorted by range index
        TVector<ui32> RangeIndices;
        TVector<TCompactionCounter> CountersForRangeIndices;
    };

private:
    const ui64 TabletId;

    TCompactionMap& CompactionMap;
    TCompressedBitmap& UsedBlocks;
    THashMap<ui64, TCompaction> CommitIdToCompaction;

public:
    TCompactionStatsTracker(
        ui64 tabletId,
        TCompactionMap& compactionMap,
        TCompressedBitmap& usedBlocks);

    [[nodiscard]] TCompactionCounter* AccessCompactionCounter(
        ui64 commitId,
        ui32 rangeIdx);
    [[nodiscard]] TVector<TCompactionCounter*> AccessCompactionCounters(
        ui32 rangeIdx);

    [[nodiscard]] bool HasCompaction() const;

    void CompactionStarted(ui64 commitId, TVector<ui32> rangeIndices);
    void ClearCountersForCompaction(ui64 commitId);
    TVector<ui32> FinishRangeCompaction(ui64 commitId);
    void CompactionFailed(ui64 commitId);

private:
    static TCompactionCounter* FindCounterForRange(
        TCompaction& compaction,
        ui32 rangeIdx);

    TCompaction& AccessCompaction(ui64 commitId);
};

};   // namespace NCloud::NBlockStore::NStorage::NPartition
