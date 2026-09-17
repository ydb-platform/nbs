#pragma once

#include <cloud/blockstore/libs/storage/core/compaction_map.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TCompactionStatsTracker
{
    struct TCompaction
    {
        ui64 CommitId;

        // Sorted by range index
        TVector<ui32> RangeIndices;
        TVector<TCompactionCounter> CountersForRangeIndices;
    };

private:
    const ui64 TabletId;

    TCompactionMap& CompactionMap;
    TCompressedBitmap& UsedBlocks;
    std::optional<TCompaction> CurrentCompaction;

public:
    TCompactionStatsTracker(
        ui64 tabletId,
        TCompactionMap& compactionMap,
        TCompressedBitmap& usedBlocks);

    [[nodiscard]] TCompactionCounter* AccessCompactionCounter(ui32 rangeIdx);

    [[nodiscard]] bool HasCompaction() const;

    void StartCompaction(ui64 commitId, TVector<ui32> rangeIndices);
    void ResetCompaction();
    TVector<ui32> FinishCompaction();
    void AbortCompaction();

private:
    void VerifyCompactionIsActive() const;

    static TCompactionCounter* FindCounterForRange(
        TCompaction& compaction,
        ui32 rangeIdx);
};

};   // namespace NCloud::NBlockStore::NStorage::NPartition
