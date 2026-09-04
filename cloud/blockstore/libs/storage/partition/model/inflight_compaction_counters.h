

#pragma once

#include <cloud/blockstore/libs/storage/core/compaction_map.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TInflightCompactionCounters
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
    TInflightCompactionCounters(
        ui64 tabletId,
        TCompactionMap& compactionMap, TCompressedBitmap& usedBlocks);

    [[nodiscard]] TVector<TCompactionCounter*> GetCompactionCounters(
        ui32 rangeIdx);

    void CompactionStarted(ui64 commitId, TVector<ui32> rangeIndices);
    void ClearCountersForCompaction(ui64 commitId);
    TVector<ui32> FinishRangeCompaction(ui64 commitId);
    void CompactionFailed(ui64 commitId);
};

};   // namespace NCloud::NBlockStore::NStorage::NPartition
