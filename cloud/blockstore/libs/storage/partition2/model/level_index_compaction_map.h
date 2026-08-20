#pragma once

#include <cloud/blockstore/libs/storage/core/compaction_map.h>
#include <cloud/blockstore/libs/storage/partition_common/model/block.h>
#include <cloud/blockstore/libs/storage/partition_common/model/blocks_filter.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <deque>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

/**
 * Compaction statistics for one level index.
 *
 * BlobCount and BlockCount include every stored version. UsedBlockCount is the
 * number of distinct block indices and is maintained by TBlocksFilter. While a
 * compaction is in flight, blobs added at or after its commit ID are tracked
 * separately so that completing the compaction removes only its input.
 */
class TLevelIndexCompactionMap
{
    struct TConcurrentRangeStat
    {
        ui16 BlobCount = 0;
        ui16 BlockCount = 0;
    };

    struct TCompaction
    {
        // Sorted indices of all ranges processed by this compaction.
        TVector<ui32> RangeIndices;
        ui64 CommitId = 0;
        THashMap<ui32, TConcurrentRangeStat> ConcurrentRangeStats;
    };

private:
    const ui64 TabletId = 0;
    const ui32 BlocksPerRange = 0;

    TBlocksFilter& BlocksFilter;

    TCompactionMap CompactionMap;

    // Compactions are ordered by strictly increasing commit ID.
    std::deque<TCompaction> Compactions;

public:
    TLevelIndexCompactionMap(
        ui64 tabletId,
        ui32 blocksPerRange,
        TBlocksFilter& blocksFilter,
        ICompactionPolicyPtr compactionPolicy);

    /** Records a blob added to this level. Blocks must be sorted by index. */
    void BlobAdded(
        const TVector<ui32>& blockIndices,
        const TVector<ui64>& commitIds,
        ui64 commitId);

    /** Registers a compaction of the specified level ranges. */
    void CompactionStarted(TVector<ui32> rangeIndices, ui64 commitId);

    /**
     * Removes the oldest compaction input from the map and retains blobs added
     * while that compaction was in flight.
     */
    TVector<ui32> CompactionFinished();

    /** Discards the oldest in-flight compaction without changing the map. */
    void CompactionFailed();

    [[nodiscard]] const TCompactionMap& GetCompactionMap() const
    {
        return CompactionMap;
    }

    std::deque<TCompaction>& GetCompactions()
    {
        return Compactions;
    }

    /** Restores one persistent range during tablet startup. */
    void LoadRange(ui32 rangeIndex, ui32 blobCount, ui32 blockCount);

private:
    void UpdateRange(ui32 rangeIndex, ui32 blobCount, ui32 blockCount,
                     bool compacted);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
