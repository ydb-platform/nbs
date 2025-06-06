#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TCompactionMapLoadState
{
public:
    TCompactionMapLoadState(
        ui32 maxRangesPerChunk,
        ui32 maxOutOfOrderChunksInflight);

    TBlockRange32 LoadNextChunk();
    TBlockRangeSet32 GetNotLoadedRanges(
        const THashSet<ui32>& rangeIndices) const;
    void EnqueueOutOfOrderRanges(const TBlockRangeSet32& ranges);
    void OnRangeLoaded(TBlockRange32 range);

protected:
    bool IsRangeLoaded(TBlockRange32 range) const;
    bool IsRangeLoading(TBlockRange32 range) const;
    void EnqueueOutOfOrderRange(TBlockRange32 range);

protected:
    const ui32 MaxRangesPerChunk = 0;
    const ui32 MaxOutOfOrderChunksInflight = 0;

    ui32 NextRangeIndex = 0;
    TBlockRange32 LoadingRange;
    TBlockRangeSet32 OutOfOrderRanges;
    TBlockRangeSet32 LoadedOutOfOrderRanges;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
