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
        ui32 maxRangesPerTx,
        ui32 maxOutOfOrderChunksInflight);

    const TBlockRange32& LoadNextChunk();
    // return true if any of rangeIndices are not loaded yet
    bool EnqueueOutOfOrderRanges(const THashSet<ui32>& rangeIndices);
    void RangeIsLoaded(const TBlockRange32& range);

protected:
    bool IsRangeLoaded(const TBlockRange32& range) const;
    bool IsRangeInLoadingQueue(const TBlockRange32& range) const;
    void EnqueueOutOfOrderRange(const TBlockRange32& range);

protected:
    const ui32 MaxRangesPerTx = 0;
    const ui32 MaxOutOfOrderChunksInflight = 0;

    ui32 NextRangeIndex = 0;
    TBlockRange32 LoadingRange;
    TBlockRangeSet32 OutOfOrderRanges;
    TBlockRangeSet32 LoadedOutOfOrderRanges;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
