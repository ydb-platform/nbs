#include "fresh_blocks_inflight.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

void TFreshBlocksInFlight::AddBlockRange(
    TBlockRange32 blockRange,
    ui64 commitId)
{
    for (const ui32 blockIndex: xrange(blockRange)) {
        BlockIndexToCount[blockIndex] += 1;
    }

    CommitIdToCount[commitId] += blockRange.Size();
    BlockCount += blockRange.Size();
}

void TFreshBlocksInFlight::RemoveBlockRange(
    TBlockRange32 blockRange,
    ui64 commitId)
{
    for (const ui32 blockIndex: xrange(blockRange)) {
        auto it = BlockIndexToCount.find(blockIndex);
        Y_VERIFY(it != BlockIndexToCount.end());

        auto& count = const_cast<size_t&>(it->second);
        Y_VERIFY(count > 0);

        if (--count == 0) {
            BlockIndexToCount.erase(it);
        }
    }

    auto it = CommitIdToCount.find(commitId);
    Y_VERIFY(it != CommitIdToCount.end());

    auto& count = const_cast<size_t&>(it->second);
    Y_VERIFY(count >= blockRange.Size());

    if ((count -= blockRange.Size()) == 0) {
        CommitIdToCount.erase(it);
    }

    Y_VERIFY(BlockCount >= blockRange.Size());
    BlockCount -= blockRange.Size();
}

size_t TFreshBlocksInFlight::Size() const
{
    return BlockCount;
}

bool TFreshBlocksInFlight::HasBlocksAt(ui32 blockIndex) const
{
    return BlockIndexToCount.contains(blockIndex);
}

bool TFreshBlocksInFlight::HasBlocksUntil(ui64 commitId) const
{
    auto it = CommitIdToCount.lower_bound(commitId);
    return it != CommitIdToCount.end();
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
