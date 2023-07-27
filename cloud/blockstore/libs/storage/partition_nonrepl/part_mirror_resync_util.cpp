#include "part_mirror_resync_util.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

std::pair<ui32, ui32> BlockRange2RangeId(TBlockRange64 range, ui32 blockSize)
{
    const auto resyncRangeBlockCount = ResyncRangeSize / blockSize;
    ui32 first = range.Start / resyncRangeBlockCount;
    ui32 last = range.End / resyncRangeBlockCount;
    return std::make_pair(first, last);
}

TBlockRange64 RangeId2BlockRange(ui32 rangeId, ui32 blockSize)
{
    const auto resyncRangeBlockCount = ResyncRangeSize / blockSize;
    ui64 start = rangeId * resyncRangeBlockCount;
    ui64 end = start + resyncRangeBlockCount - 1;
    return TBlockRange64(start, end);
}

}   // namespace NCloud::NBlockStore::NStorage
