#include "part_mirror_resync_util.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

std::pair<ui32, ui32> BlockRange2RangeId(TBlockRange64 range)
{
    ui32 first = range.Start / ResyncRangeBlockCount;
    ui32 last = range.End / ResyncRangeBlockCount;
    return std::make_pair(first, last);
}

TBlockRange64 RangeId2BlockRange(ui32 rangeId)
{
    ui64 start = rangeId * ResyncRangeBlockCount;
    ui64 end = start + ResyncRangeBlockCount - 1;
    return TBlockRange64(start, end);
}

}   // namespace NCloud::NBlockStore::NStorage
