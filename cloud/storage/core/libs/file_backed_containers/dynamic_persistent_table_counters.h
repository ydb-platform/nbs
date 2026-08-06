#pragma once

#include <util/system/types.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Aggregate counters returned by TDynamicPersistentTable::GetCounters(). Kept
// in a standalone header so consumers can depend on this struct without
// including the whole table.
struct TDynamicPersistentTableCounters
{
    // current capacity of the underlying file map, in bytes
    ui64 RawCapacityByteCount = 0;
    // bytes currently occupied by live records
    ui64 RawUsedByteCount = 0;
    // number of times the file map shrank
    ui64 ShrinkCount = 0;
    // number of times the file map expanded
    ui64 ExpansionCount = 0;
    // number of times the data area was compacted in place
    ui64 CompactionCount = 0;
    // number of expansion attempts rejected by the memory limiter
    ui64 MemoryLimiterRejectionCount = 0;
};

}   // namespace NCloud
