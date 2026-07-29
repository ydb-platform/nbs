#pragma once

#include "cloud/storage/core/libs/throttling/leaky_bucket.h"
#include "mixed_index_blocks_filter.h"

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TMixedIndexBlocksFilterLoadState
{
private:
    const ui64 RangesCount = 0;
    const ui64 RangesToLoadPerTx = 0;

    ui64 RangeToLoadIndex = 0;

    TLeakyBucket Throttling;

public:
    TMixedIndexBlocksFilterLoadState(
        ui64 rangesCount,
        ui64 rangesToLoadPerTx,
        TDuration allowedCpuTimePerTx);

    [[nodiscard]] bool IsAllRangesLoaded() const;

    struct TLoadNextRangesResult
    {
        TBlockRange32 Range;
        bool IsAllRangesLoaded;
        TDuration Throttling;
    };

    [[nodiscard]] TLoadNextRangesResult LoadNextRanges(
        const TMixedBlocksFilter& mixedBlocksFilter,
        TInstant now,
        TDuration cpuTimeSpentDuringLastTx);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
