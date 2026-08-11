#pragma once

#include "mixed_blocks_filter.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>
#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TMixedBlocksFilterLoadState
{
private:
    const TMixedBlocksFilter& MixedBlocksFilter;
    const ui64 RangesCount = 0;
    const ui64 RangesToLoadPerTx = 0;

    ui64 CompactionRangeToLoadIndex = 0;

    TLeakyBucket Throttling;

public:
    TMixedBlocksFilterLoadState(
        const TMixedBlocksFilter& mixedBlocksFilter,
        ui64 rangesCount,
        ui64 rangesToLoadPerTx,
        TDuration allowedCpuTimePerSecond);

    [[nodiscard]] bool IsAllRangesLoaded() const;

    // nullopt means that all compaction ranges are loaded.
    [[nodiscard]] std::optional<TBlockRange32> LoadNextRanges();

    [[nodiscard]] TDuration RegisterTransaction(
        TInstant now,
        TDuration cpuTimeSpentDuringLastTx);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
