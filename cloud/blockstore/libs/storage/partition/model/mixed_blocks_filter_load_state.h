#pragma once

#include "mixed_blocks_filter.h"

#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TCompactionRangesToLoad
{
    ui64 RangeIndex = 0;
    ui64 RangeCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMixedBlocksFilterLoadState
{
private:
    const TMixedBlocksFilter& MixedBlocksFilter;
    const ui64 RangesCount = 0;
    const ui64 RangesToLoadPerTx = 0;
    const TDuration AllowedCpuTimePerSecond;

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
    [[nodiscard]] std::optional<TCompactionRangesToLoad> LoadNextRanges();

    // Register a transaction in leaky bucket and return the time to wait before
    // the next transaction.
    [[nodiscard]] TDuration RegisterTransaction(
        TInstant now,
        TDuration cpuTimeSpentDuringLastTx);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
