#include "mixed_index_blocks_filter_load_state.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TMixedIndexBlocksFilterLoadState::TMixedIndexBlocksFilterLoadState(
    ui64 rangesCount,
    ui64 rangesToLoadPerTx,
    TDuration allowedCpuTimePerTx)
    : RangesCount(rangesCount)
    , RangesToLoadPerTx(rangesToLoadPerTx)
    , Throttling(
          1.0,
          allowedCpuTimePerTx.SecondsFloat(),
          allowedCpuTimePerTx.SecondsFloat())
{}

[[nodiscard]] bool TMixedIndexBlocksFilterLoadState::IsAllRangesLoaded() const
{
    return RangeToLoadIndex >= RangesCount;
}

auto TMixedIndexBlocksFilterLoadState::LoadNextRanges(
    const TMixedBlocksFilter& mixedBlocksFilter,
    TInstant now,
    TDuration cpuTimeSpentDuringLastTx) -> TLoadNextRangesResult
{
    auto waitTimeRaw = Throttling.Register(
        now,
        cpuTimeSpentDuringLastTx.SecondsFloat());
    TDuration waitTime = TDuration::MicroSeconds(waitTimeRaw * 1e6);

    while (!IsAllRangesLoaded()) {
        auto range = TBlockRange32::MakeClosedIntervalWithLimit(
            RangeToLoadIndex,
            RangeToLoadIndex + RangesToLoadPerTx - 1,
            RangesCount - 1);

        RangeToLoadIndex = static_cast<ui64>(range.End) + 1;

        bool rangesAlreadyInitialized = true;
        for (ui64 rangeIndex = range.Start; rangeIndex <= range.End;
             ++rangeIndex)
        {
            if (!mixedBlocksFilter.IsRangeInitialized(rangeIndex)) {
                rangesAlreadyInitialized = false;
                break;
            }
        }

        if (rangesAlreadyInitialized) {
            continue;
        }

        return {
            .Range = range,
            .IsAllRangesLoaded = false,
            .Throttling = waitTime};
    }

    return {
        .Range = TBlockRange32::MakeClosedInterval(0, 0),
        .IsAllRangesLoaded = true,
        .Throttling = TDuration::Zero()
    };
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
