#include "mixed_blocks_filter_load_state.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TMixedBlocksFilterLoadState::TMixedBlocksFilterLoadState(
    const TMixedBlocksFilter& mixedBlocksFilter,
    ui64 rangesCount,
    ui64 rangesToLoadPerTx,
    TDuration allowedCpuTimePerSecond)
    : MixedBlocksFilter(mixedBlocksFilter)
    , RangesCount(rangesCount)
    , RangesToLoadPerTx(rangesToLoadPerTx)
    , Throttling(
          allowedCpuTimePerSecond.SecondsFloat(),
          allowedCpuTimePerSecond.SecondsFloat(),
          allowedCpuTimePerSecond.SecondsFloat())
{}

[[nodiscard]] bool TMixedBlocksFilterLoadState::IsAllRangesLoaded() const
{
    return CompactionRangeToLoadIndex >= RangesCount;
}

auto TMixedBlocksFilterLoadState::LoadNextRanges(
    TInstant now,
    TDuration cpuTimeSpentDuringLastTx) -> TLoadNextRangesResult
{
    auto waitTimeRaw =
        Throttling.Register(now, cpuTimeSpentDuringLastTx.SecondsFloat());
    TDuration waitTime = TDuration::MicroSeconds(waitTimeRaw * 1e6);

    while (!IsAllRangesLoaded()) {
        auto compactionRanges = TBlockRange32::MakeClosedIntervalWithLimit(
            CompactionRangeToLoadIndex,
            CompactionRangeToLoadIndex + RangesToLoadPerTx - 1,
            RangesCount - 1);

        CompactionRangeToLoadIndex =
            static_cast<ui64>(compactionRanges.End) + 1;

        bool compactionRangesAlreadyInitialized = true;
        for (ui64 compactionRangeIndex = compactionRanges.Start;
             compactionRangeIndex <= compactionRanges.End;
             ++compactionRangeIndex)
        {
            if (!MixedBlocksFilter.IsCompactionRangeInitialized(
                    compactionRangeIndex))
            {
                compactionRangesAlreadyInitialized = false;
                break;
            }
        }

        if (compactionRangesAlreadyInitialized) {
            continue;
        }

        return {.CompactionRanges = compactionRanges, .Throttling = waitTime};
    }

    return {.CompactionRanges = std::nullopt, .Throttling = TDuration::Zero()};
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
