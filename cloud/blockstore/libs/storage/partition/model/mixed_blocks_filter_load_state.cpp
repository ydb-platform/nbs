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
    , AllowedCpuTimePerSecond(
          allowedCpuTimePerSecond ? allowedCpuTimePerSecond
                                  : TDuration::Seconds(1))
    , Throttling(
          AllowedCpuTimePerSecond.SecondsFloat(),
          AllowedCpuTimePerSecond.SecondsFloat(),
          AllowedCpuTimePerSecond.SecondsFloat())
{
    Y_ABORT_UNLESS(RangesCount > 0, "Ranges count must be greater than 0");
    Y_ABORT_UNLESS(
        RangesToLoadPerTx > 0,
        "Ranges to load per tx must be greater than 0");
    Y_ABORT_UNLESS(
        AllowedCpuTimePerSecond.SecondsFloat() > 0,
        "Allowed cpu time per second must be greater than 0");
}

[[nodiscard]] bool TMixedBlocksFilterLoadState::IsAllRangesLoaded() const
{
    return CompactionRangeToLoadIndex >= RangesCount;
}

[[nodiscard]] std::optional<TBlockRange32>
TMixedBlocksFilterLoadState::LoadNextRanges()
{
    while (!IsAllRangesLoaded()) {
        const auto endIndex =
            Max<ui64>() - RangesToLoadPerTx + 1 <= CompactionRangeToLoadIndex
                ? Max<ui64>()
                : CompactionRangeToLoadIndex + RangesToLoadPerTx - 1;

        auto compactionRanges = TBlockRange32::MakeClosedIntervalWithLimit(
            CompactionRangeToLoadIndex,
            endIndex,
            RangesCount - 1);

        CompactionRangeToLoadIndex =
            static_cast<ui64>(compactionRanges.End) + 1;

        bool allRangesInitialized = true;
        for (ui64 rangeIndex = compactionRanges.Start;
             rangeIndex <= compactionRanges.End;
             ++rangeIndex)
        {
            if (!MixedBlocksFilter.IsCompactionRangeInitialized(rangeIndex)) {
                allRangesInitialized = false;
                break;
            }
        }

        if (allRangesInitialized) {
            continue;
        }

        return compactionRanges;
    }

    return std::nullopt;
}

[[nodiscard]] TDuration TMixedBlocksFilterLoadState::RegisterTransaction(
    TInstant now,
    TDuration cpuTimeSpentDuringLastTx)
{
    auto postponeTime = TDuration::MicroSeconds(
        Throttling.CalculatePostponeTime(
            now,
            cpuTimeSpentDuringLastTx.SecondsFloat()) *
        1e6);

    Throttling.Register(
        now + postponeTime,
        cpuTimeSpentDuringLastTx.SecondsFloat());

    return postponeTime;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
