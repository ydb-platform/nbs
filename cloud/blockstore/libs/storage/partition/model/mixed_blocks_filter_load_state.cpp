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
{
    Y_ABORT_UNLESS(rangesCount > 0, "Ranges count must be greater than 0");
    Y_ABORT_UNLESS(
        rangesToLoadPerTx > 0,
        "Ranges to load per tx must be greater than 0");
    Y_ABORT_UNLESS(
        allowedCpuTimePerSecond.SecondsFloat() > 0,
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
        const auto endCompactionRange =
            Max<ui64>() - RangesToLoadPerTx + 1 <= CompactionRangeToLoadIndex
                ? Max<ui64>()
                : CompactionRangeToLoadIndex + RangesToLoadPerTx - 1;

        auto compactionRanges = TBlockRange32::MakeClosedIntervalWithLimit(
            CompactionRangeToLoadIndex,
            endCompactionRange,
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
