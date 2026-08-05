#include "mixed_blocks_filter_load_state.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 BlocksPerRange = TCompressedBitmap::CHUNK_SIZE;

TMixedBlocksFilter MakeFilter(ui64 rangeCount)
{
    return TMixedBlocksFilter(0, BlocksPerRange, rangeCount * BlocksPerRange);
}

void InitializeRanges(
    TMixedBlocksFilter& filter,
    TVector<ui32> rangeIndices)
{
    filter.CompactionStarted(std::move(rangeIndices), 1);
    filter.CompactionFinished();
}

void AssertRange(
    const TMixedBlocksFilterLoadState::TLoadNextRangesResult& result,
    ui32 start,
    ui32 end,
    TDuration throttling = TDuration::Zero())
{
    UNIT_ASSERT(result.CompactionRanges);
    UNIT_ASSERT_VALUES_EQUAL(start, result.CompactionRanges->Start);
    UNIT_ASSERT_VALUES_EQUAL(end, result.CompactionRanges->End);
    UNIT_ASSERT_VALUES_EQUAL(throttling, result.Throttling);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMixedBlocksFilterLoadStateTest)
{
    Y_UNIT_TEST(ShouldLoadRangesInBoundedBatches)
    {
        constexpr ui64 RangeCount = 7;
        auto filter = MakeFilter(RangeCount);
        TMixedBlocksFilterLoadState state(
            filter,
            RangeCount,
            3,
            TDuration::MilliSeconds(100));

        const auto now = TInstant::Seconds(1);

        UNIT_ASSERT(!state.IsAllRangesLoaded());
        AssertRange(state.LoadNextRanges(now, TDuration::Zero()), 0, 2);
        UNIT_ASSERT(!state.IsAllRangesLoaded());
        AssertRange(state.LoadNextRanges(now, TDuration::Zero()), 3, 5);
        UNIT_ASSERT(!state.IsAllRangesLoaded());
        AssertRange(state.LoadNextRanges(now, TDuration::Zero()), 6, 6);
        UNIT_ASSERT(state.IsAllRangesLoaded());

        const auto result = state.LoadNextRanges(now, TDuration::Zero());
        UNIT_ASSERT(!result.CompactionRanges);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), result.Throttling);
    }

    Y_UNIT_TEST(ShouldSkipInitializedBatches)
    {
        constexpr ui64 RangeCount = 10;
        auto filter = MakeFilter(RangeCount);
        InitializeRanges(filter, {0, 1, 2, 3, 6, 7, 8, 9});

        TMixedBlocksFilterLoadState state(
            filter,
            RangeCount,
            3,
            TDuration::MilliSeconds(100));

        const auto now = TInstant::Seconds(1);
        AssertRange(state.LoadNextRanges(now, TDuration::Zero()), 3, 5);

        const auto result = state.LoadNextRanges(now, TDuration::Zero());
        UNIT_ASSERT(!result.CompactionRanges);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), result.Throttling);
        UNIT_ASSERT(state.IsAllRangesLoaded());
    }

    Y_UNIT_TEST(ShouldStartCompletedWhenThereAreNoRanges)
    {
        auto filter = MakeFilter(1);
        TMixedBlocksFilterLoadState state(
            filter,
            0,
            1,
            TDuration::MilliSeconds(100));

        UNIT_ASSERT(state.IsAllRangesLoaded());

        const auto result = state.LoadNextRanges(
            TInstant::Seconds(1),
            TDuration::Seconds(1));
        UNIT_ASSERT(!result.CompactionRanges);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), result.Throttling);
    }

    Y_UNIT_TEST(ShouldThrottleAccordingToCpuTime)
    {
        constexpr ui64 RangeCount = 3;
        auto filter = MakeFilter(RangeCount);
        TMixedBlocksFilterLoadState state(
            filter,
            RangeCount,
            1,
            TDuration::MilliSeconds(100));

        const auto now = TInstant::Seconds(1);
        AssertRange(state.LoadNextRanges(now, TDuration::Zero()), 0, 0);
        AssertRange(
            state.LoadNextRanges(now, TDuration::MilliSeconds(150)),
            1,
            1,
            TDuration::MilliSeconds(500));
        AssertRange(
            state.LoadNextRanges(
                now + TDuration::MilliSeconds(500),
                TDuration::MilliSeconds(150)),
            2,
            2);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
