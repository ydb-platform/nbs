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
    const std::optional<TBlockRange32>& range,
    ui32 start,
    ui32 end)
{
    UNIT_ASSERT(range);
    UNIT_ASSERT_VALUES_EQUAL(start, range->Start);
    UNIT_ASSERT_VALUES_EQUAL(end, range->End);
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

        UNIT_ASSERT(!state.IsAllRangesLoaded());
        AssertRange(state.LoadNextRanges(), 0, 2);
        UNIT_ASSERT(!state.IsAllRangesLoaded());
        AssertRange(state.LoadNextRanges(), 3, 5);
        UNIT_ASSERT(!state.IsAllRangesLoaded());
        AssertRange(state.LoadNextRanges(), 6, 6);
        UNIT_ASSERT(state.IsAllRangesLoaded());

        UNIT_ASSERT(!state.LoadNextRanges());
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

        AssertRange(state.LoadNextRanges(), 3, 5);

        UNIT_ASSERT(!state.LoadNextRanges());
        UNIT_ASSERT(state.IsAllRangesLoaded());
    }

    Y_UNIT_TEST(ShouldSkipRangesInitializedAfterTransactionRegistration)
    {
        constexpr ui64 RangeCount = 3;
        auto filter = MakeFilter(RangeCount);
        TMixedBlocksFilterLoadState state(
            filter,
            RangeCount,
            1,
            TDuration::MilliSeconds(100));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            state.RegisterTransaction(
                TInstant::Seconds(1),
                TDuration::Zero()));

        InitializeRanges(filter, {0});
        AssertRange(state.LoadNextRanges(), 1, 1);
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
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            state.RegisterTransaction(now, TDuration::Zero()));
        AssertRange(state.LoadNextRanges(), 0, 0);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(500),
            state.RegisterTransaction(now, TDuration::MilliSeconds(150)));
        AssertRange(state.LoadNextRanges(), 1, 1);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(1500),
            state.RegisterTransaction(
                now + TDuration::MilliSeconds(500),
                TDuration::MilliSeconds(150)));
        AssertRange(state.LoadNextRanges(), 2, 2);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
