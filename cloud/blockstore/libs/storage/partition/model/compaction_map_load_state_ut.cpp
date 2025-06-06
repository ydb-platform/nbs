#include "compaction_map_load_state.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompactionMapLoadStateTestable: public TCompactionMapLoadState
{
public:
    TCompactionMapLoadStateTestable(
        ui32 maxRangesPerTx,
        ui32 maxOutOfOrderChunksInflight)
        : TCompactionMapLoadState(maxRangesPerTx, maxOutOfOrderChunksInflight)
    {}

    ui32 GetNextRangeIndex() const
    {
        return NextRangeIndex;
    }
    TBlockRange32 GetLoadingRange() const
    {
        return LoadingRange;
    }
    TBlockRangeSet32 GetOutOfOrderRanges() const
    {
        return OutOfOrderRanges;
    }
    TBlockRangeSet32 GetLoadedOutOfOrderRanges() const
    {
        return LoadedOutOfOrderRanges;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompactionMapLoadStateTest)
{
    Y_UNIT_TEST(ShouldLoadChunksInRightSequence)
    {
        const ui32 maxRangesPerTx = 5;
        const ui32 maxOutOfOrderRangeCount = 10;

        TCompactionMapLoadStateTestable state(
            maxRangesPerTx,
            maxOutOfOrderRangeCount);

        for (ui32 i = 0; i < 100; i += maxRangesPerTx) {
            const auto& chunk = state.LoadNextChunk();
            UNIT_ASSERT_EQUAL(chunk.Start, i);
            UNIT_ASSERT_EQUAL(chunk.Size(), maxRangesPerTx);
        }
    }

    Y_UNIT_TEST(ShouldNotLoadAlreadyLoadedChunk)
    {
        const ui32 maxRangesPerTx = 5;
        const ui32 maxOutOfOrderRangeCount = 10;

        TCompactionMapLoadStateTestable state(
            maxRangesPerTx,
            maxOutOfOrderRangeCount);

        state.OnRangeLoaded(
            TBlockRange32::WithLength(maxRangesPerTx, maxRangesPerTx));
        state.OnRangeLoaded(
            TBlockRange32::WithLength(2 * maxRangesPerTx, maxRangesPerTx));
        UNIT_ASSERT_EQUAL(state.GetLoadedOutOfOrderRanges().size(), 2);

        {
            const auto& chunk = state.LoadNextChunk();
            UNIT_ASSERT_EQUAL(chunk.Start, 0);
            UNIT_ASSERT(state.GetLoadedOutOfOrderRanges().empty());
        }
        {
            const auto& chunk = state.LoadNextChunk();
            UNIT_ASSERT_EQUAL(chunk.Start, 15);
        }
        {
            const auto& chunk = state.LoadNextChunk();
            UNIT_ASSERT_EQUAL(chunk.Start, 20);
        }
    }

    Y_UNIT_TEST(EnqueueOutOfOrderShouldReturnTrueIfAnyRangeNotLoaded)
    {
        const ui32 maxRangesPerTx = 5;
        const ui32 maxOutOfOrderRangeCount = 10;

        TCompactionMapLoadStateTestable state(
            maxRangesPerTx,
            maxOutOfOrderRangeCount);

        {
            const auto ranges = state.GetNotLoadedRanges({0, 3, 7, 10, 12, 23});
            UNIT_ASSERT_EQUAL(ranges.size(), 4);
            state.EnqueueOutOfOrderRanges(ranges);
            UNIT_ASSERT_EQUAL(state.GetOutOfOrderRanges().size(), 4);
        }

        state.OnRangeLoaded(TBlockRange32::WithLength(5, maxRangesPerTx));
        state.OnRangeLoaded(TBlockRange32::WithLength(10, maxRangesPerTx));
        UNIT_ASSERT_EQUAL(state.GetLoadedOutOfOrderRanges().size(), 2);

        {
            const auto ranges = state.GetNotLoadedRanges({0, 3, 7, 10, 12, 23});
            UNIT_ASSERT_EQUAL(ranges.size(), 2);
            state.EnqueueOutOfOrderRanges(ranges);
            UNIT_ASSERT_EQUAL(state.GetOutOfOrderRanges().size(), 4);
        }
    }

    Y_UNIT_TEST(EnqueueOutOfOrderShouldReturnFalseIfAllRangesLoaded)
    {
        const ui32 maxRangesPerTx = 5;
        const ui32 maxOutOfOrderRangeCount = 10;

        TCompactionMapLoadStateTestable state(
            maxRangesPerTx,
            maxOutOfOrderRangeCount);

        state.OnRangeLoaded(TBlockRange32::WithLength(5, maxRangesPerTx));
        state.OnRangeLoaded(TBlockRange32::WithLength(10, maxRangesPerTx));
        state.OnRangeLoaded(TBlockRange32::WithLength(20, maxRangesPerTx));
        UNIT_ASSERT_EQUAL(state.GetLoadedOutOfOrderRanges().size(), 3);

        {
            const auto ranges = state.GetNotLoadedRanges({7, 10, 12, 23});
            UNIT_ASSERT(ranges.empty());
            UNIT_ASSERT(state.GetOutOfOrderRanges().empty());
        }
    }

    Y_UNIT_TEST(ShouldLoadOutOfOrderChunkFirst)
    {
        const ui32 maxRangesPerTx = 5;
        const ui32 maxOutOfOrderRangeCount = 10;

        TCompactionMapLoadStateTestable state(
            maxRangesPerTx,
            maxOutOfOrderRangeCount);

        const auto ranges = state.GetNotLoadedRanges({7, 23});
        UNIT_ASSERT_EQUAL(ranges.size(), 2);
        state.EnqueueOutOfOrderRanges(ranges);
        UNIT_ASSERT_EQUAL(state.GetOutOfOrderRanges().size(), 2);

        {
            const auto& chunk = state.LoadNextChunk();
            UNIT_ASSERT_EQUAL(chunk.Start, 5);
        }
        {
            const auto& chunk = state.LoadNextChunk();
            UNIT_ASSERT_EQUAL(chunk.Start, 20);
        }
        {
            const auto& chunk = state.LoadNextChunk();
            UNIT_ASSERT_EQUAL(chunk.Start, 0);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
