#include "part_fresh_blocks_state.h"

#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/part_schema.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionFreshBlocksStateTest)
{
    Y_UNIT_TEST(ShouldCalculateFreshBlobByteCount)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);

        state.AddFreshBlob(1, 10);
        state.AddFreshBlob(3, 30);
        state.AddFreshBlob(2, 20);
        state.AddFreshBlob(5, 50);
        state.AddFreshBlob(4, 40);

        UNIT_ASSERT_VALUES_EQUAL(150, state.GetUntrimmedFreshBlobByteCount());

        state.TrimFreshBlobs(3);

        UNIT_ASSERT_VALUES_EQUAL(90, state.GetUntrimmedFreshBlobByteCount());

        state.AddFreshBlob(7, 70);

        UNIT_ASSERT_VALUES_EQUAL(160, state.GetUntrimmedFreshBlobByteCount());

        state.TrimFreshBlobs(10);

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUntrimmedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldCalculateFreshZeroBlockCount)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);

        state.AddFreshBlob(1, 10, /*zeroBlockCount=*/2);
        state.AddFreshBlob(2, 20, /*zeroBlockCount=*/0);
        state.AddFreshBlob(3, 30, /*zeroBlockCount=*/5);

        UNIT_ASSERT_VALUES_EQUAL(7, state.GetUnflushedFreshBlobZeroBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(60, state.GetUnflushedFreshBlobByteCount());

        auto flushed = state.FlushFreshBlob(2);
        UNIT_ASSERT_VALUES_EQUAL(20, flushed.ByteCount);
        UNIT_ASSERT_VALUES_EQUAL(0, flushed.ZeroBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(7, state.GetUnflushedFreshBlobZeroBlockCount());

        flushed = state.FlushFreshBlob(1);
        UNIT_ASSERT_VALUES_EQUAL(10, flushed.ByteCount);
        UNIT_ASSERT_VALUES_EQUAL(2, flushed.ZeroBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(5, state.GetUnflushedFreshBlobZeroBlockCount());

        flushed = state.FlushFreshBlob(3);
        UNIT_ASSERT_VALUES_EQUAL(30, flushed.ByteCount);
        UNIT_ASSERT_VALUES_EQUAL(5, flushed.ZeroBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUnflushedFreshBlobZeroBlockCount());
    }

    Y_UNIT_TEST(ShouldAllowIncrementingFlushCountersToMaxValue)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);

        state.AddFreshBlob(1, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui32>(),
            state.GetUnflushedFreshBlobByteCount());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
