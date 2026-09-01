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

        state.AddFreshBlob(1, 10, TInstant::Seconds(1));
        state.AddFreshBlob(3, 30, TInstant::Seconds(3));
        state.AddFreshBlob(2, 20, TInstant::Seconds(2));
        state.AddFreshBlob(5, 50, TInstant::Seconds(5));
        state.AddFreshBlob(4, 40, TInstant::Seconds(4));

        UNIT_ASSERT_VALUES_EQUAL(150, state.GetUntrimmedFreshBlobByteCount());

        state.TrimFreshBlobs(3);

        UNIT_ASSERT_VALUES_EQUAL(90, state.GetUntrimmedFreshBlobByteCount());

        state.AddFreshBlob(7, 70, TInstant::Seconds(7));

        UNIT_ASSERT_VALUES_EQUAL(160, state.GetUntrimmedFreshBlobByteCount());

        state.TrimFreshBlobs(10);

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUntrimmedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldAllowIncrementingFlushCountersToMaxValue)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);

        state.AddFreshBlob(1, Max<ui32>(), TInstant::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui32>(),
            state.GetUnflushedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldTrackOldestFreshBlobTimestamps)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Zero(),
            state.GetOldestUnflushedFreshBlobTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Zero(),
            state.GetOldestUntrimmedFreshBlobTimestamp());

        // Commit order and admission order are intentionally different. Two
        // blobs also share one timestamp, so removing either one must preserve
        // the other timestamp entry.
        state.AddFreshBlob(10, 10, TInstant::Seconds(30));
        state.AddFreshBlob(20, 20, TInstant::Seconds(10));
        state.AddFreshBlob(30, 30, TInstant::Seconds(10));
        state.AddFreshBlob(40, 40, TInstant::Seconds(20));

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(10),
            state.GetOldestUnflushedFreshBlobTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(10),
            state.GetOldestUntrimmedFreshBlobTimestamp());

        state.FlushFreshBlob(20);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(10),
            state.GetOldestUnflushedFreshBlobTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(10),
            state.GetOldestUntrimmedFreshBlobTimestamp());

        state.FlushFreshBlob(30);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(20),
            state.GetOldestUnflushedFreshBlobTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(10),
            state.GetOldestUntrimmedFreshBlobTimestamp());

        state.TrimFreshBlobs(20);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(10),
            state.GetOldestUntrimmedFreshBlobTimestamp());

        state.TrimFreshBlobs(30);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(20),
            state.GetOldestUntrimmedFreshBlobTimestamp());

        state.FlushFreshBlob(10);
        state.FlushFreshBlob(40);
        state.TrimFreshBlobs(40);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Zero(),
            state.GetOldestUnflushedFreshBlobTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Zero(),
            state.GetOldestUntrimmedFreshBlobTimestamp());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
