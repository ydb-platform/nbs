#include "flush_batch_request_counter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFlushBatchRequestCounterTest)
{
    Y_UNIT_TEST(Simple)
    {
        TFlushBatchRequestCounter counter;
        TFlushBatchLimits flushBatchLimits{.MaxWriteRequestSize = 10};

        UNIT_ASSERT_VALUES_EQUAL(0, counter.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(0, counter.GetSumWriteRequestsSize());

        counter.AddRequestInterval(flushBatchLimits, 10, 30);

        UNIT_ASSERT_VALUES_EQUAL(2, counter.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(20, counter.GetSumWriteRequestsSize());

        counter.Reset();

        UNIT_ASSERT_VALUES_EQUAL(0, counter.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(0, counter.GetSumWriteRequestsSize());
    }

    Y_UNIT_TEST(Separate)
    {
        TFlushBatchRequestCounter counter;
        TFlushBatchLimits flushBatchLimits{.MaxWriteRequestSize = 10};

        counter.AddRequestInterval(flushBatchLimits, 10, 30);
        counter.AddRequestInterval(flushBatchLimits, 0, 9);
        counter.AddRequestInterval(flushBatchLimits, 31, 40);

        UNIT_ASSERT_VALUES_EQUAL(4, counter.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(38, counter.GetSumWriteRequestsSize());
    }

    Y_UNIT_TEST(MergeTouching)
    {
        TFlushBatchRequestCounter counter;
        TFlushBatchLimits flushBatchLimits{.MaxWriteRequestSize = 100};

        counter.AddRequestInterval(flushBatchLimits, 0, 10);
        counter.AddRequestInterval(flushBatchLimits, 30, 40);
        counter.AddRequestInterval(flushBatchLimits, 10, 30);

        UNIT_ASSERT_VALUES_EQUAL(1, counter.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(40, counter.GetSumWriteRequestsSize());
    }

    Y_UNIT_TEST(MergeOverlapping)
    {
        TFlushBatchRequestCounter counter;
        TFlushBatchLimits flushBatchLimits{.MaxWriteRequestSize = 100};

        counter.AddRequestInterval(flushBatchLimits, 0, 11);
        counter.AddRequestInterval(flushBatchLimits, 29, 40);
        counter.AddRequestInterval(flushBatchLimits, 10, 30);

        UNIT_ASSERT_VALUES_EQUAL(1, counter.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(40, counter.GetSumWriteRequestsSize());
    }

    Y_UNIT_TEST(MergeFullCover)
    {
        TFlushBatchRequestCounter counter;
        TFlushBatchLimits flushBatchLimits{.MaxWriteRequestSize = 100};

        counter.AddRequestInterval(flushBatchLimits, 1, 10);
        counter.AddRequestInterval(flushBatchLimits, 29, 39);
        counter.AddRequestInterval(flushBatchLimits, 0, 40);

        UNIT_ASSERT_VALUES_EQUAL(1, counter.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(40, counter.GetSumWriteRequestsSize());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
