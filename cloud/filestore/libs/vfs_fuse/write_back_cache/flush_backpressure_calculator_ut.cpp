#include "flush_backpressure_calculator.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFlushBackpressureCalculatorTest)
{
    Y_UNIT_TEST(NoBackpressureIfNodeHasNoCachedData)
    {
        TFlushBackpressureCalculator calculator(
            TFlushBatchLimits{
                .MaxWriteRequestSize = 1,
                .MaxWriteRequestsCount = 1,
                .MaxSumWriteRequestsSize = 1,
                .MaxQueuedFlushBatchesPerNode = 1,
            });

        UNIT_ASSERT(!calculator.GetBackpressureStatus(0, 0, 0));
    }

    Y_UNIT_TEST(NoBackpressureIfThresholdIsZero)
    {
        TFlushBackpressureCalculator calculator(
            TFlushBatchLimits{
                .MaxWriteRequestSize = 100,
                .MaxWriteRequestsCount = 4,
                .MaxSumWriteRequestsSize = 200,
                .MaxQueuedFlushBatchesPerNode = 0,
            });

        UNIT_ASSERT(!calculator.GetBackpressureStatus(10000, 10000, 1000));
    }

    Y_UNIT_TEST(NoBackpressureIfRequestCountIsBelowThreshold)
    {
        TFlushBackpressureCalculator calculator(
            TFlushBatchLimits{
                .MaxWriteRequestSize = 1,
                .MaxWriteRequestsCount = 1,
                .MaxSumWriteRequestsSize = 1,
                .MaxQueuedFlushBatchesPerNode = 5,
            });

        UNIT_ASSERT(!calculator.GetBackpressureStatus(1, 3, 100));
        UNIT_ASSERT(!calculator.GetBackpressureStatus(5, 3, 100));
        UNIT_ASSERT(calculator.GetBackpressureStatus(6, 3, 100));
    }

    Y_UNIT_TEST(BackpressureOnMaxSumWriteRequestsSize)
    {
        TFlushBackpressureCalculator calculator(
            TFlushBatchLimits{
                .MaxWriteRequestSize = 10,
                .MaxWriteRequestsCount = 10,
                .MaxSumWriteRequestsSize = 5,
                .MaxQueuedFlushBatchesPerNode = 2,
            });

        UNIT_ASSERT(!calculator.GetBackpressureStatus(7, 7, 10));
        UNIT_ASSERT(calculator.GetBackpressureStatus(7, 7, 11));
    }

    Y_UNIT_TEST(NoBackpressureOnZeroMaxSumWriteRequestsSize)
    {
        TFlushBackpressureCalculator calculator(
            TFlushBatchLimits{
                .MaxWriteRequestSize = 10,
                .MaxWriteRequestsCount = 10,
                .MaxSumWriteRequestsSize = 0,
                .MaxQueuedFlushBatchesPerNode = 2,
            });

        UNIT_ASSERT(!calculator.GetBackpressureStatus(7, 7, 10));
        UNIT_ASSERT(!calculator.GetBackpressureStatus(7, 7, 11));
    }

    Y_UNIT_TEST(BackpressureOnMaxWriteRequestsCount)
    {
        TFlushBackpressureCalculator calculator(
            TFlushBatchLimits{
                .MaxWriteRequestSize = 20,
                .MaxWriteRequestsCount = 3,
                .MaxSumWriteRequestsSize = 100,
                .MaxQueuedFlushBatchesPerNode = 2,
            });

        UNIT_ASSERT(!calculator.GetBackpressureStatus(10, 6, 10));
        UNIT_ASSERT(calculator.GetBackpressureStatus(10, 7, 10));

        // Average region size = 41, min requests per region = 3
        UNIT_ASSERT(!calculator.GetBackpressureStatus(10, 2, 81));
        UNIT_ASSERT(calculator.GetBackpressureStatus(10, 4, 81));
    }

    Y_UNIT_TEST(NoBackpressureOnZeroMaxWriteRequestSize)
    {
        TFlushBackpressureCalculator calculator(
            TFlushBatchLimits{
                .MaxWriteRequestSize = 0,
                .MaxWriteRequestsCount = 5,
                .MaxSumWriteRequestsSize = 100,
                .MaxQueuedFlushBatchesPerNode = 2,
            });

        UNIT_ASSERT(!calculator.GetBackpressureStatus(20, 10, 50));
        UNIT_ASSERT(calculator.GetBackpressureStatus(20, 11, 50));
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
