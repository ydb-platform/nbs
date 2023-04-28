#include "ping_metrics.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPingMetricsTest)
{
    const TDuration HalfDecay = TDuration::Seconds(15);

    Y_UNIT_TEST(ShouldCalculateMetrics)
    {
        TPingMetrics metrics;

        metrics.Update(TInstant::MilliSeconds(100), HalfDecay, 4096);
        UNIT_ASSERT_VALUES_EQUAL(4096, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(1, metrics.GetRequests());

        metrics.Update(TInstant::MilliSeconds(600), HalfDecay, 4096);
        UNIT_ASSERT_VALUES_EQUAL(8098, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(1, metrics.GetRequests());

        metrics.Update(TInstant::MilliSeconds(600), HalfDecay, 4096);
        UNIT_ASSERT_VALUES_EQUAL(12194, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(2, metrics.GetRequests());

        metrics.Update(TInstant::MilliSeconds(600), HalfDecay, 8192);
        UNIT_ASSERT_VALUES_EQUAL(20386, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(3, metrics.GetRequests());

        metrics.Update(TInstant::MilliSeconds(600), HalfDecay, 8192);
        UNIT_ASSERT_VALUES_EQUAL(28578, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(4, metrics.GetRequests());

        metrics.Update(TInstant::MilliSeconds(15'600), HalfDecay, 4096);
        UNIT_ASSERT_VALUES_EQUAL(18385, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(3, metrics.GetRequests());
    }

    Y_UNIT_TEST(ShouldUpdateMetrics)
    {
        TPingMetrics metrics;

        for (ui32 i = 0; i < 8; ++i) {
            metrics.Update(TInstant::Seconds(1), HalfDecay, 8192);
        }

        UNIT_ASSERT_VALUES_EQUAL(65536, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(8, metrics.GetRequests());

        metrics.Update(TInstant::Seconds(16), HalfDecay, 0);
        UNIT_ASSERT_VALUES_EQUAL(32768, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(4, metrics.GetRequests());

        metrics.Update(TInstant::Seconds(31), HalfDecay, 0);
        UNIT_ASSERT_VALUES_EQUAL(16384, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(2, metrics.GetRequests());

        metrics.Update(TInstant::Seconds(46), HalfDecay, 0);
        UNIT_ASSERT_VALUES_EQUAL(8192, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(1, metrics.GetRequests());

        metrics.Update(TInstant::Seconds(61), HalfDecay, 0);
        UNIT_ASSERT_VALUES_EQUAL(4096, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, metrics.GetRequests());

        metrics.Update(TInstant::Seconds(76), HalfDecay, 0);
        UNIT_ASSERT_VALUES_EQUAL(2048, metrics.GetBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, metrics.GetRequests());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
