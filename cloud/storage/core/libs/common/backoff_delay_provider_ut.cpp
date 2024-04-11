#include "backoff_delay_provider.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

Y_UNIT_TEST_SUITE(TBackoffDelayProvider)
{
    Y_UNIT_TEST(ExampleUsage)
    {
        TBackoffDelayProvider provider(
            TDuration::Seconds(1),
            TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), provider.GetDelay());
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(2), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(4), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(8), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(16), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(30), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(30), provider.GetDelay());
    }

    Y_UNIT_TEST(Reset)
    {
        TBackoffDelayProvider provider(
            TDuration::Seconds(10),
            TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(10), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(20), provider.GetDelay());
        provider.Reset();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(10), provider.GetDelay());
    }

    Y_UNIT_TEST(GetAndIncrease)
    {
        TBackoffDelayProvider provider(
            TDuration::Seconds(1),
            TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            provider.GetDelayAndIncrease());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(2),
            provider.GetDelayAndIncrease());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(4),
            provider.GetDelayAndIncrease());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(8),
            provider.GetDelayAndIncrease());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(10),
            provider.GetDelayAndIncrease());
    }
}

}   // namespace NCloud
