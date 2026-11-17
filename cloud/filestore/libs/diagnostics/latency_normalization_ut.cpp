#include "latency_normalization.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NDiagnostics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLatencyNormalizationTest)
{
    Y_UNIT_TEST(ShouldShrinkSmallSamplesTowardPrior)
    {
        const auto result = NormalizeLatencyUs(
            3100,
            1,
            1000,
            1.0);

        // One request at 3100 us plus 20 prior requests at 1000 us.
        UNIT_ASSERT_DOUBLES_EQUAL(1100, result, 1e-9);
    }

    Y_UNIT_TEST(ShouldApplyTimeDecayAfterPriorAdjustment)
    {
        const auto result = NormalizeLatencyUs(
            3100,
            1,
            1000,
            0.5);

        UNIT_ASSERT_DOUBLES_EQUAL(550, result, 1e-9);
    }

    Y_UNIT_TEST(ShouldHaveLittlePriorImpactForLargeSamples)
    {
        const auto result = NormalizeLatencyUs(
            2020000,
            1000,
            1000,
            1.0);

        UNIT_ASSERT_DOUBLES_EQUAL(2000, result, 1e-9);
    }
}

}   // namespace NCloud::NFileStore::NDiagnostics
