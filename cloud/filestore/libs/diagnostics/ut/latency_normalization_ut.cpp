#include <cloud/filestore/libs/diagnostics/latency_normalization.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NDiagnostics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLatencyNormalizationTest)
{
    Y_UNIT_TEST(ShouldShrinkSmallSamplesTowardPrior)
    {
        const auto result = NormalizeLatencyUs(
            4744,  // observed total latency, us
            1,
            2000,  // prior mean latency, us
            1.0);  // no time decay

        UNIT_ASSERT_DOUBLES_EQUAL(2130.6666666667, result, 1e-9);
    }

    Y_UNIT_TEST(ShouldApplyTimeDecayAfterPriorAdjustment)
    {
        const auto result = NormalizeLatencyUs(
            4744,
            1,
            2000,
            0.5);

        UNIT_ASSERT_DOUBLES_EQUAL(1065.3333333333, result, 1e-9);
    }

    Y_UNIT_TEST(ShouldHaveLittlePriorImpactForLargeSamples)
    {
        const auto result = NormalizeLatencyUs(
            186301987,
            89555,
            2000,
            1.0);

        UNIT_ASSERT_DOUBLES_EQUAL(2080.2901144, result, 1e-6);
    }
}

}   // namespace NCloud::NFileStore::NDiagnostics
