#include "volume_params.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace std::chrono_literals;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRuntimeVolumeParamsTest)
{
    Y_UNIT_TEST(MaxTimedOutDeviceStateDurationOverrideValue)
    {
        struct TTestParam
        {
            TString Key;
            TString Value;
            TInstant ValidUntil;
            TInstant Now;
            TDuration ExpectedValue;
            TString Id;
        };

        TInstant now;
        const TString timeoutKey = "max-timed-out-device-state-duration";

        const TVector<TTestParam> testParams{
            {.Key = "key",
             .Value = "10s",
             .ValidUntil = now + 1min,
             .Now = now,
             .ExpectedValue = {},
             .Id = "unknown key"},
            {.Key = timeoutKey,
             .Value = "10s",
             .ValidUntil = now + 1min,
             .Now = now,
             .ExpectedValue = 10s,
             .Id = "valid value"},
            {.Key = timeoutKey,
             .Value = "10s",
             .ValidUntil = now + 1min,
             .Now = now + 2min,
             .ExpectedValue = {},
             .Id = "expired value"},
            {.Key = timeoutKey,
             .Value = "invalid value",
             .ValidUntil = now + 1min,
             .Now = now,
             .ExpectedValue = {},
             .Id = "invalid value"},
        };

        const auto performTest = [&](const auto& testParam)
        {
            Cerr << "Test id: " << testParam.Id << "\n";

            TRuntimeVolumeParams params{{{
                .Key = testParam.Key,
                .Value = testParam.Value,
                .ValidUntil = testParam.ValidUntil,
            }}};

            UNIT_ASSERT_VALUES_EQUAL(
                testParam.ExpectedValue,
                params.GetMaxTimedOutDeviceStateDurationOverride(
                    testParam.Now));
        };

        for (const auto& testParam: testParams) {
            performTest(testParam);
        }
    }

    Y_UNIT_TEST(ParamExpiration)
    {
        struct TTestParam
        {
            TVector<TRuntimeVolumeParamsValue> ParamValues;
            TMaybe<TDuration> ExpectedDelay;
            TString Id;
        };

        TInstant now;
        now += 1min;

        const TVector<TTestParam> testParams{
            {.ParamValues = {}, .ExpectedDelay = Nothing(), .Id = "no values"},
            {.ParamValues =
                 {{{
                       .Key = "closest expiration",
                       .Value = {},
                       .ValidUntil = now + 1min,
                   },
                   {
                       .Key = "farest expiration",
                       .Value = {},
                       .ValidUntil = now + 2min,
                   }}},
             .ExpectedDelay = 1min,
             .Id = "delay in future"},
            {.ParamValues =
                 {{{
                       .Key = "not expired",
                       .Value = {},
                       .ValidUntil = now + 1min,
                   },
                   {
                       .Key = "expired",
                       .Value = {},
                       .ValidUntil = now - 30s,
                   }}},
             .ExpectedDelay = 1ms,
             .Id = "delay in past"}};

        const auto performTest = [&](const auto& testParam)
        {
            Cerr << "Test id: " << testParam.Id << "\n";
            ;

            TRuntimeVolumeParams params(testParam.ParamValues);

            UNIT_ASSERT_VALUES_EQUAL(
                testParam.ExpectedDelay,
                params.GetNextExpirationDelay(now));
        };

        for (const auto& testParam: testParams) {
            performTest(testParam);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
