#include "common_filter_params.h"

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NProfileTool {

Y_UNIT_TEST_SUITE(TCommonFilterParamsTest)
{
    Y_UNIT_TEST(ShouldParseSystemdTimestamps)
    {
        NLastGetopt::TOpts opts;
        const TCommonFilterParams params(opts);
        const char* argv[] = {
            "profile-tool",
            "--since",
            "Fri 2012-11-23 11:12:13 UTC",
            "--until",
            "@1395716396",
        };
        const NLastGetopt::TOptsParseResultException parsed(
            &opts, std::size(argv), argv);
        UNIT_ASSERT_VALUES_EQUAL(
            params.GetSince(parsed).GetRef(),
            TInstant::ParseIso8601("2012-11-23T11:12:13Z"));
        UNIT_ASSERT_VALUES_EQUAL(params.GetUntil(parsed).GetRef(),
                                 TInstant::Seconds(1395716396));
    }

    Y_UNIT_TEST(ShouldShareReferenceTimeForRelativeFilters)
    {
        NLastGetopt::TOpts opts;
        const TCommonFilterParams params(opts);
        const char* argv[] = {"profile-tool", "--since=-1h", "--until=now"};
        const NLastGetopt::TOptsParseResultException parsed(
            &opts, std::size(argv), argv);
        const auto since = params.GetSince(parsed);
        const auto until = params.GetUntil(parsed);
        UNIT_ASSERT(since);
        UNIT_ASSERT(until);
        UNIT_ASSERT_VALUES_EQUAL(*until - *since, TDuration::Hours(1));
        UNIT_ASSERT_VALUES_EQUAL(params.GetUntil(parsed).GetRef(),
                                 until.GetRef());
    }

    Y_UNIT_TEST(ShouldIgnoreInvalidAndMissingTimestamps)
    {
        NLastGetopt::TOpts opts;
        const TCommonFilterParams params(opts);
        const char* argv[] = {"profile-tool", "--since", "invalid"};
        const NLastGetopt::TOptsParseResultException parsed(
            &opts, std::size(argv), argv);
        UNIT_ASSERT(!params.GetSince(parsed));
        UNIT_ASSERT(!params.GetUntil(parsed));
    }
}

}   // namespace NCloud::NFileStore::NProfileTool
