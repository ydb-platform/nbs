#include "common_filter_params.h"

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/testing/common/scope.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NProfileTool {

namespace {

void AssertGrafanaRange(TStringBuf json, TStringBuf now, TStringBuf since,
                        TStringBuf until)
{
    NLastGetopt::TOpts opts;
    const TCommonFilterParams params(opts, TInstant::ParseIso8601(now));
    const TString argument = TString("--grafana-range=") + json;
    const char* argv[] = {"profile-tool", argument.c_str()};
    const NLastGetopt::TOptsParseResultException parsed(&opts, std::size(argv),
                                                        argv);
    UNIT_ASSERT_VALUES_EQUAL_C(params.GetUntil(parsed).GetRef(),
                               TInstant::ParseIso8601(until), json);
    UNIT_ASSERT_VALUES_EQUAL_C(params.GetSince(parsed).GetRef(),
                               TInstant::ParseIso8601(since), json);
    UNIT_ASSERT_VALUES_EQUAL_C(params.GetUntil(parsed).GetRef(),
                               TInstant::ParseIso8601(until), json);
}

}   // namespace

Y_UNIT_TEST_SUITE(TCommonFilterParamsTest)
{
    Y_UNIT_TEST(ShouldDefaultTimeFiltersToUtc)
    {
        const NTesting::TScopedEnvironment timezone("TZ", "Asia/Tokyo");
        // It is already January 2 in Tokyo, so omitted dates and day keywords
        // must also use UTC, not just timestamps with an explicit date.
        const auto now = TInstant::ParseIso8601("2023-01-01T23:30:00Z");
        const struct
        {
            const char* Input;
            const char* Expected;
        } cases[] = {
            {"2023-01-01T10:00:00", "2023-01-01T10:00:00Z"},
            {"2023-01-01", "2023-01-01T00:00:00Z"},
            {"10:00", "2023-01-01T10:00:00Z"},
            {"today", "2023-01-01T00:00:00Z"},
            {"tomorrow", "2023-01-02T00:00:00Z"},
            {"2023-01-01T10:00:00+09:00", "2023-01-01T01:00:00Z"},
            {"2023-01-01 10:00:00 Asia/Tokyo", "2023-01-01T01:00:00Z"},
            {"today Asia/Tokyo", "2023-01-01T15:00:00Z"},
        };
        for (const auto& example: cases) {
            NLastGetopt::TOpts opts;
            const TCommonFilterParams params(opts, now);
            const char* argv[] = {
                "profile-tool",
                "--since",
                example.Input,
                "--until",
                example.Input,
            };
            const NLastGetopt::TOptsParseResultException parsed(
                &opts, std::size(argv), argv);
            const auto expected = TInstant::ParseIso8601(example.Expected);
            UNIT_ASSERT_VALUES_EQUAL_C(
                params.GetSince(parsed).GetRef(), expected, example.Input);
            UNIT_ASSERT_VALUES_EQUAL_C(
                params.GetUntil(parsed).GetRef(), expected, example.Input);
        }
    }

    Y_UNIT_TEST(ShouldDefaultZonelessGrafanaTimestampsToUtc)
    {
        const NTesting::TScopedEnvironment timezone("TZ", "Asia/Tokyo");
        AssertGrafanaRange(
            R"({"from":"2023-01-01T10:00:00","to":"2023-01-01T11:00:00"})",
            "2023-01-01T23:30:00Z",
            "2023-01-01T10:00:00Z",
            "2023-01-01T11:00:00Z");
    }

    Y_UNIT_TEST(ShouldParseCopiedGrafanaRanges)
    {
        constexpr TStringBuf now = "2026-10-06T14:46:30.146Z";
        AssertGrafanaRange(
            R"({"from":"2026-10-06T14:31:30.146Z","to":"2026-10-06T14:46:30.146Z"})",
            now, "2026-10-06T14:31:30.146Z", now);
        AssertGrafanaRange(R"({"from":"now-2d","to":"now"})", now,
                           "2026-10-04T14:46:30.146Z", now);
        AssertGrafanaRange(R"({"from":"now-15m","to":"now"})", now,
                           "2026-10-06T14:31:30.146Z", now);
        AssertGrafanaRange(R"({"from":"now-1h","to":"now"})", now,
                           "2026-10-06T13:46:30.146Z", now);
        AssertGrafanaRange(R"({"from":"now-6M","to":"now"})", now,
                           "2026-04-06T14:46:30.146Z", now);
    }

    Y_UNIT_TEST(ShouldClampGrafanaCalendarOffsets)
    {
        AssertGrafanaRange(
            R"({"from":"now-1M","to":"now"})", "2026-03-31T14:46:30.146Z",
            "2026-02-28T14:46:30.146Z", "2026-03-31T14:46:30.146Z");
        AssertGrafanaRange(
            R"({"from":"now-1M","to":"now"})", "2024-03-31T14:46:30.146Z",
            "2024-02-29T14:46:30.146Z", "2024-03-31T14:46:30.146Z");
        AssertGrafanaRange(
            R"({"from":"now-1y","to":"now"})", "2024-02-29T14:46:30.146Z",
            "2023-02-28T14:46:30.146Z", "2024-02-29T14:46:30.146Z");
        AssertGrafanaRange(
            R"({"from":"now","to":"now+1M"})", "2026-01-31T14:46:30.146Z",
            "2026-01-31T14:46:30.146Z", "2026-02-28T14:46:30.146Z");
        AssertGrafanaRange(
            R"({"from":"now-1Q","to":"now"})", "2026-05-31T14:46:30.146Z",
            "2026-02-28T14:46:30.146Z", "2026-05-31T14:46:30.146Z");
    }

    Y_UNIT_TEST(ShouldUseSameNowForBothGrafanaBounds)
    {
        constexpr TStringBuf now = "2026-10-06T14:46:30.146Z";
        AssertGrafanaRange(R"({"from":"now-1h","to":"now-15m"})", now,
                           "2026-10-06T13:46:30.146Z",
                           "2026-10-06T14:31:30.146Z");
        AssertGrafanaRange(R"({"from":"now","to":"now"})", now, now, now);
        AssertGrafanaRange(
            R"({"to":"now-15m", "from":"2026-10-06T15:00:00+02:00"})", now,
            "2026-10-06T13:00:00Z", "2026-10-06T14:31:30.146Z");
        AssertGrafanaRange(R"({"from":"now-6M+1d","to":"now+1s"})", now,
                           "2026-04-07T14:46:30.146Z",
                           "2026-10-06T14:46:31.146Z");
        AssertGrafanaRange(R"({"from":"now-1w","to":"now"})", now,
                           "2026-09-29T14:46:30.146Z", now);
    }

    Y_UNIT_TEST(ShouldRejectInvalidGrafanaRanges)
    {
        for (const auto json: {
                 "", "not json", "[]", "null", "{}", R"({"from":"now"})",
                 R"({"to":"now"})", R"({"from":123,"to":"now"})",
                 R"({"from":"now","to":null})",
                 R"({"from":"now","to":"now"})junk",
            R"({"from":"bad","to":"now"})", R"({"from":"now","to":"bad"})",
                 R"({"from":"now+1s","to":"now"})",
                 R"({"from":"now-1x","to":"now"})",
                 R"({"from":"now--1h","to":"now"})",
                 R"({"from":"now-1","to":"now"})",
                 R"({"from":"now-99999y","to":"now"})",
                 R"({"from":"now-18446744073709551616s","to":"now"})",
                 R"({"from":"now-18446744073709551615d","to":"now"})",
                 R"({"from":"now","to":"now+18446744073709551615M"})",
             })
        {
            NLastGetopt::TOpts opts;
            const TCommonFilterParams params(opts);
            const char* argv[] = {"profile-tool", "--grafana-range", json};
            const NLastGetopt::TOptsParseResultException parsed(
                &opts, std::size(argv), argv);
            UNIT_ASSERT_EXCEPTION(params.GetSince(parsed),
                                  NLastGetopt::TUsageException);
            UNIT_ASSERT_EXCEPTION(params.GetUntil(parsed),
                                  NLastGetopt::TUsageException);
        }
    }

    Y_UNIT_TEST(ShouldRejectConflictingTimeFilters)
    {
        for (const auto option: {"--since=now", "--until=now"}) {
            NLastGetopt::TOpts opts;
            const TCommonFilterParams params(opts);
            const char* argv[] = {
                "profile-tool",
                "--grafana-range",
                R"({"from":"now-1h","to":"now"})",
                option,
            };
            const NLastGetopt::TOptsParseResultException parsed(
                &opts, std::size(argv), argv);
            UNIT_ASSERT_EXCEPTION(params.GetSince(parsed),
                                  NLastGetopt::TUsageException);
            UNIT_ASSERT_EXCEPTION(params.GetUntil(parsed),
                                  NLastGetopt::TUsageException);
        }
    }

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
