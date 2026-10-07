#include <cloud/storage/core/libs/systemd_time/timestamp.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NSystemdTime {
namespace {

const TInstant Now = TInstant::ParseIso8601("2012-11-23T10:15:22.123456Z");

void AssertParse(TStringBuf input, TStringBuf expected)
{
    TInstant result;
    UNIT_ASSERT_C(
        TryParseTimestamp(
            input,
            result,
            Now,
            NDatetime::GetTimeZone("Asia/Shanghai")),
        input);
    UNIT_ASSERT_VALUES_EQUAL_C(result, TInstant::ParseIso8601(expected), input);
}

}   // namespace

Y_UNIT_TEST_SUITE(TTimestampParser)
{
    Y_UNIT_TEST(SpecificationExamples)
    {
        AssertParse("Fri 2012-11-23 11:12:13", "2012-11-23T03:12:13Z");
        AssertParse("2012-11-23 11:12:13", "2012-11-23T03:12:13Z");
        AssertParse("2012-11-23 11:12:13 UTC", "2012-11-23T11:12:13Z");
        AssertParse("2012-11-23T11:12:13Z", "2012-11-23T11:12:13Z");
        AssertParse("2012-11-23T11:12+02:00", "2012-11-23T09:12:00Z");
        AssertParse("2012-11-23", "2012-11-22T16:00:00Z");
        AssertParse("12-11-23", "2012-11-22T16:00:00Z");
        AssertParse("11:12:13", "2012-11-23T03:12:13Z");
        AssertParse("11:12", "2012-11-23T03:12:00Z");
        AssertParse("now", "2012-11-23T10:15:22.123456Z");
        AssertParse("today", "2012-11-22T16:00:00Z");
        AssertParse("today UTC", "2012-11-23T00:00:00Z");
        AssertParse("yesterday", "2012-11-21T16:00:00Z");
        AssertParse("tomorrow", "2012-11-23T16:00:00Z");
        AssertParse("tomorrow Pacific/Auckland", "2012-11-23T11:00:00Z");
        AssertParse("+3h30min", "2012-11-23T13:45:22.123456Z");
        AssertParse("-5s", "2012-11-23T10:15:17.123456Z");
        AssertParse("11min ago", "2012-11-23T10:04:22.123456Z");
        AssertParse("@1395716396", "2014-03-25T02:59:56Z");
        AssertParse(
            "2014-03-25 03:59:56.654563",
            "2014-03-24T19:59:56.654563Z");
    }

    Y_UNIT_TEST(TimezonesAndWeekdays)
    {
        for (const auto input:
             {
                 "Fri 2012-11-23 23:02:15 CET",
                 "friday 2012-11-23T23:02:15 CET",
                 "FRI 2012-11-23T23:02:15 +01",
                 "2012-11-23 23:02:15 +0100",
                 "2012-11-23 23:02:15 +01:00",
                 "2012-11-23T23:02:15+01:00",
                 "2012-11-23 22:02:15Z",
                 "2012-11-23 22:02:15 Z",
                 "2012-11-24 07:02:15 Asia/Tokyo",
                 "2012-11-23 16:32:15 -05:30",
                 "2012-11-23T16:32:15-05:30",
                 "2012-11-23 16:32:15 -0530",
                 "2012-11-23 17:02:15 -05",
             })
        {
            AssertParse(input, "2012-11-23T22:02:15Z");
        }
        AssertParse("2012-11-23 UTC", "2012-11-23T00:00:00Z");
        AssertParse("12-11-23 Asia/Tokyo", "2012-11-22T15:00:00Z");
        AssertParse("11:12 UTC", "2012-11-23T11:12:00Z");
        AssertParse(" 2012-11-23T11:12:13Z \t", "2012-11-23T11:12:13Z");
        AssertParse("1969-12-31 23:00:00 -06", "1970-01-01T05:00:00Z");
    }

    Y_UNIT_TEST(FractionsAndEpoch)
    {
        AssertParse("2012-11-23T11:12:13.1Z", "2012-11-23T11:12:13.100000Z");
        AssertParse(
            "2012-11-23T11:12:13.000001Z",
            "2012-11-23T11:12:13.000001Z");
        AssertParse("11:12:13.123", "2012-11-23T03:12:13.123000Z");
        AssertParse("@0", "1970-01-01T00:00:00Z");
        AssertParse("@1395716396.123456", "2014-03-25T02:59:56.123456Z");
        AssertParse("@0.000001", "1970-01-01T00:00:00.000001Z");
        AssertParse("@0.0000009", "1970-01-01T00:00:00Z");
        AssertParse("2068-01-01 UTC", "2068-01-01T00:00:00Z");
        AssertParse("68-01-01 UTC", "2068-01-01T00:00:00Z");
        AssertParse("70-01-01 UTC", "1970-01-01T00:00:00Z");
        AssertParse("2400-02-29 UTC", "2400-02-29T00:00:00Z");
        AssertParse("9999-12-31 23:59:59 UTC", "9999-12-31T23:59:59Z");
    }

    Y_UNIT_TEST(CalendarFractionTruncation)
    {
        AssertParse("11:12:13.1234567", "2012-11-23T03:12:13.123456Z");
        AssertParse(
            "2012-11-23T11:12:13.123456789Z",
            "2012-11-23T11:12:13.123456Z");
        AssertParse(
            "2012-11-23T11:12:13.123456789+02:00",
            "2012-11-23T09:12:13.123456Z");
        AssertParse(
            "2018-08-09 07:06:05.123456789123456789123456789",
            "2018-08-08T23:06:05.123456Z");
        AssertParse(
            "2012-11-23 23:59:59.999999999999999999999999999 UTC",
            "2012-11-23T23:59:59.999999Z");
        AssertParse("1970-01-01T00:00:00.0000009Z", "1970-01-01T00:00:00Z");
        AssertParse(
            "2012-11-23 11:12:13.1234567 UTC +1s",
            "2012-11-23T11:12:14.123456Z");
    }

    Y_UNIT_TEST(RelativeTimeUnits)
    {
        static const struct
        {
            TStringBuf Span;
            ui64 Microseconds;
        } examples[] = {
            {"2 h", 7200000000},
            {"2hours", 7200000000},
            {"48hr", 172800000000},
            {"1y 12month", 63115200000000},
            {"55s500ms", 55500000},
            {"300ms20s 5day", 432020300000},
            {"1M", 2629800000000},
            {"1month", 2629800000000},
            {"1months", 2629800000000},
            {"12M", 31557600000000},
            {"12month", 31557600000000},
            {"12months", 31557600000000},
            {"1y", 31557600000000},
            {"1years", 31557600000000},
            {"1week", 604800000000},
            {"1weeks", 604800000000},
            {"1w", 604800000000},
            {"1d", 86400000000},
            {"1days", 86400000000},
            {"1hour", 3600000000},
            {"1hours", 3600000000},
            {"1minutes", 60000000},
            {"1minute", 60000000},
            {"1m", 60000000},
            {"1seconds", 1000000},
            {"1second", 1000000},
            {"1sec", 1000000},
            {"1msec", 1000},
            {"1usec", 1},
            {"1us", 1},
            {"1μs", 1},
            {"1µs", 1},
            {"2", 2000000},
            {"1m 2 3s", 65000000},
            {".5h", 1800000000},
            {"1.25s", 1250000},
            {"0.000001s", 1},
            {"0.0000009s", 0},
            {".000000000001M", 2},
            {"0", 0},
        };

        for (const auto& example: examples) {
            TInstant result;
            const TString positive = TString("+") + example.Span;
            UNIT_ASSERT_C(TryParseTimestamp(positive, result, Now), positive);
            UNIT_ASSERT_VALUES_EQUAL_C(
                result.MicroSeconds(),
                Now.MicroSeconds() + example.Microseconds,
                positive);
            const TString left = TString(example.Span) + " left";
            UNIT_ASSERT_C(TryParseTimestamp(left, result, Now), left);
            UNIT_ASSERT_VALUES_EQUAL_C(
                result.MicroSeconds(),
                Now.MicroSeconds() + example.Microseconds,
                left);
            const TString negative = TString("-") + example.Span;
            UNIT_ASSERT_C(TryParseTimestamp(negative, result, Now), negative);
            UNIT_ASSERT_VALUES_EQUAL_C(
                result.MicroSeconds(),
                Now.MicroSeconds() - example.Microseconds,
                negative);
            const TString ago = TString(example.Span) + " ago";
            UNIT_ASSERT_C(TryParseTimestamp(ago, result, Now), ago);
            UNIT_ASSERT_VALUES_EQUAL_C(
                result.MicroSeconds(),
                Now.MicroSeconds() - example.Microseconds,
                ago);
        }
    }

    Y_UNIT_TEST(UsesSelectedZoneForToday)
    {
        const auto now = TInstant::ParseIso8601("2012-11-23T23:30:00Z");
        TInstant result;
        UNIT_ASSERT(TryParseTimestamp("today Asia/Tokyo", result, now));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            TInstant::ParseIso8601("2012-11-23T15:00:00Z"));
        UNIT_ASSERT(TryParseTimestamp("11:12 Asia/Tokyo", result, now));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            TInstant::ParseIso8601("2012-11-24T02:12:00Z"));
        const auto local = NDatetime::GetLocalTimeZone();
        const auto date = cctz::civil_day(NDatetime::Convert(now, local));
        UNIT_ASSERT(TryParseTimestamp("today", result, now));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            NDatetime::Convert(cctz::civil_second(date), local));
    }

    Y_UNIT_TEST(DaylightSavingTransitions)
    {
        AssertParse("2024-03-31 02:30 Europe/Berlin", "2024-03-31T01:30:00Z");
        AssertParse("2024-10-27 02:30 Europe/Berlin", "2024-10-27T00:30:00Z");
        TInstant result;
        const auto now = TInstant::ParseIso8601("2024-03-31T10:00:00Z");
        UNIT_ASSERT(TryParseTimestamp("tomorrow Europe/Berlin", result, now));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            TInstant::ParseIso8601("2024-03-31T22:00:00Z"));
        UNIT_ASSERT(TryParseTimestamp("yesterday Europe/Berlin", result, now));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            TInstant::ParseIso8601("2024-03-29T23:00:00Z"));
    }

    Y_UNIT_TEST(RejectsInvalidInputWithoutChangingResult)
    {
        for (const auto input:
             {
                 "",
                 " ",
                 "garbage",
                 "Fri",
                 "today garbage",
                 "todayZ",
                 "now UTC",
                 "2012-11-23 11:12:13 No/Such_Zone",
                 "2012-11-23 11:12:13UTC",
                 "Thu 2012-11-23",
                 "Monday 2012-11-23T11:12:13Z",
                 "2012-02-30",
                 "2013-02-29",
                 "2100-02-29",
                 "2012-00-01",
                 "2012-13-01",
                 "2012-11-00",
                 "2012-11-31",
                 "0000-01-01",
                 "2012-11-23T",
                 "2012-11-23T UTC",
                 "2012-11-23 24:00",
                 "11:60",
                 "11:12:60",
                 "11:12.5",
                 "11:12:13.",
                 "11:12:13.1234567x",
                 "11:12:13.1234567.8",
                 "2012-11-23T11:12+25:00",
                 "2012-11-23T11:12+01:60",
                 "2012-11-23T11:12+0100",
                 "2012-11-23T11:12+01",
                 "2012-11-23T11:12 +1:00",
                 "2012-11-23T11:12 +24:01",
                 "+",
                 "-",
                 "+-1h",
                 "+1h-2m",
                 "+1h+2m",
                 "+1unknown",
                 "+1ns",
                 "+1.2.3s",
                 "+1.s",
                 "+infinity",
                 "2h",
                 "1h ago UTC",
                 "@-1",
                 "@1 UTC",
                 "@1.",
                 "@18446744073709551616",
                 "+18446744073709551616s",
                 "+18446744073709551615us1us",
                 "1969-01-01 UTC",
                 "Mon..Fri *-*-* 12:00",
             })
        {
            TInstant result = Now;
            UNIT_ASSERT_C(!TryParseTimestamp(input, result, Now), input);
            UNIT_ASSERT_VALUES_EQUAL_C(result, Now, input);
        }
        const TString embeddedNull("2012-11-23\0 UTC", 15);
        TInstant result = Now;
        UNIT_ASSERT(!TryParseTimestamp(embeddedNull, result, Now));
        UNIT_ASSERT_VALUES_EQUAL(result, Now);
        const TString zoneWithNull("2012-11-23 UTC\0suffix", 21);
        UNIT_ASSERT(!TryParseTimestamp(zoneWithNull, result, Now));
        UNIT_ASSERT_VALUES_EQUAL(result, Now);
    }

    Y_UNIT_TEST(ChecksTimestampArithmeticBounds)
    {
        TInstant result;
        UNIT_ASSERT(TryParseTimestamp("@18446744073709.551615", result, Now));
        UNIT_ASSERT_VALUES_EQUAL(result, TInstant::Max());
        UNIT_ASSERT(!TryParseTimestamp("@18446744073709.551616", result, Now));
        UNIT_ASSERT(!TryParseTimestamp("+1us", result, TInstant::Max()));
        UNIT_ASSERT(!TryParseTimestamp("-1us", result, TInstant::Zero()));
        UNIT_ASSERT(
            TryParseTimestamp("-1us", result, TInstant::MicroSeconds(1)));
        UNIT_ASSERT_VALUES_EQUAL(result, TInstant::Zero());
        UNIT_ASSERT(TryParseTimestamp(
            "+1us",
            result,
            TInstant::Max() - TDuration::MicroSeconds(1)));
        UNIT_ASSERT_VALUES_EQUAL(result, TInstant::Max());
    }
}

}   // namespace NSystemdTime
