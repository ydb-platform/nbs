#include <cloud/storage/core/libs/systemd_time/timestamp.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NSystemdTime {
namespace {

// Cases adapted from chrono-systemd-time to this parser's documented behavior.
// Covers systemd-style calendar and relative timestamps plus parser extensions
// such as epoch spans and timestamp modifiers. Instants before the Unix epoch
// are rejected because TInstant has an unsigned representation.

TInstant At(
    const cctz::civil_second& civil,
    const NDatetime::TTimeZone& zone,
    ui64 microseconds = 0)
{
    return NDatetime::Convert(civil, zone) +
           TDuration::MicroSeconds(microseconds);
}

void AssertParse(
    TStringBuf input,
    TInstant expected,
    TInstant now,
    const NDatetime::TTimeZone& zone)
{
    TInstant result;
    UNIT_ASSERT_C(TryParseTimestamp(input, result, now, zone), input);
    UNIT_ASSERT_VALUES_EQUAL_C(result, expected, input);
}

void AssertReject(TStringBuf input)
{
    const auto now = TInstant::ParseIso8601("2018-06-21T01:02:03.203918Z");
    TInstant result = now;
    UNIT_ASSERT_C(
        !TryParseTimestamp(input, result, now, NDatetime::GetUtcTimeZone()),
        input);
    UNIT_ASSERT_VALUES_EQUAL_C(result, now, input);
}

}   // namespace

Y_UNIT_TEST_SUITE(TChronoAdaptedCases)
{
    Y_UNIT_TEST(CalendarFormats)
    {
        const struct
        {
            TStringBuf Input;
            cctz::civil_second Civil;
            ui64 Microseconds;
        } cases[] = {
            {"2018-08-09 07:06:05", cctz::civil_second{2018, 8, 9, 7, 6, 5}, 0},
            {"18-08-09 07:06:05", cctz::civil_second{2018, 8, 9, 7, 6, 5}, 0},
            {"2018-08-09 07:06", cctz::civil_second{2018, 8, 9, 7, 6, 0}, 0},
            {"18-08-09 07:06", cctz::civil_second{2018, 8, 9, 7, 6, 0}, 0},
            {"2018-08-09", cctz::civil_second{2018, 8, 9}, 0},
            {"18-08-09", cctz::civil_second{2018, 8, 9}, 0},
            {"10:11:12", cctz::civil_second{2018, 6, 21, 10, 11, 12}, 0},
            {"10:11", cctz::civil_second{2018, 6, 21, 10, 11, 0}, 0},
            {"2018-08-09 07:06:05.123",
             cctz::civil_second{2018, 8, 9, 7, 6, 5},
             123000},
            {"18-08-09 07:06:05.1",
             cctz::civil_second{2018, 8, 9, 7, 6, 5},
             100000},
            {"10:11:12.1234",
             cctz::civil_second{2018, 6, 21, 10, 11, 12},
             123400},
        };

        for (const auto& zone:
             {
                 NDatetime::GetUtcTimeZone(),
                 NDatetime::GetLocalTimeZone(),
                 NDatetime::GetTimeZone("Asia/Shanghai"),
             })
        {
            // Freeze the reference date in each zone instead of reading the
            // clock repeatedly, which could cross midnight during the test.
            const auto now =
                At(cctz::civil_second{2018, 6, 21, 1, 2, 3}, zone, 203918);
            for (const auto& example: cases) {
                AssertParse(
                    example.Input,
                    At(example.Civil, zone, example.Microseconds),
                    now,
                    zone);
            }
        }
    }

    Y_UNIT_TEST(Keywords)
    {
        for (const auto& zone:
             {
                 NDatetime::GetUtcTimeZone(),
                 NDatetime::GetLocalTimeZone(),
                 NDatetime::GetTimeZone("Asia/Shanghai"),
             })
        {
            const auto now =
                At(cctz::civil_second{2018, 6, 21, 1, 2, 3}, zone, 203918);
            AssertParse("now", now, now, zone);
            AssertParse("epoch", TInstant::Zero(), now, zone);
            AssertParse(
                "today",
                At(cctz::civil_second{2018, 6, 21}, zone),
                now,
                zone);
            AssertParse(
                "tomorrow",
                At(cctz::civil_second{2018, 6, 22}, zone),
                now,
                zone);
            AssertParse(
                "yesterday",
                At(cctz::civil_second{2018, 6, 20}, zone),
                now,
                zone);
        }
    }

    Y_UNIT_TEST(RelativeFormsAndRepeatedUnits)
    {
        const struct
        {
            TStringBuf Span;
            ui64 Seconds;
        } cases[] = {
            {"1s", 1},
            {"1s2m", 121},
            {"1s 2m", 121},
            {"1s 2s", 3},
            {"1s 4m 2s", 243},
        };

        for (const auto& zone:
             {
                 NDatetime::GetUtcTimeZone(),
                 NDatetime::GetLocalTimeZone(),
                 NDatetime::GetTimeZone("Asia/Shanghai"),
             })
        {
            const auto now =
                At(cctz::civil_second{2018, 6, 21, 1, 2, 3}, zone, 203918);
            for (const auto& example: cases) {
                const auto offset = TDuration::Seconds(example.Seconds);
                AssertParse(
                    TString("+") + example.Span,
                    now + offset,
                    now,
                    zone);
                AssertParse(
                    TString(example.Span) + " left",
                    now + offset,
                    now,
                    zone);
                AssertParse(
                    TString("-") + example.Span,
                    now - offset,
                    now,
                    zone);
                AssertParse(
                    TString(example.Span) + " ago",
                    now - offset,
                    now,
                    zone);
            }
        }
    }

    Y_UNIT_TEST(Whitespace)
    {
        const auto zone = NDatetime::GetUtcTimeZone();
        const auto now =
            At(cctz::civil_second{2018, 6, 21, 1, 2, 3}, zone, 203918);
        for (const auto input: {"+ 1s", "+ 1 s", "1 s left", "1  s  left"}) {
            AssertParse(input, now + TDuration::Seconds(1), now, zone);
        }
        for (const auto input:
             {
                 "+1s2m",
                 "+ 1s2m",
                 "+1 s2m",
                 "+ 1 s2m",
                 "+ 1 s 2m",
                 "+ 1 s 2 m",
             })
        {
            AssertParse(input, now + TDuration::Seconds(121), now, zone);
        }
        for (const auto input: {"- 1 s 2 m", "1  s  2  m  ago"}) {
            AssertParse(input, now - TDuration::Seconds(121), now, zone);
        }
    }

    Y_UNIT_TEST(SpacedUnitAliases)
    {
        const struct
        {
            TStringBuf Unit;
            ui64 Microseconds;
        } cases[] = {
            {"seconds", 1000000},
            {"second", 1000000},
            {"sec", 1000000},
            {"s", 1000000},
            {"minutes", 60000000},
            {"minute", 60000000},
            {"min", 60000000},
            {"m", 60000000},
            {"months", 2629800000000},
            {"month", 2629800000000},
            {"M", 2629800000000},
            {"msec", 1000},
            {"ms", 1000},
            {"hours", 3600000000},
            {"hour", 3600000000},
            {"hr", 3600000000},
            {"h", 3600000000},
            {"days", 86400000000},
            {"day", 86400000000},
            {"d", 86400000000},
            {"weeks", 604800000000},
            {"week", 604800000000},
            {"w", 604800000000},
            {"years", 31557600000000},
            {"year", 31557600000000},
            {"y", 31557600000000},
            {"usec", 1},
            {"us", 1},
            {"µs", 1},
        };

        const auto zone = NDatetime::GetUtcTimeZone();
        const auto now =
            At(cctz::civil_second{2018, 6, 21, 1, 2, 3}, zone, 203918);
        for (const auto& example: cases) {
            const auto span = TString("1 ") + example.Unit;
            const auto offset = TDuration::MicroSeconds(example.Microseconds);
            AssertParse(TString("+ ") + span, now + offset, now, zone);
            AssertParse(TString("- ") + span, now - offset, now, zone);
            AssertParse(span + " left", now + offset, now, zone);
            AssertParse(span + " ago", now - offset, now, zone);
        }
    }

    Y_UNIT_TEST(MalformedInputs)
    {
        for (const auto input:
             {
                 "today+1s",
                 "today-1s",
                 "1sleft",
                 "1sago",
                 "today + - 1s",
                 "today - 1s + 5m",
                 "2018/08/12 01:02:03.1234",
                 "2018/08/12 01:02:03",
                 "+1000000000d 100s",
                 "+100s 1000000000d",
                 "2018-08-09 07:06:05.123 4",
                 "2018-08-09 07:06:05.123a4",
                 "+5 bad",
                 "5 bad ago",
                 "today +5 bad",
                 "today -5s 6 bad",
             })
        {
            AssertReject(input);
        }
    }

    Y_UNIT_TEST(UnitlessValuesUseSeconds)
    {
        const auto zone = NDatetime::GetUtcTimeZone();
        const auto now =
            At(cctz::civil_second{2018, 6, 21, 1, 2, 3}, zone, 203918);
        // systemd treats every unitless component as seconds.
        AssertParse("+5", now + TDuration::Seconds(5), now, zone);
        AssertParse("5 ago", now - TDuration::Seconds(5), now, zone);
        AssertParse("+ 1 1s", now + TDuration::Seconds(2), now, zone);
        AssertParse("+ 4m 1 1s", now + TDuration::Seconds(242), now, zone);
    }

    Y_UNIT_TEST(TimestampModifiers)
    {
        const struct
        {
            TStringBuf Input;
            TStringBuf Base;
        } cases[] = {
            {"today", "2018-06-21T00:00:00Z"},
            {"tomorrow", "2018-06-22T00:00:00Z"},
            {"yesterday", "2018-06-20T00:00:00Z"},
            {"epoch", "1970-01-01T00:00:00Z"},
            {"now", "2018-06-21T01:02:03.203918Z"},
            {"2018-08-09 07:06:05", "2018-08-09T07:06:05Z"},
            {"18-08-09 07:06:05", "2018-08-09T07:06:05Z"},
            {"2018-08-09 07:06", "2018-08-09T07:06:00Z"},
            {"18-08-09 07:06", "2018-08-09T07:06:00Z"},
            {"2018-08-09", "2018-08-09T00:00:00Z"},
            {"18-08-09", "2018-08-09T00:00:00Z"},
            {"10:11:12", "2018-06-21T10:11:12Z"},
            {"10:11", "2018-06-21T10:11:00Z"},
        };

        const auto zone = NDatetime::GetUtcTimeZone();
        const auto now =
            At(cctz::civil_second{2018, 6, 21, 1, 2, 3}, zone, 203918);
        for (const auto& example: cases) {
            const auto base = TInstant::ParseIso8601(example.Base);
            for (const auto seconds: {1, 121}) {
                const TString span = seconds == 1 ? "1s" : "1s2m";
                const auto offset = TDuration::Seconds(seconds);
                AssertParse(
                    TString(example.Input) + " +" + span,
                    base + offset,
                    now,
                    zone);
                const auto negative = TString(example.Input) + " -" + span;
                if (base < TInstant::Seconds(seconds)) {
                    AssertReject(negative);
                } else {
                    AssertParse(negative, base - offset, now, zone);
                }
            }
        }
    }

    Y_UNIT_TEST(EpochSpans)
    {
        const auto zone = NDatetime::GetTimeZone("Asia/Shanghai");
        const auto now = TInstant::ParseIso8601("2018-06-21T01:02:03.203918Z");

        const struct
        {
            TStringBuf Input;
            ui64 Microseconds;
        } cases[] = {
            {"epoch", 0},
            {"@", 0},
            {"@1s", 1000000},
            {"@1s 2m", 121000000},
            {"@ 1 s", 1000000},
            {"@  1s", 1000000},
            {"@1h2m", 3720000000},
            {"@0.000001s", 1},
            {"@1s 500ms", 1500000},
            {"@ 1 1s", 2000000},
            {"@1s +1s", 2000000},
            {"@1s -1s", 0},
        };

        for (const auto& example: cases) {
            AssertParse(
                example.Input,
                TInstant::MicroSeconds(example.Microseconds),
                now,
                zone);
        }
    }

    Y_UNIT_TEST(ModifierWhitespaceAndUnitlessValues)
    {
        const auto zone = NDatetime::GetUtcTimeZone();
        const auto now =
            At(cctz::civil_second{2018, 6, 21, 1, 2, 3}, zone, 203918);
        const auto today = At(cctz::civil_second{2018, 6, 21}, zone);
        for (const auto input:
             {"today +1s", "today + 1s", "today +1 s", "today + 1 s"})
        {
            AssertParse(input, today + TDuration::Seconds(1), now, zone);
        }
        for (const auto input:
             {
                 "today +1s2m",
                 "today + 1s2m",
                 "today +1 s2m",
                 "today + 1 s2m",
                 "today + 1 s 2m",
                 "today + 1 s 2 m",
                 "today\t+\t1 s 2 m",
             })
        {
            AssertParse(input, today + TDuration::Seconds(121), now, zone);
        }
        AssertParse("today +5", today + TDuration::Seconds(5), now, zone);
        AssertParse("today -5s 6", today - TDuration::Seconds(11), now, zone);
        AssertParse("today + 1s 2s", today + TDuration::Seconds(3), now, zone);
        AssertParse(
            "today + 1s 4m 2s",
            today + TDuration::Seconds(243),
            now,
            zone);
        AssertParse("today + 1 1s", today + TDuration::Seconds(2), now, zone);
        AssertParse(
            "today + 4m 1 1s",
            today + TDuration::Seconds(242),
            now,
            zone);
        AssertParse("now +0.5s", now + TDuration::MilliSeconds(500), now, zone);
    }

    Y_UNIT_TEST(ModifierTimezones)
    {
        const auto zone = NDatetime::GetTimeZone("Asia/Shanghai");
        const auto now = TInstant::ParseIso8601("2018-06-21T01:02:03.203918Z");

        const struct
        {
            TStringBuf Input;
            TStringBuf Expected;
        } cases[] = {
            {"2018-08-09 07:06:05 +1s", "2018-08-08T23:06:06Z"},
            {"2018-08-09T07:06:05+05:30 -1h", "2018-08-09T00:36:05Z"},
            {"2018-08-09 07:06:05 +05:30 +1h", "2018-08-09T02:36:05Z"},
            {"2018-08-09 07:06:05 -0530 +1s", "2018-08-09T12:36:06Z"},
            {"2018-08-09 07:06:05 UTC +1s", "2018-08-09T07:06:06Z"},
            {"2018-08-09 07:06:05 Asia/Tokyo -1s", "2018-08-08T22:06:04Z"},
            {"Thu 2018-08-09T07:06:05.123Z +1us",
             "2018-08-09T07:06:05.123001Z"},
            {"today UTC +1s", "2018-06-21T00:00:01Z"},
            {"10:11:12 UTC -1s", "2018-06-21T10:11:11Z"},
            {"today +05", "2018-06-20T19:00:00Z"},
            {"today -05", "2018-06-20T05:00:00Z"},
            {"today +05 +1s", "2018-06-20T19:00:01Z"},
            {"today +5", "2018-06-20T16:00:05Z"},
        };

        for (const auto& example: cases) {
            AssertParse(
                example.Input,
                TInstant::ParseIso8601(example.Expected),
                now,
                zone);
        }
    }

    Y_UNIT_TEST(ModifiersAcrossDaylightSavingTransitions)
    {
        const auto zone = NDatetime::GetUtcTimeZone();
        const auto now = TInstant::ParseIso8601("2024-03-31T10:00:00Z");

        const struct
        {
            TStringBuf Input;
            TStringBuf Expected;
        } cases[] = {
            {"2024-03-31 01:30 Europe/Berlin +1h", "2024-03-31T01:30:00Z"},
            {"2024-10-27 02:30 Europe/Berlin +1h", "2024-10-27T01:30:00Z"},
            {"today Europe/Berlin +1day", "2024-03-31T23:00:00Z"},
            {"tomorrow Europe/Berlin -1day", "2024-03-30T22:00:00Z"},
        };

        for (const auto& example: cases) {
            AssertParse(
                example.Input,
                TInstant::ParseIso8601(example.Expected),
                now,
                zone);
        }
    }

    Y_UNIT_TEST(ModifierArithmeticBounds)
    {
        const auto zone = NDatetime::GetUtcTimeZone();
        const auto now = TInstant::ParseIso8601("2018-06-21T01:02:03.203918Z");
        AssertParse("epoch +0s", TInstant::Zero(), now, zone);
        AssertParse("epoch -0s", TInstant::Zero(), now, zone);
        AssertParse(
            "epoch +18446744073709551615us",
            TInstant::Max(),
            now,
            zone);
        AssertParse(
            "@18446744073709551615us -1us",
            TInstant::Max() - TDuration::MicroSeconds(1),
            now,
            zone);
        TInstant result = now;
        UNIT_ASSERT(
            !TryParseTimestamp("now +1us", result, TInstant::Max(), zone));
        UNIT_ASSERT_VALUES_EQUAL(result, now);
        UNIT_ASSERT(
            !TryParseTimestamp("now -1us", result, TInstant::Zero(), zone));
        UNIT_ASSERT_VALUES_EQUAL(result, now);
        for (const auto input:
             {
                 "epoch -1s",
                 "@0 -1us",
                 "@18446744073709551615us +1us",
                 "epoch +18446744073709551615us 1us",
             })
        {
            AssertReject(input);
        }
    }

    Y_UNIT_TEST(RejectsMalformedModifiers)
    {
        for (const auto input:
             {
                 "now+1s",
                 "today-1s",
                 "today +",
                 "today -",
                 "today + - 1s",
                 "now +1s +2m",
                 "now -1s +2m",
                 "now +1s -2m",
                 "+1s +2m",
                 "@1s +2s -1s",
                 "today +1unknown",
                 "today +1ns",
                 "today +1s ago",
                 "now UTC +1s",
                 "Wed 2018-08-09 07:06:05 +1s",
                 "garbage +1s",
                 "@-1s",
                 "@+1s",
                 "@ 1s garbage",
                 "@1s UTC",
             })
        {
            AssertReject(input);
        }
    }
}

}   // namespace NSystemdTime
