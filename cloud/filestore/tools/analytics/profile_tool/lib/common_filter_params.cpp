#include "common_filter_params.h"

#include <cloud/storage/core/libs/systemd_time/timestamp.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/ascii.h>
#include <util/string/cast.h>

#include <algorithm>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FileSystemIdLabel = "fs-id";
constexpr TStringBuf NodeIdLabel = "node-id";
constexpr TStringBuf HandleLabel = "handle";
constexpr TStringBuf SinceLabel = "since";
constexpr TStringBuf UntilLabel = "until";
constexpr TStringBuf GrafanaRangeLabel = "grafana-range";

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TMaybe<T> Parse(
    TStringBuf label,
    const NLastGetopt::TOptsParseResultException& parseResult)
{
    if (!parseResult.Has(label.data())) {
        return {};
    }

    return parseResult.Get<T>(label.data());
}

TMaybe<TInstant> ParseTimestamp(
    TStringBuf label,
    const NLastGetopt::TOptsParseResultException& parseResult,
    TInstant now)
{
    if (!parseResult.Has(label.data())) {
        return {};
    }

    const auto res = parseResult.Get<TString>(label.data());
    TInstant ts;
    if (!NSystemdTime::TryParseTimestamp(
            res,
            ts,
            now,
            NDatetime::GetUtcTimeZone()))
    {
        Cerr << "Failed to parse time format: " << res << Endl;
        Cerr << "Parameter \"" << label << "\" will be ignored" << Endl;
        return {};
    }

    return ts;
}

bool TryRoundGrafanaTimestamp(
    TInstant current,
    char unit,
    bool roundUp,
    TInstant& result)
{
    const auto utc = NDatetime::GetUtcTimeZone();
    const cctz::time_point<cctz::seconds> reference{
        cctz::seconds(current.Seconds())};
    const auto civil = utc.lookup(reference).cs;
    cctz::civil_second start;
    cctz::civil_second end;
    switch (unit) {
        case 's':
            start = civil;
            end = start + 1;
            break;
        case 'm':
            start = cctz::civil_second(cctz::civil_minute(civil));
            end = cctz::civil_second(cctz::civil_minute(civil) + 1);
            break;
        case 'h':
            start = cctz::civil_second(cctz::civil_hour(civil));
            end = cctz::civil_second(cctz::civil_hour(civil) + 1);
            break;
        case 'd':
            start = cctz::civil_second(cctz::civil_day(civil));
            end = cctz::civil_second(cctz::civil_day(civil) + 1);
            break;
        case 'w': {
            // Copied ranges omit Grafana's locale/week-start preference.
            const auto day = cctz::prev_weekday(
                cctz::civil_day(civil) + 1,
                cctz::weekday::sunday);
            start = cctz::civil_second(day);
            end = cctz::civil_second(day + 7);
            break;
        }
        case 'M':
            start = cctz::civil_second(cctz::civil_month(civil));
            end = cctz::civil_second(cctz::civil_month(civil) + 1);
            break;
        case 'Q': {
            const cctz::civil_month month(
                civil.year(),
                (civil.month() - 1) / 3 * 3 + 1);
            start = cctz::civil_second(month);
            end = cctz::civil_second(month + 3);
            break;
        }
        case 'y':
            start = cctz::civil_second(cctz::civil_year(civil));
            end = cctz::civil_second(cctz::civil_year(civil) + 1);
            break;
        default:
            return false;
    }

    // Grafana's endOf() uses millisecond precision.
    const auto seconds =
        utc.lookup(roundUp ? end : start).pre.time_since_epoch().count() -
        (roundUp ? 1 : 0);
    const ui64 fraction = roundUp ? 999000 : 0;
    if (seconds < 0 ||
        static_cast<ui64>(seconds) >
            (TInstant::Max().MicroSeconds() - fraction) / 1000000)
    {
        return false;
    }
    result = TInstant::MicroSeconds(seconds * 1000000ULL + fraction);
    return true;
}

bool TryParseGrafanaTimestamp(
    TStringBuf input,
    TInstant now,
    bool roundUp,
    TInstant& result)
{
    if (!input.SkipPrefix("now")) {
        return TInstant::TryParseIso8601(input, result);
    }

    auto current = now;
    const auto utc = NDatetime::GetUtcTimeZone();
    while (!input.empty()) {
        const auto operation = input;
        const char sign = input.front();
        if (sign != '+' && sign != '-' && sign != '/') {
            return false;
        }
        input.Skip(1);
        size_t digits = 0;
        while (digits < input.size() && IsAsciiDigit(input[digits])) {
            ++digits;
        }
        ui64 count = 1;
        if ((!digits && sign != '/') ||
            (digits && !TryFromString(input.Head(digits), count)))
        {
            return false;
        }
        input.Skip(digits);
        // Copied JSON omits the fiscal-year start month. Use Grafana's
        // January default, so fiscal quarters/years match calendar periods.
        if (sign == '/' && input.SkipPrefix("f")) {
            if (input.empty() || (input.front() != 'Q' && input.front() != 'y'))
            {
                return false;
            }
        }
        if (input.empty()) {
            return false;
        }
        const char unit = input.front();
        input.Skip(1);
        if (sign == '/') {
            if (count != 1 ||
                !TryRoundGrafanaTimestamp(current, unit, roundUp, current))
            {
                return false;
            }
            continue;
        }
        if (unit == 'M' || unit == 'Q' || unit == 'y') {
            // Grafana uses calendar months, unlike systemd's fixed 30.44 days.
            const ui64 monthsPerUnit = unit == 'y' ? 12 : unit == 'Q' ? 3 : 1;
            if (count > 120000 / monthsPerUnit) {
                return false;
            }
            const cctz::time_point<cctz::seconds> reference{
                cctz::seconds(current.Seconds())};
            const auto civil = utc.lookup(reference).cs;
            const i64 months = count * monthsPerUnit;
            const auto month =
                cctz::civil_month(civil) + (sign == '-' ? -months : months);
            if (month.year() < 1970 || month.year() > 9999) {
                return false;
            }
            const auto lastDay = (cctz::civil_day(month + 1) - 1).day();
            const cctz::civil_second shifted(
                month.year(),
                month.month(),
                std::min(civil.day(), lastDay),
                civil.hour(),
                civil.minute(),
                civil.second());
            current = TInstant::Seconds(
                          utc.lookup(shifted).pre.time_since_epoch().count()) +
                      TDuration::MicroSeconds(current.MicroSecondsOfSecond());
        } else {
            if (unit != 's' && unit != 'm' && unit != 'h' && unit != 'd' &&
                unit != 'w')
            {
                return false;
            }
            if (!NSystemdTime::TryParseTimestamp(
                    operation.Head(operation.size() - input.size()),
                    current,
                    current,
                    utc))
            {
                return false;
            }
        }
    }
    result = current;
    return true;
}

struct TTimeRange
{
    TInstant Since;
    TInstant Until;
};

TTimeRange ParseGrafanaRange(
    const NLastGetopt::TOptsParseResultException& parseResult,
    TInstant now)
{
    if (parseResult.Has(SinceLabel.data()) ||
        parseResult.Has(UntilLabel.data()))
    {
        ythrow NLastGetopt::TUsageException()
            << "--grafana-range cannot be combined with --since or --until";
    }
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(
            parseResult.Get<TString>(GrafanaRangeLabel.data()),
            &json) ||
        !json.IsMap() || !json["from"].IsString() || !json["to"].IsString())
    {
        ythrow NLastGetopt::TUsageException()
            << "--grafana-range requires a JSON object with string fields "
               "\"from\" and \"to\"";
    }
    TTimeRange range;
    if (!TryParseGrafanaTimestamp(
            json["from"].GetString(),
            now,
            false,
            range.Since))
    {
        ythrow NLastGetopt::TUsageException()
            << "Invalid --grafana-range \"from\": " << json["from"].GetString();
    }
    if (!TryParseGrafanaTimestamp(
            json["to"].GetString(),
            now,
            true,
            range.Until))
    {
        ythrow NLastGetopt::TUsageException()
            << "Invalid --grafana-range \"to\": " << json["to"].GetString();
    }
    if (range.Since > range.Until) {
        ythrow NLastGetopt::TUsageException()
            << "--grafana-range \"from\" must not be later than \"to\"";
    }
    return range;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommonFilterParams::TCommonFilterParams(
    NLastGetopt::TOpts& opts,
    TInstant referenceTime)
    : ReferenceTime(referenceTime)
{
    opts.AddLongOption(
            FileSystemIdLabel.data(),
            "FileSystemId, used for filtering")
        .RequiredArgument("STR");

    opts.AddLongOption(
            GrafanaRangeLabel.data(),
            "Time range copied from Grafana as JSON with string fields "
            "from/to. "
            "Accepts ISO 8601 or now with +/- offsets in s, m, h, d, w, M, Q, "
            "y "
            "(e.g. '{\"from\":\"now-15m\",\"to\":\"now\"}'). "
            "Supports /unit rounding in s, m, h, d, w, M, Q, y: start of "
            "period for from, final millisecond for to. "
            "Fiscal /fQ and /fy rounding assumes a January fiscal-year start. "
            "Calendar arithmetic and rounding use UTC; weeks start Sunday. "
            "Cannot be combined with --since or --until.")
        .RequiredArgument("JSON");

    opts.AddLongOption(NodeIdLabel.data(), "NodeId, used for filtering")
        .RequiredArgument("NUM");

    opts.AddLongOption(HandleLabel.data(), "Handle, used for filtering")
        .RequiredArgument("NUM");

    opts.AddLongOption(
            SinceLabel.data(),
            "Since timestamp, used for filtering. "
            "Format: systemd.time timestamp (e.g. '2026-10-01T12:00:00Z', "
            "'today', '-2h', '30min ago'; omitted timezone defaults to "
            "UTC). ")
        .RequiredArgument("STR");

    opts.AddLongOption(
            UntilLabel.data(),
            "Until timestamp, used for filtering. "
            "Format: systemd.time timestamp (e.g. '2026-10-01T12:00:00Z', "
            "'now', '+1h'; omitted timezone defaults to UTC). ")
        .RequiredArgument("STR");
}

TMaybe<TString> TCommonFilterParams::GetFileSystemId(
    const NLastGetopt::TOptsParseResultException& parseResult) const
{
    return Parse<TString>(FileSystemIdLabel, parseResult);
}

TMaybe<ui64> TCommonFilterParams::GetNodeId(
    const NLastGetopt::TOptsParseResultException& parseResult) const
{
    return Parse<ui64>(NodeIdLabel, parseResult);
}

TMaybe<ui64> TCommonFilterParams::GetHandle(
    const NLastGetopt::TOptsParseResultException& parseResult) const
{
    return Parse<ui64>(HandleLabel, parseResult);
}

TMaybe<TInstant> TCommonFilterParams::GetSince(
    const NLastGetopt::TOptsParseResultException& parseResult) const
{
    if (parseResult.Has(GrafanaRangeLabel.data())) {
        return ParseGrafanaRange(parseResult, ReferenceTime).Since;
    }
    return ParseTimestamp(SinceLabel, parseResult, ReferenceTime);
}

TMaybe<TInstant> TCommonFilterParams::GetUntil(
    const NLastGetopt::TOptsParseResultException& parseResult) const
{
    if (parseResult.Has(GrafanaRangeLabel.data())) {
        return ParseGrafanaRange(parseResult, ReferenceTime).Until;
    }
    return ParseTimestamp(UntilLabel, parseResult, ReferenceTime);
}

}   // namespace NCloud::NFileStore::NProfileTool
