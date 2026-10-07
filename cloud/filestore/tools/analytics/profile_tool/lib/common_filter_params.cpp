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

bool TryParseGrafanaTimestamp(TStringBuf input, TInstant now, TInstant& result)
{
    if (!input.SkipPrefix("now")) {
        return TInstant::TryParseIso8601(input, result);
    }

    auto current = now;
    const auto utc = NDatetime::GetUtcTimeZone();
    while (!input.empty()) {
        const auto operation = input;
        const char sign = input.front();
        if (sign != '+' && sign != '-') {
            return false;
        }
        input.Skip(1);
        size_t digits = 0;
        while (digits < input.size() && IsAsciiDigit(input[digits])) {
            ++digits;
        }
        ui64 count;
        if (!digits || !TryFromString(input.Head(digits), count)) {
            return false;
        }
        input.Skip(digits);
        if (input.empty()) {
            return false;
        }
        const char unit = input.front();
        input.Skip(1);
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
    if (!TryParseGrafanaTimestamp(json["from"].GetString(), now, range.Since)) {
        ythrow NLastGetopt::TUsageException()
            << "Invalid --grafana-range \"from\": " << json["from"].GetString();
    }
    if (!TryParseGrafanaTimestamp(json["to"].GetString(), now, range.Until)) {
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
            "Relative calendar arithmetic uses UTC. "
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
