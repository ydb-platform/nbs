#include "common_filter_params.h"

#include <library/cpp/getopt/small/last_getopt.h>
#include <cloud/storage/core/libs/systemd_time/timestamp.h>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FileSystemIdLabel = "fs-id";
constexpr TStringBuf NodeIdLabel = "node-id";
constexpr TStringBuf HandleLabel = "handle";
constexpr TStringBuf SinceLabel = "since";
constexpr TStringBuf UntilLabel = "until";

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
    if (!NSystemdTime::TryParseTimestamp(res, ts, now)) {
        Cerr << "Failed to parse time format: " << res << Endl;
        Cerr << "Parameter \"" << label << "\" will be ignored" << Endl;
        return {};
    }

    return ts;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommonFilterParams::TCommonFilterParams(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption(
            FileSystemIdLabel.data(),
            "FileSystemId, used for filtering")
        .RequiredArgument("STR");

    opts.AddLongOption(
            NodeIdLabel.data(),
            "NodeId, used for filtering")
        .RequiredArgument("NUM");

    opts.AddLongOption(
            HandleLabel.data(),
            "Handle, used for filtering")
        .RequiredArgument("NUM");

    opts.AddLongOption(
            SinceLabel.data(),
            "Since timestamp, used for filtering. "
            "Format: systemd.time timestamp (e.g. '2026-10-01T12:00:00Z', "
            "'today', '-2h', '30min ago'; omitted timezone defaults to "
            "local). ")
        .RequiredArgument("STR");

    opts.AddLongOption(
            UntilLabel.data(),
            "Until timestamp, used for filtering. "
            "Format: systemd.time timestamp (e.g. '2026-10-01T12:00:00Z', "
            "'now', '+1h'; omitted timezone defaults to local). ")
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
    return ParseTimestamp(SinceLabel, parseResult, ReferenceTime);
}

TMaybe<TInstant> TCommonFilterParams::GetUntil(
    const NLastGetopt::TOptsParseResultException& parseResult) const
{
    return ParseTimestamp(UntilLabel, parseResult, ReferenceTime);
}

}   // namespace NCloud::NFileStore::NProfileTool
