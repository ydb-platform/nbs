#include "common_filter_params.h"

#include <library/cpp/getopt/small/last_getopt.h>

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
    if (!parseResult.Has(label.Data())) {
        return {};
    }

    return parseResult.Get<T>(label.Data());
}

template <>
TMaybe<TInstant> Parse(
    TStringBuf label,
    const NLastGetopt::TOptsParseResultException& parseResult)
{
    if (!parseResult.Has(label.Data())) {
        return {};
    }

    const auto res = parseResult.Get<TString>(label.Data());
    TInstant ts;
    if (!TInstant::TryParseIso8601(res, ts)) {
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
            FileSystemIdLabel.Data(),
            "FileSystemId, used for filtering")
        .RequiredArgument("STR");

    opts.AddLongOption(
            NodeIdLabel.Data(),
            "NodeId, used for filtering")
        .RequiredArgument("NUM");

    opts.AddLongOption(
            HandleLabel.Data(),
            "Handle, used for filtering")
        .RequiredArgument("NUM");

    opts.AddLongOption(
            SinceLabel.Data(),
            "Since timestamp, used for filtering. "
            "Format: YYYY-MM-DDThh:mm:ss (https://www.iso.org/standard/40874.html)")
        .RequiredArgument("STR");

    opts.AddLongOption(
            UntilLabel.Data(),
            "Until timestamp, used for filtering. "
            "Format: YYYY-MM-DDThh:mm:ss (https://www.iso.org/standard/40874.html)")
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
    return Parse<TInstant>(SinceLabel, parseResult);
}

TMaybe<TInstant> TCommonFilterParams::GetUntil(
    const NLastGetopt::TOptsParseResultException& parseResult) const
{
    return Parse<TInstant>(UntilLabel, parseResult);
}

}   // namespace NCloud::NFileStore::NProfileTool
