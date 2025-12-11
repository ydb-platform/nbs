#include "options.h"

#include <util/generic/serialized_enum.h>

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TOptions::TOptions(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("path").RequiredArgument("STR").Required().StoreResult(
        &Path);

    opts.AddLongOption("bs").RequiredArgument("NUM").StoreResultDef(&BlockSize);

    opts.AddLongOption("count").RequiredArgument("NUM").StoreResultDef(
        &BlockCount);

    opts.AddLongOption("skip").RequiredArgument("NUM").StoreResultDef(
        &StartIndex);

    opts.AddLongOption("test-id").RequiredArgument("NUM").StoreResultDef(
        &TestId);

    opts.AddLongOption("format")
        .RequiredArgument("{ " + GetEnumAllNames<EFormat>() + " }")
        .DefaultValue(ToString(EFormat::Text))
        .Handler1T<TString>([this](const auto& s)
                            { Format = FromString<EFormat>(s); });

    TOptsParseResultException(&opts, argc, argv);
}
