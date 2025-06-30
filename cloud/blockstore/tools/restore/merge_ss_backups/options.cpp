#include "options.h"

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TOptions::TOptions(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("src-root")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&SrcRoot);

    opts.AddLongOption("output")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&OutputPath);

    TOptsParseResultException(&opts, argc, argv);
}
