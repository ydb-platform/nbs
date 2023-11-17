#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

namespace NCloud::NBlockStore {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("file", "path to file or block device")
        .RequiredArgument("STR")
        .StoreResult(&FilePath);

    opts.AddLongOption("block-count", "table block count")
        .RequiredArgument("NUM")
        .StoreResult(&BlockCount);

    opts.AddLongOption("write-rate", "percentage of write requests")
        .RequiredArgument("NUM")
        .DefaultValue(50)
        .StoreResult(&WriteRate);

    opts.AddLongOption("iodepth")
        .RequiredArgument("NUM")
        .DefaultValue(32)
        .StoreResult(&IoDepth);

    opts.AddLongOption("request-count")
        .RequiredArgument("NUM")
        .DefaultValue(1'000'000)
        .StoreResult(&RequestCount);

    TOptsParseResultException(&opts, argc, argv);
}

}   // namespace NCloud::NBlockStore
