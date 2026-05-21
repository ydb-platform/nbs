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
        .StoreResult(&FilePath)
        .DefaultValue("/dev/vdb");

    opts.AddLongOption("filesize", "size of file or block device")
        .RequiredArgument("NUM")
        .StoreResult(&FileSize)
        .Required();

    opts.AddLongOption("offset")
        .RequiredArgument("NUM")
        .StoreResult(&Offset)
        .DefaultValue(0);

    opts.AddLongOption("step", "only each <step> block will be written/read")
        .RequiredArgument("NUM")
        .StoreResult(&Step)
        .DefaultValue(1);

    opts.AddLongOption("blocksize")
        .RequiredArgument("NUM")
        .StoreResult(&BlockSize)
        .DefaultValue(512);

    opts.AddLongOption("iodepth")
        .RequiredArgument("NUM")
        .StoreResult(&IoDepth)
        .DefaultValue(8);

    opts.AddLongOption("zero-check")
        .StoreTrue(&CheckZero);

    opts.AddLongOption("read-only", "perform only read stage without write")
        .StoreTrue(&ReadOnly);

    opts.AddLongOption("verbose", "output level for diagnostics messages")
        .OptionalArgument("STR")
        .StoreResult(&VerboseLevel);

    TOptsParseResultException res(&opts, argc, argv);

    if (res.FindLongOptParseResult("verbose") && !VerboseLevel) {
        VerboseLevel = "debug";
    }
}

}   // namespace NCloud::NBlockStore
