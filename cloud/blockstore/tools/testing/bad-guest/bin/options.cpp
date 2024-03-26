#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("file", "path to file or block device")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&FilePath);

    opts.AddLongOption("total-block-count", "total block count")
        .RequiredArgument("NUM")
        .Required()
        .StoreResult(&TotalBlockCount);

    opts.AddLongOption("chunk-block-count", "chunk block count")
        .RequiredArgument("NUM")
        .Required()
        .StoreResult(&ChunkBlockCount);

    opts.AddLongOption("block-size", "block size")
        .RequiredArgument("NUM")
        .DefaultValue(4_KB)
        .StoreResult(&BlockSize);

    TOptsParseResultException(&opts, argc, argv);
}

}   // namespace NCloud::NBlockStore
