#include "options.h"

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>

namespace NCloud::NFileStore::NUnsensitivifier {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption('h');

    opts.AddLongOption("in", "in file name")
        .RequiredArgument("STR")
        .StoreResult(&InFile);

    opts.AddLongOption("out", "out file name")
        .RequiredArgument("STR")
        .StoreResult(&OutFile);

    TString mode;
    opts.AddLongOption("mode", "Transform mode")
        .RequiredArgument("STR")
        .StoreResult(&mode)
        .DefaultValue("nodeid");

    TOptsParseResultException res(&opts, argc, argv);

    if (mode == "empty") {
        Mode = EMode::Empty;
    } else if (mode == "hash") {
        Mode = EMode::Hash;
    } else if (mode == "nodeid") {
        Mode = EMode::Nodeid;
    }
}

}   // namespace NCloud::NFileStore::NUnsensitivifier
