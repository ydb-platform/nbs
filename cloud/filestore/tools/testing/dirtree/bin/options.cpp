#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

namespace NCloud::NFileStore {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("test-dir", "path to test directory")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&TestDir);

    //
    // Producer settings.
    //

    opts.AddLongOption("producer-threads", "number of producer threads")
        .RequiredArgument("NUM")
        .DefaultValue(4)
        .StoreResult(&ProducerThreads);

    opts.AddLongOption("children-count", "file and subdir children per dir")
        .RequiredArgument("NUM")
        .DefaultValue(10)
        .StoreResult(&ChildrenCount);

    opts.AddLongOption("symlink-children-count", "symlink children per dir")
        .RequiredArgument("NUM")
        .DefaultValue(2)
        .StoreResult(&SymlinkChildrenCount);

    opts.AddLongOption(
            "subdir-probability",
            "the probability of creating a subdir as a child")
        .RequiredArgument("NUM")
        .DefaultValue(0.5)
        .StoreResult(&SubdirProbability);

    opts.AddLongOption(
            "decay-factor",
            "the factor by which subdir-probability is multiplied upon each"
            " level of hierarchy")
        .RequiredArgument("NUM")
        .DefaultValue(0.15)
        .StoreResult(&DecayFactor);

    opts.AddLongOption(
            "seed",
            "the seed for the rng used to decide whether we should create a"
            " subdirectory or a file")
        .RequiredArgument("NUM")
        .DefaultValue(0)
        .StoreResult(&Seed);

    TOptsParseResultException(&opts, argc, argv);
}

}   // namespace NCloud::NFileStore
