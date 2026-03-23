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

    opts.AddLongOption("producer-threads", "number of producer threads")
        .RequiredArgument("NUM")
        .DefaultValue(4)
        .StoreResult(&ProducerThreads);

    opts.AddLongOption("stealer-threads", "number of stealer threads")
        .RequiredArgument("NUM")
        .DefaultValue(2)
        .StoreResult(&StealerThreads);

    opts.AddLongOption("files-per-producer", "max files per producer")
        .RequiredArgument("NUM")
        .DefaultValue(1000)
        .StoreResult(&FilesPerProducer);

    opts.AddLongOption("duration", "test duration in seconds")
        .RequiredArgument("NUM")
        .DefaultValue(60)
        .StoreResult(&TestDurationSec);

    opts.AddLongOption("file-size", "file size in bytes")
        .RequiredArgument("NUM")
        .DefaultValue(4096)
        .StoreResult(&FileSize);

    opts.AddLongOption(
            "report-path",
            "path to the file where the results will be written")
        .RequiredArgument("STR")
        .DefaultValue("bench-report.json")
        .StoreResult(&ReportPath);

    opts.AddLongOption(
            "mpi-cross-rank-stealing",
            "allow stealers to steal from producers on other MPI ranks "
            "(requires shared filesystem)")
        .NoArgument()
        .SetFlag(&MpiCrossRankStealing);

    TOptsParseResultException(&opts, argc, argv);
}

}   // namespace NCloud::NFileStore
