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

    opts.AddLongOption("files-per-producer", "max files per producer")
        .RequiredArgument("NUM")
        .DefaultValue(1000)
        .StoreResult(&FilesPerProducer);

    opts.AddLongOption(
            "unlink-percentage",
            "the probability (in %) to unlink a random file upon each producer"
            " iteration")
        .RequiredArgument("NUM")
        .DefaultValue(20)
        .StoreResult(&UnlinkPercentage);

    opts.AddLongOption(
            "producer-sleep-duration",
            "producer sleep duration between iterations")
        .RequiredArgument("DURATION")
        .DefaultValue(TDuration::MilliSeconds(1))
        .StoreResult(&ProducerSleepDuration);

    opts.AddLongOption("file-size", "file size in bytes")
        .RequiredArgument("NUM")
        .DefaultValue(4096)
        .StoreResult(&FileSize);

    //
    // Stealer settings.
    //

    opts.AddLongOption("stealer-threads", "number of stealer threads")
        .RequiredArgument("NUM")
        .DefaultValue(2)
        .StoreResult(&StealerThreads);

    opts.AddLongOption(
            "stealer-sleep-duration",
            "stealer sleep duration between iterations")
        .RequiredArgument("DURATION")
        .DefaultValue(TDuration::MilliSeconds(10))
        .StoreResult(&ProducerSleepDuration);

    //
    // Lister settings.
    //

    opts.AddLongOption("lister-threads", "number of lister threads")
        .RequiredArgument("NUM")
        .DefaultValue(0)
        .StoreResult(&ListerThreads);

    opts.AddLongOption(
            "lister-sleep-duration",
            "lister sleep duration between iterations")
        .RequiredArgument("DURATION")
        .DefaultValue(TDuration::MilliSeconds(10))
        .StoreResult(&ProducerSleepDuration);

    //
    // Common settings.
    //

    opts.AddLongOption("duration", "test duration")
        .RequiredArgument("DURATION")
        .DefaultValue(TDuration::Minutes(1))
        .StoreResult(&TestDuration);

    opts.AddLongOption(
            "report-path",
            "path to the file where the results will be written")
        .RequiredArgument("STR")
        .DefaultValue("bench-report.json")
        .StoreResult(&ReportPath);

    TOptsParseResultException(&opts, argc, argv);
}

}   // namespace NCloud::NFileStore
