#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

namespace NCloud::NFileStore {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    TString mode;
    opts.AddLongOption("mode", "bench, multi-client or sync-hub")
        .RequiredArgument("STR")
        .DefaultValue("bench")
        .StoreResult(&mode);

    opts.AddLongOption(
            "test-dir",
            "path to test directory (bench and multi-client modes)")
        .RequiredArgument("STR")
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

    opts.AddLongOption(
            "create-file-attempt-count",
            "the number of attempts to create+write each file, only EBADF is"
            " retried")
        .RequiredArgument("NUM")
        .DefaultValue(1)
        .StoreResult(&CreateFileAttemptCount);

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
        .StoreResult(&StealerSleepDuration);

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
        .StoreResult(&ListerSleepDuration);

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

    opts.AddLongOption(
            "skip-file-validation",
            "skips file ino, size and content validation")
        .NoArgument()
        .SetFlag(&SkipFileValidation);

    //
    // MultiClient and SyncHub settings.
    //

    opts.AddLongOption("instance-id", "multi-client instance id")
        .RequiredArgument("NUM")
        .DefaultValue(0)
        .StoreResult(&InstanceId);

    opts.AddLongOption("instance-count", "number of multi-client instances")
        .RequiredArgument("NUM")
        .DefaultValue(2)
        .StoreResult(&InstanceCount);

    TString syncAddress;
    opts.AddLongOption(
            "sync-address",
            "hub address (multi-client mode), HOST:PORT")
        .RequiredArgument("STR")
        .StoreResult(&syncAddress);

    opts.AddLongOption("sync-port", "port to listen on (sync-hub mode)")
        .RequiredArgument("NUM")
        .StoreResult(&SyncPort);

    opts.AddLongOption(
            "sync-timeout",
            "max wait for the other instances at each barrier")
        .RequiredArgument("DURATION")
        .DefaultValue(TDuration::Minutes(5))
        .StoreResult(&SyncTimeout);

    opts.AddLongOption("rounds", "number of modify + check rounds")
        .RequiredArgument("NUM")
        .DefaultValue(10)
        .StoreResult(&Rounds);

    opts.AddLongOption("files-per-round", "files created per round")
        .RequiredArgument("NUM")
        .DefaultValue(100)
        .StoreResult(&FilesPerRound);

    opts.AddLongOption(
            "rename-percentage",
            "the probability (in %) to rename each existing file in a round")
        .RequiredArgument("NUM")
        .DefaultValue(20)
        .StoreResult(&RenamePercentage);

    opts.AddLongOption(
            "check-delay",
            "min delay between the end of a modification phase and the"
            " following check phase")
        .RequiredArgument("DURATION")
        .DefaultValue(TDuration::Seconds(1))
        .StoreResult(&CheckDelay);

    TOptsParseResultException(&opts, argc, argv);

    if (mode == "bench") {
        Mode = EMode::Bench;
    } else if (mode == "multi-client") {
        Mode = EMode::MultiClient;
    } else if (mode == "sync-hub") {
        Mode = EMode::SyncHub;
    } else {
        ythrow yexception() << "unknown mode: " << mode;
    }

    if (Mode != EMode::SyncHub && !TestDir) {
        ythrow yexception() << "--test-dir is required";
    }

    if (Mode == EMode::MultiClient) {
        TStringBuf host;
        TStringBuf port;
        if (!TStringBuf(syncAddress).TryRSplit(':', host, port) || !host) {
            ythrow yexception() << "--sync-address HOST:PORT is required";
        }
        SyncHost = TString(host);
        SyncPort = FromString<ui16>(port);

        if (InstanceId >= InstanceCount) {
            ythrow yexception() << "--instance-id should be less than"
                << " --instance-count";
        }
    }

    if (Mode == EMode::SyncHub && !SyncPort) {
        ythrow yexception() << "--sync-port is required";
    }
}

}   // namespace NCloud::NFileStore
