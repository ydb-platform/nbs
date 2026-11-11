#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/algorithm.h>

namespace NCloud::NFastShard::NLoadTest {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, const char* argv[])
{
    TOpts opts;
    opts.AddHelpOption('h');
    opts.AddVersionOption();
    opts.SetFreeArgsNum(0);
    opts.SetTitle(
        "Load generator for the fastshard storage node protocol: "
        "WriteLogRecord / ReadPages against one device");

    opts.AddLongOption("host", "storage node host")
        .RequiredArgument("STR")
        .DefaultValue(Host)
        .StoreResult(&Host);

    opts.AddLongOption("port", "storage node port")
        .RequiredArgument("NUM")
        .StoreResult(&Port);

    opts.AddLongOption("client-id", "ClientId sent in request headers")
        .RequiredArgument("STR")
        .DefaultValue(Test.ClientId)
        .StoreResult(&Test.ClientId);

    opts.AddLongOption(
            "request-timeout",
            "request timeout in milliseconds sent in request headers")
        .RequiredArgument("NUM")
        .StoreResult(&Test.RequestTimeoutMs);

    opts.AddLongOption("device-uuid", "device to load")
        .RequiredArgument("STR")
        .StoreResult(&Test.DeviceUUID);

    opts.AddLongOption("generation", "writer generation for AcquireDevices")
        .RequiredArgument("NUM")
        .StoreResult(&Test.Generation);

    opts.AddLongOption(
            "no-acquire",
            "do not acquire / release the device around the run")
        .NoArgument()
        .SetFlag(&Test.NoAcquire);

    opts.AddLongOption("iodepth", "requests in flight")
        .RequiredArgument("NUM")
        .DefaultValue(Test.IoDepth)
        .StoreResult(&Test.IoDepth);

    ui64 durationSeconds = 0;
    opts.AddLongOption("duration", "run for this many seconds")
        .RequiredArgument("NUM")
        .StoreResult(&durationSeconds);

    opts.AddLongOption("requests", "run this many requests")
        .RequiredArgument("NUM")
        .StoreResult(&Test.Requests);

    opts.AddLongOption("page-size", "logical page size in bytes")
        .RequiredArgument("NUM")
        .DefaultValue(Test.PageSize)
        .StoreResult(&Test.PageSize);

    opts.AddLongOption(
            "page-count",
            "requests address pages [0, NUM) of the device")
        .RequiredArgument("NUM")
        .DefaultValue(Test.PageCount)
        .StoreResult(&Test.PageCount);

    opts.AddLongOption("write-pages", "pages per WriteLogRecord")
        .RequiredArgument("NUM")
        .DefaultValue(Test.WritePages)
        .StoreResult(&Test.WritePages);

    opts.AddLongOption("read-pages", "pages per ReadPages")
        .RequiredArgument("NUM")
        .DefaultValue(Test.ReadPages)
        .StoreResult(&Test.ReadPages);

    opts.AddLongOption(
            "read-percent",
            "share of ReadPages requests, 0..100; the rest are "
            "WriteLogRecord")
        .RequiredArgument("NUM")
        .DefaultValue(Test.ReadPercent)
        .StoreResult(&Test.ReadPercent);

    opts.AddLongOption(
            "advance-every",
            "advance the lsn low watermark every NUM written records "
            "(0 disables)")
        .RequiredArgument("NUM")
        .DefaultValue(Test.AdvanceEvery)
        .StoreResult(&Test.AdvanceEvery);

    ui64 retrySeconds = RetryTimeout.Seconds();
    opts.AddLongOption(
            "retry-timeout",
            "keep retrying a request that fails with a retriable error "
            "(E_REJECTED, E_TIMEOUT, ...) for up to NUM seconds (0 disables "
            "retries)")
        .RequiredArgument("NUM")
        .DefaultValue(retrySeconds)
        .StoreResult(&retrySeconds);

    opts.AddLongOption("name", "test name in the results")
        .RequiredArgument("STR")
        .DefaultValue(Test.Name)
        .StoreResult(&Test.Name);

    opts.AddLongOption(
            "results",
            "write the results as JSON to this file (stdout if not "
            "specified)")
        .RequiredArgument("STR")
        .StoreResult(&ResultsFile);

    ui64 reportSeconds = 0;
    opts.AddLongOption(
            "report-interval",
            "print progress to stderr every NUM seconds (0 disables)")
        .RequiredArgument("NUM")
        .StoreResult(&reportSeconds);

    opts.AddLongOption("verbose", "enable silk debug logging")
        .NoArgument()
        .SetFlag(&Verbose);

    TOptsParseResultException parseResult(&opts, argc, argv);

    Test.Duration = TDuration::Seconds(durationSeconds);
    Test.ReportInterval = TDuration::Seconds(reportSeconds);
    RetryTimeout = TDuration::Seconds(retrySeconds);

    if (!Port) {
        ythrow TUsageException() << "--port is required";
    }

    Validate();
}

void TOptions::Validate() const
{
    if (!Test.DeviceUUID) {
        ythrow TUsageException() << "--device-uuid is required";
    }
    if (!Test.IoDepth) {
        ythrow TUsageException() << "--iodepth must be positive";
    }
    if (!Test.Duration && !Test.Requests) {
        ythrow TUsageException()
            << "either --duration or --requests must be set";
    }
    if (!Test.PageSize) {
        ythrow TUsageException() << "--page-size must be positive";
    }
    if (!Test.WritePages || !Test.ReadPages) {
        ythrow TUsageException()
            << "--write-pages and --read-pages must be positive";
    }
    if (Test.PageCount < Max<ui64>(Test.WritePages, Test.ReadPages)) {
        ythrow TUsageException()
            << "--page-count must be at least --write-pages and "
               "--read-pages";
    }
    if (Test.ReadPercent > 100) {
        ythrow TUsageException() << "--read-percent must be 0..100";
    }
}

}   // namespace NCloud::NFastShard::NLoadTest
