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
        .DefaultValue(ClientId)
        .StoreResult(&ClientId);

    opts.AddLongOption(
            "request-timeout",
            "request timeout in milliseconds sent in request headers")
        .RequiredArgument("NUM")
        .StoreResult(&RequestTimeoutMs);

    opts.AddLongOption("device-uuid", "device to load")
        .RequiredArgument("STR")
        .StoreResult(&DeviceUUID);

    opts.AddLongOption("generation", "writer generation for AcquireDevices")
        .RequiredArgument("NUM")
        .StoreResult(&Generation);

    opts.AddLongOption(
            "no-acquire",
            "do not acquire / release the device around the run")
        .NoArgument()
        .SetFlag(&NoAcquire);

    opts.AddLongOption("iodepth", "requests in flight")
        .RequiredArgument("NUM")
        .DefaultValue(IoDepth)
        .StoreResult(&IoDepth);

    ui64 durationSeconds = 0;
    opts.AddLongOption("duration", "run for this many seconds")
        .RequiredArgument("NUM")
        .StoreResult(&durationSeconds);

    opts.AddLongOption("requests", "run this many requests")
        .RequiredArgument("NUM")
        .StoreResult(&Requests);

    opts.AddLongOption("page-size", "logical page size in bytes")
        .RequiredArgument("NUM")
        .DefaultValue(PageSize)
        .StoreResult(&PageSize);

    opts.AddLongOption(
            "page-count",
            "requests address pages [0, NUM) of the device")
        .RequiredArgument("NUM")
        .DefaultValue(PageCount)
        .StoreResult(&PageCount);

    opts.AddLongOption("write-pages", "pages per WriteLogRecord")
        .RequiredArgument("NUM")
        .DefaultValue(WritePages)
        .StoreResult(&WritePages);

    opts.AddLongOption("read-pages", "pages per ReadPages")
        .RequiredArgument("NUM")
        .DefaultValue(ReadPages)
        .StoreResult(&ReadPages);

    opts.AddLongOption(
            "read-percent",
            "share of ReadPages requests, 0..100; the rest are "
            "WriteLogRecord")
        .RequiredArgument("NUM")
        .DefaultValue(ReadPercent)
        .StoreResult(&ReadPercent);

    opts.AddLongOption(
            "advance-every",
            "advance the lsn low watermark every NUM written records "
            "(0 disables)")
        .RequiredArgument("NUM")
        .DefaultValue(AdvanceEvery)
        .StoreResult(&AdvanceEvery);

    opts.AddLongOption("name", "test name in the results")
        .RequiredArgument("STR")
        .DefaultValue(Name)
        .StoreResult(&Name);

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

    Duration = TDuration::Seconds(durationSeconds);
    ReportInterval = TDuration::Seconds(reportSeconds);

    if (!Port) {
        ythrow TUsageException() << "--port is required";
    }

    Validate();
}

void TOptions::Validate() const
{
    if (!DeviceUUID) {
        ythrow TUsageException() << "--device-uuid is required";
    }
    if (!IoDepth) {
        ythrow TUsageException() << "--iodepth must be positive";
    }
    if (!Duration && !Requests) {
        ythrow TUsageException()
            << "either --duration or --requests must be set";
    }
    if (!PageSize) {
        ythrow TUsageException() << "--page-size must be positive";
    }
    if (!WritePages || !ReadPages) {
        ythrow TUsageException()
            << "--write-pages and --read-pages must be positive";
    }
    if (PageCount < Max<ui64>(WritePages, ReadPages)) {
        ythrow TUsageException()
            << "--page-count must be at least --write-pages and "
               "--read-pages";
    }
    if (ReadPercent > 100) {
        ythrow TUsageException() << "--read-percent must be 0..100";
    }
}

}   // namespace NCloud::NFastShard::NLoadTest
