#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/stream/file.h>

namespace NCloud::NBlockStore::NLoadTest {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("test-name")
        .RequiredArgument("STR")
        .StoreResult(&TestName);

    opts.AddLongOption("client-config")
        .RequiredArgument("STR")
        .StoreResult(&ClientConfig);

    opts.AddLongOption("host", "connect host")
        .RequiredArgument("STR")
        .StoreResult(&Host);

    opts.AddLongOption("port", "connect port")
        .RequiredArgument("NUM")
        .StoreResult(&InsecurePort);

    opts.AddLongOption("secure-port", "connect secure port (overrides --port)")
        .RequiredArgument("NUM")
        .StoreResult(&SecurePort);

    opts.AddLongOption("endpoint-storage-dir")
        .RequiredArgument("STR")
        .StoreResult(&EndpointStorageDir);

    opts.AddLongOption("mon-file")
        .RequiredArgument("STR")
        .StoreResult(&MonitoringConfig);

    opts.AddLongOption("mon-address")
        .RequiredArgument("STR")
        .StoreResult(&MonitoringAddress);

    opts.AddLongOption("mon-port")
        .RequiredArgument("NUM")
        .StoreResult(&MonitoringPort);

    opts.AddLongOption("mon-threads")
        .RequiredArgument("NUM")
        .StoreResult(&MonitoringThreads);

    opts.AddLongOption("spdk-config")
        .RequiredArgument("STR")
        .StoreResult(&SpdkConfig);

    opts.AddLongOption("config")
        .RequiredArgument("STR")
        .StoreResult(&TestConfig);

    opts.AddLongOption("results")
        .RequiredArgument("STR")
        .StoreResult(&OutputFile);

    const auto& verbose = opts.AddLongOption("verbose", "output level for diagnostics messages")
        .OptionalArgument("STR")
        .StoreResult(&VerboseLevel);

    opts.AddLongOption("timeout", "timeout in seconds")
        .OptionalArgument("NUM")
        .Handler1T<ui32>([this] (const auto& timeout) {
            Timeout = TDuration::Seconds(timeout);
        });

    opts.AddLongOption("grpc-trace", "turn on grpc tracing")
        .NoArgument()
        .StoreTrue(&EnableGrpcTracing);

    TOptsParseResultException res(&opts, argc, argv);

    if (res.Has(&verbose) && !VerboseLevel) {
        VerboseLevel = "debug";
    }
}

IOutputStream& TOptions::GetOutputStream()
{
    if (OutputFile) {
        if(!OutputStream) {
            OutputStream.reset(new TOFStream(OutputFile));
        }
        return *OutputStream;
    }
    return Cout;
}

}   // namespace NCloud::NBlockStore::NLoadTest
