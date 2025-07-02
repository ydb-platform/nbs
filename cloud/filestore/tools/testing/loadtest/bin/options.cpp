#include "options.h"

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>

namespace NCloud::NFileStore::NLoadTest {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption('h');

    opts.AddLongOption("config", "config file name")
        .RequiredArgument("STR")
        .StoreResult(&ConfigFile);

    opts.AddLongOption("host", "connect host")
        .RequiredArgument("STR")
        .StoreResult(&Host);

    opts.AddLongOption("port", "connect port")
        .RequiredArgument("NUM")
        .StoreResult(&InsecurePort);

    opts.AddLongOption("secure-port", "connect secure port (overrides --port)")
        .RequiredArgument("NUM")
        .StoreResult(&SecurePort);

    opts.AddLongOption("unix-socket-path")
        .RequiredArgument("STR")
        .StoreResult(&UnixSocketPath);

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

    opts.AddLongOption("tests-config")
        .RequiredArgument("STR")
        .StoreResult(&TestsConfig);

    opts.AddLongOption("results")
        .RequiredArgument("STR")
        .StoreResult(&OutputFile);

    const auto& verbose = opts.AddLongOption("verbose", "output level for diagnostics messages")
        .OptionalArgument("STR")
        .StoreResult(&VerboseLevel);

    opts.AddLongOption("timeout", "timeout in seconds")
        .OptionalArgument("NUM")
        .StoreResult(&Timeout);

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
    if (OutputFile && OutputFile != "-") {
        if(!OutputStream) {
            OutputStream.reset(new TOFStream(OutputFile));
        }

        return *OutputStream;
    }

    return Cout;
}

}   // namespace NCloud::NFileStore::NLoadTest
