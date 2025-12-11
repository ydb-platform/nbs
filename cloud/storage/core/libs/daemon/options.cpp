#include "options.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TOptionsBase::TOptionsBase()
{
    Opts.AddHelpOption();

    Opts.AddLongOption("mon-address")
        .RequiredArgument()
        .StoreResult(&MonitoringAddress);

    Opts.AddLongOption("mon-port")
        .RequiredArgument("NUM")
        .StoreResult(&MonitoringPort);

    Opts.AddLongOption("mon-threads")
        .RequiredArgument("NUM")
        .StoreResult(&MonitoringThreads);

    Opts.AddLongOption("diag-file")
        .RequiredArgument("FILE")
        .StoreResult(&DiagnosticsConfig);

    Opts.AddLongOption("server-file")
        .RequiredArgument("FILE")
        .StoreResult(&ServerConfig);

    Opts.AddLongOption("server-port")
        .RequiredArgument("NUM")
        .StoreResult(&ServerPort);

    Opts.AddLongOption("secure-server-port")
        .RequiredArgument("NUM")
        .StoreResult(&SecureServerPort);

    Opts.AddLongOption("profile-file")
        .RequiredArgument("PATH")
        .StoreResult(&ProfileFile);

    Opts.AddLongOption("grpc-trace", "turn on grpc tracing")
        .NoArgument()
        .StoreTrue(&EnableGrpcTracing);

    Opts.AddLongOption("verbose", "output level for diagnostics messages")
        .OptionalArgument("STR")
        .StoreResult(&VerboseLevel);

    Opts.AddLongOption(
            "lock-memory",
            "lock process memory after initialization")
        .NoArgument()
        .DefaultValue(true)
        .StoreTrue(&MemLock);
}

}   // namespace NCloud
