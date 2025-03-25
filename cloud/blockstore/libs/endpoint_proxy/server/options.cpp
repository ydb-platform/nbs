#include "options.h"

namespace NCloud::NBlockStore::NServer {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TOptions::TOptions()
{
    Opts.AddLongOption("root-certs-file")
        .RequiredArgument("FILE")
        .StoreResult(&RootCertsFile);

    Opts.AddLongOption("key-file")
        .RequiredArgument("FILE")
        .StoreResult(&KeyFile);

    Opts.AddLongOption("cert-file")
        .RequiredArgument("FILE")
        .StoreResult(&CertFile);

    Opts.AddLongOption("unix-socket-path")
        .RequiredArgument("FILE")
        .StoreResult(&UnixSocketPath);

    Opts.AddLongOption("netlink")
        .NoArgument()
        .SetFlag(&Netlink);

    Opts.AddLongOption("stored-endpoints-path")
        .RequiredArgument("FILE")
        .StoreResult(&StoredEndpointsPath);

    Opts.AddLongOption("nbd-request-timeout")
        .RequiredArgument("NUM")
        .Handler1T<TString>([this] (const auto& s) {
            NbdRequestTimeout = TDuration::Parse(s);
        });

    Opts.AddLongOption("nbd-reconnect-delay")
        .RequiredArgument("NUM")
        .Handler1T<TString>([this] (const auto& s) {
            NbdReconnectDelay = TDuration::Parse(s);
        });

    // deprecated
    Opts.AddLongOption("without-libnl")
        .NoArgument()
        .SetFlag(&WithoutLibnl);

    Opts.AddLongOption(
            "debug-restart-events-count",
            "issue multiple restart events for each io error. this option is "
            "used to debug nbd server reconnect")
        .RequiredArgument("NUM")
        .StoreResult(&DebugRestartEventsCount);
}

void TOptions::Parse(int argc, char** argv)
{
    auto res = std::make_unique<TOptsParseResultException>(&Opts, argc, argv);
    if (res->FindLongOptParseResult("verbose") != nullptr && !VerboseLevel) {
        VerboseLevel = "debug";
    }
}

}   // namespace NCloud::NBlockStore::NServer
