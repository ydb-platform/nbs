#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/serialized_enum.h>

namespace NCloud::NBlockStore::NServer {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TOptionsCommon::TOptionsCommon()
{
    Opts.AddLongOption("service", "service to run")
        .RequiredArgument("{" + GetEnumAllNames<EServiceKind>() + "}")
        .DefaultValue(ToString(EServiceKind::Ydb))
        .Handler1T<TString>([this] (const auto& s) {
            ServiceKind = FromString<EServiceKind>(s);
        });

    Opts.AddLongOption("data-server-port")
        .RequiredArgument("NUM")
        .StoreResult(&DataServerPort);

    Opts.AddLongOption("unix-socket-path")
        .RequiredArgument("FILE")
        .StoreResult(&UnixSocketPath);

    Opts.AddLongOption("client-file")
        .RequiredArgument("FILE")
        .StoreResult(&EndpointConfig);

    Opts.AddLongOption("disk-agent-file")
        .RequiredArgument("FILE")
        .StoreResult(&DiskAgentConfig);

    // TODO: remove it later (NBS-4171)
    Opts.AddLongOption("local-storage-file")
        .RequiredArgument("FILE");

    Opts.AddLongOption("dr-proxy-file")
        .RequiredArgument("FILE")
        .StoreResult(&DiskRegistryProxyConfig);

    Opts.AddLongOption("discovery-file")
        .RequiredArgument("FILE")
        .StoreResult(&DiscoveryConfig);

    Opts.AddLongOption("rdma-file")
        .RequiredArgument("FILE")
        .DefaultValue("")
        .StoreResult(&RdmaConfig);

    Opts.AddLongOption("trace-file")
        .RequiredArgument("PATH")
        .StoreResult(&TraceServiceConfig);

    Opts.AddLongOption("temporary-server", "run temporary server for blue-green deployment")
        .NoArgument()
        .StoreTrue(&TemporaryServer);

    Opts.AddLongOption(
            "skip-device-locality-validation",
            "skip device locality validation (for the testing purpose only)")
        .NoArgument()
        .SetFlag(&SkipDeviceLocalityValidation);
}

}   // namespace NCloud::NBlockStore::NServer
