#include "options.h"

namespace NCloud::NBlockStore::NServer {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TOptions::TOptions()
{
    Opts.AddLongOption("storage-file")
        .RequiredArgument("FILE")
        .StoreResult(&StorageConfig);

    Opts.AddLongOption("disk-agent-file")
        .RequiredArgument("FILE")
        .StoreResult(&DiskAgentConfig);

    Opts.AddLongOption("dr-proxy-file")
        .RequiredArgument("FILE")
        .StoreResult(&DiskRegistryProxyConfig);

    Opts.AddLongOption("features-file")
        .RequiredArgument("FILE")
        .StoreResult(&FeaturesConfig);

    Opts.AddLongOption("rdma-file")
        .RequiredArgument("FILE")
        .DefaultValue("")
        .StoreResult(&RdmaConfig);

    Opts.AddLongOption("syslog-service")
        .RequiredArgument("STR")
        .StoreResult(&SysLogService);

    Opts.AddLongOption("node-type")
        .RequiredArgument("STR")
        .DefaultValue("disk-agent")
        .StoreResult(&NodeType);

    Opts.AddLongOption(
            "temporary-agent",
            "run temporary disk agent for blue-green deployment")
        .NoArgument()
        .StoreTrue(&TemporaryAgent);

    Opts.AddLongOption("rdma-target-port")
        .RequiredArgument("NUM")
        .DefaultValue(0)
        .StoreResult(&RdmaTargetPort);
}

void TOptions::Parse(int argc, char** argv)
{
    auto res = std::make_unique<TOptsParseResultException>(&Opts, argc, argv);
    if (res->FindLongOptParseResult("verbose") != nullptr && !VerboseLevel) {
        VerboseLevel = "debug";
    }

    Y_ENSURE(
        res->FindLongOptParseResult("ic-port"),
        "'--ic-port' option is required for kikimr service");

    Y_ENSURE(
        res->FindLongOptParseResult("domain"),
        "'--domain' option is required for kikimr service");

    Y_ENSURE(
        SysConfig && DomainsConfig || LoadCmsConfigs,
        "sys-file and domains-file options are required if "
        "load-configs-from-cms is not set");
}

}   // namespace NCloud::NBlockStore::NServer
