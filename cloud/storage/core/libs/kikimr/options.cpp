#include "options.h"

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

TOptionsYdbBase::TOptionsYdbBase()
{
    Opts.AddLongOption("auth-file")
        .RequiredArgument("FILE")
        .StoreResult(&AuthConfig);

    Opts.AddLongOption("kikimr-features-file")
        .RequiredArgument("FILE")
        .StoreResult(&KikimrFeaturesConfig);

    Opts.AddLongOption("restarts-count-file")
        .RequiredArgument("PATH")
        .StoreResult(&RestartsCountFile);

    Opts.AddLongOption("suppress-version-check", "Suppress version compatibility checking via IC")
        .NoArgument()
        .StoreTrue(&SuppressVersionCheck);

    Opts.AddLongOption("log-file")
        .RequiredArgument("FILE")
        .StoreResult(&LogConfig);

    Opts.AddLongOption("shared-cache-file")
        .OptionalArgument("FILE")
        .StoreResult(&SharedCacheConfig);

    Opts.AddLongOption("sys-file")
        .RequiredArgument("FILE")
        .StoreResult(&SysConfig);

    Opts.AddLongOption("domains-file")
        .RequiredArgument("FILE")
        .StoreResult(&DomainsConfig);

    Opts.AddLongOption("ic-file")
        .RequiredArgument("PATH")
        .StoreResult(&InterconnectConfig);

    Opts.AddLongOption("ic-port")
        .RequiredArgument("NUM")
        .StoreResult(&InterconnectPort);

    Opts.AddLongOption("mon-file")
        .RequiredArgument("PATH")
        .StoreResult(&MonitoringConfig);

    Opts.AddLongOption("domain")
        .RequiredArgument("STR")
        .StoreResult(&Domain);

    Opts.AddLongOption("scheme-shard-dir")
        .RequiredArgument("STR")
        .StoreResult(&SchemeShardDir);

    Opts.AddLongOption("node-broker")
        .RequiredArgument("STR:NUM")
        .StoreResult(&NodeBrokerAddress);

    Opts.AddLongOption("node-broker-port")
        .RequiredArgument("PORT")
        .StoreResult(&NodeBrokerPort);

    Opts.AddLongOption("node-broker-secure-port")
        .RequiredArgument("PORT")
        .StoreResult(&NodeBrokerSecurePort);

    Opts.AddLongOption(
        "use-secure-registration",
        "Use secure connection to node broker")
        .NoArgument()
        .StoreTrue(&UseNodeBrokerSsl);

    Opts.AddLongOption("naming-file")
        .RequiredArgument("FILE")
        .StoreResult(&NameServiceConfig);

    Opts.AddLongOption("dynamic-naming-file")
        .RequiredArgument("FILE")
        .StoreResult(&DynamicNameServiceConfig);

    Opts.AddLongOption("location-file")
        .RequiredArgument("PATH")
        .StoreResult(&LocationFile);

    Opts.AddLongOption("load-configs-from-cms", "load configs from CMS")
        .NoArgument()
        .StoreTrue(&LoadCmsConfigs);
}

}   // namespace NCloud::NStorage
