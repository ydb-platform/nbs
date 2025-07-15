#include "node.h"
#include "node_registration_helpers.h"

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <contrib/ydb/core/base/event_filter.h>
#include <contrib/ydb/core/base/location.h>
#include <contrib/ydb/core/config/init/init.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/protos/node_broker.pb.h>
#include <contrib/ydb/public/lib/deprecated/kicli/kicli.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/event.h>
#include <contrib/ydb/library/yaml_config/yaml_config.h>

#include <util/generic/vector.h>
#include <util/network/address.h>
#include <util/network/socket.h>
#include <util/random/shuffle.h>
#include <util/string/join.h>
#include <util/stream/file.h>
#include <util/system/hostname.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NYdb;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& name)
{
    return TFileInput(name).ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

TString ExtractYdbError(const TStatus& status)
{
    TStringStream out;
    status.GetIssues().PrintTo(out, true);
    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

TString GetNetworkAddress(const TString& host)
{
    TNetworkAddress addr(host, 0);

    // prefer ipv6
    for (auto it = addr.Begin(), end = addr.End(); it != end; ++it) {
        if (it->ai_family == AF_INET6) {
            return NAddr::PrintHost(NAddr::TAddrInfo(&*it));
        }
    }

    // fallback to ipv4
    for (auto it = addr.Begin(), end = addr.End(); it != end; ++it) {
        if (it->ai_family == AF_INET) {
            return NAddr::PrintHost(NAddr::TAddrInfo(&*it));
        }
    }

    ythrow TServiceError(E_FAIL)
        << "could not resolve address for host: " << host;
}

////////////////////////////////////////////////////////////////////////////////

NGRpcProxy::TGRpcClientConfig CreateKikimrConfig(
    const TRegisterDynamicNodeOptions& options,
    const TString& nodeBrokerAddress)
{
    NGRpcProxy::TGRpcClientConfig config(
        nodeBrokerAddress,
        options.Settings.RegistrationTimeout);

    if (options.UseNodeBrokerSsl) {
        config.EnableSsl = true;
        auto& sslCredentials = config.SslCredentials;
        if (options.Settings.PathToGrpcCaFile) {
            sslCredentials.pem_root_certs =
                ReadFile(options.Settings.PathToGrpcCaFile);
        }
        if (options.Settings.PathToGrpcCertFile &&
            options.Settings.PathToGrpcPrivateKeyFile)
        {
            sslCredentials.pem_cert_chain =
                ReadFile(options.Settings.PathToGrpcCertFile);
            sslCredentials.pem_private_key =
                ReadFile(options.Settings.PathToGrpcPrivateKeyFile);
        }
    }

    return config;
}

////////////////////////////////////////////////////////////////////////////////

// TODO: this function is a copy-paste of not synced function from ydb repo
NKikimrConfig::TAppConfig GetYamlConfigFromResult(
    const NKikimr::NClient::TConfigurationResult& result,
    const TMap<TString, TString>& labels)
{
    NKikimrConfig::TAppConfig appConfig;
    if (result.HasYamlConfig() && !result.GetYamlConfig().empty()) {
        NYamlConfig::ResolveAndParseYamlConfig(
            result.GetYamlConfig(),
            result.GetVolatileYamlConfigs(),
            labels,
            appConfig,
            nullptr,
            nullptr);
    } else {
        // TODO: for testing purposes. Rethink later
        ythrow TServiceError(E_FAIL)
            << "no yaml " << result.GetVolatileYamlConfigs().size();
    }
    return appConfig;
}

TResultOrError<NKikimrConfig::TAppConfig> GetConfigsFromCms(
    ui32 nodeId,
    const TString& hostName,
    const TString& nodeBrokerAddress,
    const TRegisterDynamicNodeOptions& options,
    NClient::TKikimr& kikimr,
    NKikimrConfig::TStaticNameserviceConfig& nsConfig)
{
    auto configurator = kikimr.GetNodeConfigurator();

    // TODO: why version=1, why empty token
    auto configResult = configurator.SyncGetNodeConfig(
        nodeId,
        hostName,
        options.SchemeShardDir,
        options.Settings.NodeType,
        options.Domain,
        "",
        true,
        1);

    if (!configResult.IsSuccess()) {
        return MakeError(
            E_FAIL,
            "Unable to get config from " + nodeBrokerAddress.Quote() + ": " +
                configResult.GetErrorMessage());
    }

    // TODO: labels should be consistent with
    // cloud/storage/core/libs/kikimr/config_dispatcher_helpers.cpp
    // (SetupConfigDispatcher)
    const TMap<TString, TString> labels{{"nbs", "true"}};
    NKikimrConfig::TAppConfig yamlConfig =
        GetYamlConfigFromResult(configResult, labels);
    NYamlConfig::ReplaceUnmanagedKinds(configResult.GetConfig(), yamlConfig);

    auto cmsConfig = configResult.GetConfig();

    if (cmsConfig.HasNameserviceConfig()) {
        cmsConfig.MutableNameserviceConfig()->SetSuppressVersionCheck(
            nsConfig.GetSuppressVersionCheck());
    }

    // TODO: merge with other config via GetActualDynConfig
    cmsConfig.MergeFrom(yamlConfig);

    return yamlConfig;
}

////////////////////////////////////////////////////////////////////////////////

TDriverConfig CreateDriverConfig(
    const TRegisterDynamicNodeOptions& options,
    const TString& addr)
{
    TDriverConfig config;

    if (options.Settings.PathToGrpcCaFile) {
        config.UseSecureConnection(
            ReadFile(options.Settings.PathToGrpcCaFile).c_str());
    }
    if (options.Settings.PathToGrpcCertFile &&
        options.Settings.PathToGrpcPrivateKeyFile)
    {
        auto certificate = ReadFile(options.Settings.PathToGrpcCertFile);
        auto privateKey = ReadFile(options.Settings.PathToGrpcPrivateKeyFile);
        config.UseClientCertificate(certificate.c_str(), privateKey.c_str());
    }

    config.SetAuthToken(options.Settings.NodeRegistrationToken);
    config.SetEndpoint(addr);

    return config;
}

////////////////////////////////////////////////////////////////////////////////

NDiscovery::TNodeRegistrationResult TryToRegisterDynamicNodeViaDiscoveryService(
    const TRegisterDynamicNodeOptions& options,
    const TString& addr,
    const NDiscovery::TNodeRegistrationSettings& settings)
{
    auto connection = TDriver(CreateDriverConfig(options, addr));

    auto client = NDiscovery::TDiscoveryClient(connection);
    NDiscovery::TNodeRegistrationResult result =
        client.NodeRegistration(settings).GetValueSync();
    connection.Stop(true);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

struct TLegacyNodeRegistrant
    : public INodeRegistrant
{
    const TString HostName;
    const TString HostAddress;
    const TRegisterDynamicNodeOptions& Options;
    NKikimrConfig::TStaticNameserviceConfig& NsConfig;
    NKikimrConfig::TDynamicNodeConfig& DnConfig;

    TNodeLocation Location;

    TLegacyNodeRegistrant(
        TString hostName,
        TString hostAddress,
        const TRegisterDynamicNodeOptions& options,
        NKikimrConfig::TStaticNameserviceConfig& nsConfig,
        NKikimrConfig::TDynamicNodeConfig& dnConfig)
        : HostName(std::move(hostName))
        , HostAddress(std::move(hostAddress))
        , Options(options)
        , NsConfig(nsConfig)
        , DnConfig(dnConfig)
    {
        NActorsInterconnect::TNodeLocation proto;
        proto.SetDataCenter(options.DataCenter);
        proto.SetRack(options.Rack);
        proto.SetUnit(ToString(options.Body));
        Location = TNodeLocation(proto);
    }

    TResultOrError<TRegistrationResult> RegisterNode(
        const TString& nodeBrokerAddress) override
    {
        NClient::TKikimr kikimr(CreateKikimrConfig(Options, nodeBrokerAddress));

        TMaybe<TString> path;
        if (Options.SchemeShardDir) {
            path = Options.SchemeShardDir;
        }

        auto registrant = kikimr.GetNodeRegistrant();
        auto result = registrant.SyncRegisterNode(
            Options.Domain,
            HostName,
            Options.InterconnectPort,
            HostAddress,
            HostName,
            Location,
            false, // fixedNodeId
            path);

        if (!result.IsSuccess()) {
            return MakeError(
                E_FAIL,
                result.GetErrorMessage());
        }

        NsConfig.ClearNode();
        for (const auto& node: result.Record().GetNodes()) {
            if (node.GetNodeId() == result.GetNodeId()) {
                // update node information based on registration response
                *DnConfig.MutableNodeInfo() = node;
            } else {
                *NsConfig.AddNode() = CreateStaticNodeInfo(node);
            }
        }

        return TRegistrationResult{result.GetNodeId(), result.GetScopeId()};
    }

    TResultOrError<NKikimrConfig::TAppConfig> GetConfigs(
        const TString& nodeBrokerAddress,
        ui32 nodeId) override
    {
        NClient::TKikimr kikimr(CreateKikimrConfig(Options, nodeBrokerAddress));

        return GetConfigsFromCms(
            nodeId,
            HostName,
            nodeBrokerAddress,
            Options,
            kikimr,
            NsConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryNodeRegistrant
    : public INodeRegistrant
{
    const TString HostName;
    const TString HostAddress;
    const TRegisterDynamicNodeOptions& Options;
    NKikimrConfig::TStaticNameserviceConfig& NsConfig;
    NKikimrConfig::TDynamicNodeConfig& DnConfig;
    const TNodeLocation Location;

    NDiscovery::TNodeRegistrationSettings Settings;

    TDiscoveryNodeRegistrant(
        TString hostName,
        TString hostAddress,
        const TRegisterDynamicNodeOptions& options,
        NKikimrConfig::TStaticNameserviceConfig& nsConfig,
        NKikimrConfig::TDynamicNodeConfig& dnConfig)
        : HostName(std::move(hostName))
        , HostAddress(std::move(hostAddress))
        , Options(options)
        , NsConfig(nsConfig)
        , DnConfig(dnConfig)
    {
        NDiscovery::TNodeLocation location;
        location.DataCenter = Options.DataCenter;
        location.Rack = Options.Rack;
        location.Unit = ToString(Options.Body);

        Settings.Location(location);
        Settings.Address(HostAddress);
        Settings.ResolveHost(HostName);
        Settings.Host(HostName);
        Settings.DomainPath(Options.Domain);
        Settings.Port(Options.InterconnectPort);
        Settings.Path(Options.SchemeShardDir);
    }

    TResultOrError<TRegistrationResult> RegisterNode(
        const TString& nodeBrokerAddress) override
    {
        auto result = TryToRegisterDynamicNodeViaDiscoveryService(
            Options,
            nodeBrokerAddress,
            Settings);

        if (!result.IsSuccess()) {
            return MakeError(
                E_FAIL,
                ExtractYdbError(result));
        }

        NsConfig.ClearNode();
        for (const auto& node: result.GetNodes()) {
            if (node.NodeId == result.GetNodeId()) {
                // update node information based on registration response
                *DnConfig.MutableNodeInfo() = CreateNodeInfo(
                    node,
                    result.HasNodeName()
                        ? result.GetNodeName()
                                         : std::optional<TString>{});
            } else {
                *NsConfig.AddNode() = CreateStaticNodeInfo(node);
            }
        }

        return TRegistrationResult{
            result.GetNodeId(),
            NActors::TScopeId{
                result.GetScopeTabletId(),
                result.GetScopePathId()}};
    }

    TResultOrError<NKikimrConfig::TAppConfig> GetConfigs(
        const TString& nodeBrokerAddress,
        ui32 nodeId) override
    {
        NClient::TKikimr kikimr(CreateKikimrConfig(Options, nodeBrokerAddress));

        return GetConfigsFromCms(
            nodeId,
            HostName,
            nodeBrokerAddress,
            Options,
            kikimr,
            NsConfig);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

INodeRegistrantPtr CreateNodeRegistrant(
    NKikimrConfig::TAppConfigPtr appConfig,
    const TRegisterDynamicNodeOptions& options,
    TLog& Log)
{
    auto& nsConfig = *appConfig->MutableNameserviceConfig();
    auto& dnConfig = *appConfig->MutableDynamicNodeConfig();

    const auto& hostName = FQDNHostName();
    const auto& hostAddress = GetNetworkAddress(hostName);

    if (options.UseNodeBrokerSsl) {
        STORAGE_WARN("Trying to register with discovery service: ");
        return std::make_unique<TDiscoveryNodeRegistrant>(
            hostName,
            hostAddress,
            options,
            nsConfig,
            dnConfig);
    }
    const TNodeLocation location(
        options.DataCenter,
        {},
        options.Rack,
        ToString(options.Body));
    STORAGE_WARN(
        "Trying to register with legacy service: " << location.ToString());
    return std::make_unique<TLegacyNodeRegistrant>(
        hostName,
        hostAddress,
        options,
        nsConfig,
        dnConfig);
}

////////////////////////////////////////////////////////////////////////////////

/**
 * Registers a dynamic node with retry policies:
 *
 * 1. **Registration Phase**:
 *    - Retries up to `Settings.MaxAttempts` times.
 *    - Fixed delay (`Settings.ErrorTimeout`) between attempts.
 *
 * 2. **Configuration Phase** (if `LoadCmsConfigs` is true):
 *    - Retries indefinitely with exponential backoff.
 *    - Backoff range: [`LoadConfigsFromCmsRetryMinDelay`,
 * `LoadConfigsFromCmsRetryMaxDelay`].
 *    - Total timeout: `LoadConfigsFromCmsTotalTimeout`.
 *
 * Throws `TServiceError` on unrecoverable failures.
 */
TRegisterDynamicNodeResult RegisterDynamicNode(
    NKikimrConfig::TAppConfigPtr appConfig,
    const TRegisterDynamicNodeOptions& options,
    INodeRegistrantPtr registrant,
    TLog& Log,
    ITimerPtr timer)
{
    auto& nsConfig = *appConfig->MutableNameserviceConfig();

    const auto& hostName = FQDNHostName();
    const auto& hostAddress = GetNetworkAddress(hostName);

    TVector<TString> addrs;
    if (options.NodeBrokerAddress) {
        addrs.push_back(options.NodeBrokerAddress);
    } else {
        auto port = options.UseNodeBrokerSsl && options.NodeBrokerSecurePort
                        ? options.NodeBrokerSecurePort
                        : options.NodeBrokerPort;
        if (port) {
            for (const auto& node: nsConfig.GetNode()) {
                addrs.emplace_back(Join(":", node.GetHost(), port));
            }
            Shuffle(addrs.begin(), addrs.end());
        }
    }

    if (!addrs) {
        ythrow TServiceError(E_FAIL)
            << "cannot register dynamic node: "
            << "neither NodeBrokerAddress nor NodeBrokerPort specified";
    }

    ui32 nodeId = 0;
    TScopeId scopeId;
    TString registeredNodeBrokerAddress;

    ui32 attempts = 0;
    for (;;) {
        const auto& nodeBrokerAddress = addrs[attempts++ % addrs.size()];

        auto [result, error] = registrant->RegisterNode(nodeBrokerAddress);

        if (HasError(error)) {
            const auto& msg = error.GetMessage();
            if (attempts == options.Settings.MaxAttempts) {
                ythrow TServiceError(E_FAIL)
                    << "Cannot register dynamic node: " << msg;
            }

            STORAGE_WARN(
                "Failed to register dynamic node at "
                << nodeBrokerAddress.Quote() << ": " << msg
                << ". Will retry after " << options.Settings.ErrorTimeout);
            timer->Sleep(options.Settings.ErrorTimeout);
            continue;
        }

        STORAGE_INFO(
            "Registered dynamic node at "
            << nodeBrokerAddress.Quote() << " with address "
            << hostAddress.Quote() << " with node id "
            << std::get<0>(result));

        registeredNodeBrokerAddress = nodeBrokerAddress;
        std::tie(nodeId, scopeId) = result;
        break;
    }

    TMaybe<NKikimrConfig::TAppConfig> configurationResult;

    if (!options.LoadCmsConfigs) {
        return {nodeId, scopeId, {}};
    }

    TBackoffDelayProvider retryDelayProvider(
        options.Settings.LoadConfigsFromCmsRetryMinDelay,
        options.Settings.LoadConfigsFromCmsRetryMaxDelay);
    const auto deadline =
        timer->Now() + options.Settings.LoadConfigsFromCmsTotalTimeout;
    for (;;) {
        auto [config, error] =
            registrant->GetConfigs(registeredNodeBrokerAddress, nodeId);

        if (HasError(error)) {
            const auto& msg = error.GetMessage();
            if (deadline < timer->Now()) {
                ythrow TServiceError(E_FAIL)
                    << "Cannot configure dynamic node: " << msg;
            }

            auto delay = retryDelayProvider.GetDelayAndIncrease();
            STORAGE_WARN(
                "Failed to configure dynamic node at "
                << registeredNodeBrokerAddress.Quote() << ": " << msg
                << ". Will retry after " << delay);
            timer->Sleep(delay);
            continue;
        }

        STORAGE_INFO("Got configs from CMS");
        configurationResult = std::move(config);
        break;
    }

    return {nodeId, scopeId, std::move(configurationResult)};
}

}   // namespace NCloud::NStorage
