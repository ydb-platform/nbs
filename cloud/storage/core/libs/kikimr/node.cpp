#include "public.h"
#include "node.h"

#include <cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/event_filter.h>
#include <ydb/core/base/location.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event.h>

#include <util/generic/vector.h>
#include <util/network/address.h>
#include <util/network/socket.h>
#include <util/random/shuffle.h>
#include <util/string/join.h>
#include <util/system/hostname.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TRegisterDynamicNodeResult RegisterDynamicNode(
    NKikimrConfig::TAppConfigPtr appConfig,
    const TRegisterDynamicNodeOptions& options,
    TLog& Log)
{
    auto& nsConfig = *appConfig->MutableNameserviceConfig();
    auto& dnConfig = *appConfig->MutableDynamicNodeConfig();

    auto hostName = FQDNHostName();
    auto hostAddress = GetNetworkAddress(hostName);

    TVector<TString> addrs;
    if (options.NodeBrokerAddress) {
        addrs.push_back(options.NodeBrokerAddress);
    } else if (options.NodeBrokerPort) {
        for (const auto& node: nsConfig.GetNode()) {
            addrs.emplace_back(Join(":", node.GetHost(), options.NodeBrokerPort));
        }
        Shuffle(addrs.begin(), addrs.end());
    }

    if (!addrs) {
        ythrow TServiceError(E_FAIL)
            << "cannot register dynamic node: "
            << "neither NodeBrokerAddress nor NodeBrokerPort specified";
    }

    NActorsInterconnect::TNodeLocation proto;
    proto.SetDataCenter(options.DataCenter);
    proto.SetRack(options.Rack);
    proto.SetUnit(ToString(options.Body));
    const TNodeLocation location(proto);

    int attempts = 0;
    for (;;) {
        const auto& nodeBrokerAddress = addrs[attempts++ % addrs.size()];

        NGRpcProxy::TGRpcClientConfig config(nodeBrokerAddress, options.RegistrationTimeout);
        NClient::TKikimr kikimr(config);

        STORAGE_INFO("Trying to register dynamic node at " << nodeBrokerAddress.Quote());

        auto registrant = kikimr.GetNodeRegistrant();
        auto result = registrant.SyncRegisterNode(
            options.Domain,
            hostName,
            options.InterconnectPort,
            hostAddress,
            hostName,
            location,
            false,
            options.SchemeShardDir);

        if (!result.IsSuccess()) {
            if (attempts == options.MaxAttempts) {
                ythrow TServiceError(E_FAIL)
                    << "Cannot register dynamic node: " << result.GetErrorMessage();
            }

            STORAGE_WARN("Failed to register dynamic node at " << nodeBrokerAddress.Quote()
                << ": " << result.GetErrorMessage());
            Sleep(options.ErrorTimeout);
            continue;
        }

        STORAGE_INFO(
            "Registered dynamic node at " << nodeBrokerAddress.Quote() <<
            " with address " << hostAddress.Quote());

        nsConfig.ClearNode();
        for (const auto& node: result.Record().GetNodes()) {
            if (node.GetNodeId() == result.GetNodeId()) {
                dnConfig.MutableNodeInfo()->CopyFrom(node);
            } else {
                auto& info = *nsConfig.AddNode();
                info.SetNodeId(node.GetNodeId());
                info.SetAddress(node.GetAddress());
                info.SetPort(node.GetPort());
                info.SetHost(node.GetHost());
                info.SetInterconnectHost(node.GetResolveHost());
                info.MutableLocation()->CopyFrom(node.GetLocation());
            }
        }

        TMaybe<NKikimrConfig::TAppConfig> cmsConfig;

        if (options.LoadCmsConfigs) {
            auto configurator = kikimr.GetNodeConfigurator();
            auto configResult = configurator.SyncGetNodeConfig(
                result.GetNodeId(),
                hostName,
                options.SchemeShardDir,
                options.NodeType,
                options.Domain);

            if (!configResult.IsSuccess()) {
                ythrow TServiceError(E_FAIL)
                    << "Unable to get config from " << nodeBrokerAddress.Quote()
                    << ": " << configResult.GetErrorMessage();
            }

            cmsConfig = configResult.GetConfig();

            if (cmsConfig->HasNameserviceConfig()) {
                cmsConfig->MutableNameserviceConfig()
                    ->SetSuppressVersionCheck(nsConfig.GetSuppressVersionCheck());
            }
        }

        return { result.GetNodeId(), result.GetScopeId(), std::move(cmsConfig) };
    }
}

}   // namespace NCloud::NBlockStore::NStorage
