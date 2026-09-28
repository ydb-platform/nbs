#include "helpers.h"

#include <cloud/blockstore/libs/discovery/config.h>
#include <cloud/blockstore/libs/server/config.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void RemoveStaticOnlyBlockstoreFields(NProto::TBlockstoreConfig& config)
{
    if (config.HasServer() && config.GetServer().HasServerConfig()) {
        auto* serverConfig = config.MutableServer()->MutableServerConfig();
        serverConfig->ClearDynamicYamlConfigurationEnabled();
        if (serverConfig->ByteSizeLong() == 0) {
            config.MutableServer()->ClearServerConfig();
        }
    }

    if (config.HasServer() && config.GetServer().ByteSizeLong() == 0) {
        config.ClearServer();
    }

    if (config.HasStorageService()) {
        auto* storageConfig = config.MutableStorageService();
        storageConfig->ClearConfigDispatcherSettings();
        storageConfig->ClearSchemeShardDir();
        storageConfig->ClearNodeType();
        if (storageConfig->ByteSizeLong() == 0) {
            config.ClearStorageService();
        }
    }

    if (config.HasDiskAgent()) {
        auto* diskAgentConfig = config.MutableDiskAgent();
        diskAgentConfig->ClearDedicatedDiskAgent();
        if (diskAgentConfig->ByteSizeLong() == 0) {
            config.ClearDiskAgent();
        }
    }
}

// Normalize overrides while preserving the static agent role and linked
// discovery ports; remove empty sections so ignored overrides do not publish.
void NormalizeDynamicBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    NProto::TBlockstoreConfig& dynamicConfig)
{
    RemoveStaticOnlyBlockstoreFields(dynamicConfig);

    // DiskAgent: preserve the static Enabled value when DedicatedDiskAgent is
    // set.
    if (dynamicConfig.HasDiskAgent() &&
        staticConfig.GetDiskAgent().GetDedicatedDiskAgent())
    {
        auto* diskAgentConfig = dynamicConfig.MutableDiskAgent();
        diskAgentConfig->ClearEnabled();
        if (diskAgentConfig->ByteSizeLong() == 0) {
            dynamicConfig.ClearDiskAgent();
        }
    }

    // Discovery: preserve a link expressed by equal static ports, unless a
    // dynamic discovery port is explicitly supplied.
    const auto& dynamicServer = dynamicConfig.GetServer().GetServerConfig();
    if (dynamicServer.HasPort() || dynamicServer.HasSecurePort()) {
        const NServer::TServerAppConfig staticServer(staticConfig.GetServer());
        const NDiscovery::TDiscoveryConfig staticDiscovery(
            staticConfig.GetDiscoveryService());

        if (dynamicServer.HasPort() &&
            !dynamicConfig.GetDiscoveryService().HasConductorInstancePort() &&
            staticDiscovery.GetConductorInstancePort() == staticServer.GetPort())
        {
            dynamicConfig.MutableDiscoveryService()->SetConductorInstancePort(
                dynamicServer.GetPort());
        }

        if (dynamicServer.HasSecurePort() &&
            !dynamicConfig.GetDiscoveryService()
                 .HasConductorSecureInstancePort() &&
            staticDiscovery.GetConductorSecureInstancePort() ==
                staticServer.GetSecurePort())
        {
            dynamicConfig.MutableDiscoveryService()
                ->SetConductorSecureInstancePort(dynamicServer.GetSecurePort());
        }
    }
}

TResultOrError<NProto::TBlockstoreConfig> ExtractBlockstoreConfig(
    const google::protobuf::Message& privateDatabaseConfig)
{
    // Preserve parser diagnostics while classifying every TError as a failure.
    const auto* descriptor = privateDatabaseConfig.GetDescriptor();
    if (descriptor == NCloud::NProto::TError::descriptor()) {
        NCloud::NProto::TError error;
        error.CopyFrom(privateDatabaseConfig);
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Failed to parse PrivateDatabaseConfig from CMS: "
                << FormatError(error));
    }

    // Reject unexpected types without including their configuration values.
    if (descriptor != NProto::TBlockstoreConfig::descriptor()) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "Internal error: received an unexpected "
                   "PrivateDatabaseConfig payload type "
                << descriptor->full_name() << "; expected "
                << NProto::TBlockstoreConfig::descriptor()->full_name());
    }

    // Copy the payload so callers can normalize it without changing the source.
    NProto::TBlockstoreConfig config;
    config.CopyFrom(privateDatabaseConfig);
    return config;
}

}   // namespace NCloud::NBlockStore
