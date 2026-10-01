/*******************************************************************************

Configuration construction owns immutable runtime wrappers for all merged
sections. Storage configuration copies retain the shared ICB controls and use
the features wrapper created from the merged protobuf configuration.

*******************************************************************************/

#include "blockstore_config_management.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

// The private IBlockstoreConfig implementation and owner of its runtime
// wrappers and protobuf sections. Only MakeBlockstoreConfig() and
// MakeStartupBlockstoreConfig() create instances.
class TBlockstoreConfigImpl final: public IBlockstoreConfig
{
public:
    TBlockstoreConfigImpl(
        const NProto::TBlockstoreConfig& config,
        NFeatures::TFeaturesConfigPtr featuresConfig,
        NStorage::TStorageConfigPtr storageConfig,
        NStorage::TDiskAgentConfigPtr diskAgentConfig);

    const NServer::TServerAppConfigPtr& GetServerConfig() const override;

    const NFeatures::TFeaturesConfigPtr&
    GetFeaturesConfig() const override;

    const NStorage::TStorageConfigPtr& GetStorageConfig() const override;

    const TDiagnosticsConfigPtr& GetDiagnosticsConfig() const override;

    const NDiscovery::TDiscoveryConfigPtr&
    GetDiscoveryServiceConfig() const override;

    const NClient::TClientAppConfigPtr& GetEndpointConfig() const override;

    const NStorage::TDiskAgentConfigPtr&
    GetDiskAgentConfig() const override;

    const NStorage::TDiskRegistryProxyConfigPtr&
    GetDiskRegistryProxyConfig() const override;

    const NSpdk::TSpdkEnvConfigPtr& GetSpdkEnvConfig() const override;

    const NRdma::TRdmaConfigPtr& GetRdmaConfig() const override;

    const NYdbStats::TYdbStatsConfigPtr&
    GetYdbStatsConfig() const override;

    const NLogbroker::TLogbrokerConfigPtr&
    GetLogbrokerConfig() const override;

    const NNotify::TNotifyConfigPtr& GetNotifyConfig() const override;

    const NIamClient::TIamClientConfigPtr&
    GetIamClientConfig() const override;

    const TGrpcClientConfigPtr& GetKmsClientConfig() const override;

    const TGrpcClientConfigPtr& GetComputeClientConfig() const override;

    const TRootKmsConfigPtr& GetRootKmsConfig() const override;

    const NCells::TCellsConfigPtr& GetCellsConfig() const override;

    const TLocalNVMeConfigPtr& GetLocalNVMeConfig() const override;

private:
    const NServer::TServerAppConfigPtr ServerConfig;
    const NFeatures::TFeaturesConfigPtr FeaturesConfig;
    const NStorage::TStorageConfigPtr StorageConfig;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const NDiscovery::TDiscoveryConfigPtr DiscoveryServiceConfig;
    const NClient::TClientAppConfigPtr EndpointConfig;
    const NStorage::TDiskAgentConfigPtr DiskAgentConfig;
    const NStorage::TDiskRegistryProxyConfigPtr DiskRegistryProxyConfig;
    const NSpdk::TSpdkEnvConfigPtr SpdkEnvConfig;
    const NRdma::TRdmaConfigPtr RdmaConfig;
    const NYdbStats::TYdbStatsConfigPtr YdbStatsConfig;
    const NLogbroker::TLogbrokerConfigPtr LogbrokerConfig;
    const NNotify::TNotifyConfigPtr NotifyConfig;
    const NIamClient::TIamClientConfigPtr IamClientConfig;
    const TGrpcClientConfigPtr KmsClientConfig;
    const TGrpcClientConfigPtr ComputeClientConfig;
    const TRootKmsConfigPtr RootKmsConfig;
    const NCells::TCellsConfigPtr CellsConfig;
    const TLocalNVMeConfigPtr LocalNVMeConfig;
};

TBlockstoreConfigImpl::TBlockstoreConfigImpl(
    const NProto::TBlockstoreConfig& config,
    NFeatures::TFeaturesConfigPtr featuresConfig,
    NStorage::TStorageConfigPtr storageConfig,
    NStorage::TDiskAgentConfigPtr diskAgentConfig)
    : ServerConfig(
          std::make_shared<NServer::TServerAppConfig>(config.GetServer()))
    , FeaturesConfig(std::move(featuresConfig))
    , StorageConfig(std::move(storageConfig))
    , DiagnosticsConfig(
          std::make_shared<TDiagnosticsConfig>(config.GetDiagnostics()))
    , DiscoveryServiceConfig(
          std::make_shared<NDiscovery::TDiscoveryConfig>(
              config.GetDiscoveryService()))
    , EndpointConfig(
          std::make_shared<NClient::TClientAppConfig>(config.GetEndpoint()))
    , DiskAgentConfig(std::move(diskAgentConfig))
    , DiskRegistryProxyConfig(
          std::make_shared<NStorage::TDiskRegistryProxyConfig>(
              config.GetDiskRegistryProxy()))
    , SpdkEnvConfig(
          std::make_shared<NSpdk::TSpdkEnvConfig>(config.GetSpdkEnv()))
    , RdmaConfig(std::make_shared<NRdma::TRdmaConfig>(config.GetRdma()))
    , YdbStatsConfig(
          std::make_shared<NYdbStats::TYdbStatsConfig>(config.GetYdbStats()))
    , LogbrokerConfig(
          std::make_shared<NLogbroker::TLogbrokerConfig>(config.GetLogbroker()))
    , NotifyConfig(std::make_shared<NNotify::TNotifyConfig>(config.GetNotify()))
    , IamClientConfig(
          std::make_shared<NIamClient::TIamClientConfig>(config.GetIamClient()))
    , KmsClientConfig(
          std::make_shared<NProto::TGrpcClientConfig>(config.GetKmsClient()))
    , ComputeClientConfig(
          std::make_shared<NProto::TGrpcClientConfig>(
              config.GetComputeClient()))
    , RootKmsConfig(
          std::make_shared<NProto::TRootKmsConfig>(config.GetRootKms()))
    , CellsConfig(std::make_shared<NCells::TCellsConfig>(config.GetCells()))
    , LocalNVMeConfig(std::make_shared<TLocalNVMeConfig>(config.GetLocalNVMe()))
{
    Y_ABORT_UNLESS(FeaturesConfig);
    Y_ABORT_UNLESS(StorageConfig);
    Y_ABORT_UNLESS(DiskAgentConfig);
}

const NServer::TServerAppConfigPtr&
TBlockstoreConfigImpl::GetServerConfig() const
{
    return ServerConfig;
}

const NFeatures::TFeaturesConfigPtr&
TBlockstoreConfigImpl::GetFeaturesConfig() const
{
    return FeaturesConfig;
}

const NStorage::TStorageConfigPtr&
TBlockstoreConfigImpl::GetStorageConfig() const
{
    return StorageConfig;
}

const TDiagnosticsConfigPtr&
TBlockstoreConfigImpl::GetDiagnosticsConfig() const
{
    return DiagnosticsConfig;
}

const NDiscovery::TDiscoveryConfigPtr&
TBlockstoreConfigImpl::GetDiscoveryServiceConfig() const
{
    return DiscoveryServiceConfig;
}

const NClient::TClientAppConfigPtr&
TBlockstoreConfigImpl::GetEndpointConfig() const
{
    return EndpointConfig;
}

const NStorage::TDiskAgentConfigPtr&
TBlockstoreConfigImpl::GetDiskAgentConfig() const
{
    return DiskAgentConfig;
}

const NStorage::TDiskRegistryProxyConfigPtr&
TBlockstoreConfigImpl::GetDiskRegistryProxyConfig() const
{
    return DiskRegistryProxyConfig;
}

const NSpdk::TSpdkEnvConfigPtr&
TBlockstoreConfigImpl::GetSpdkEnvConfig() const
{
    return SpdkEnvConfig;
}

const NRdma::TRdmaConfigPtr& TBlockstoreConfigImpl::GetRdmaConfig() const
{
    return RdmaConfig;
}

const NYdbStats::TYdbStatsConfigPtr&
TBlockstoreConfigImpl::GetYdbStatsConfig() const
{
    return YdbStatsConfig;
}

const NLogbroker::TLogbrokerConfigPtr&
TBlockstoreConfigImpl::GetLogbrokerConfig() const
{
    return LogbrokerConfig;
}

const NNotify::TNotifyConfigPtr&
TBlockstoreConfigImpl::GetNotifyConfig() const
{
    return NotifyConfig;
}

const NIamClient::TIamClientConfigPtr&
TBlockstoreConfigImpl::GetIamClientConfig() const
{
    return IamClientConfig;
}

const TGrpcClientConfigPtr&
TBlockstoreConfigImpl::GetKmsClientConfig() const
{
    return KmsClientConfig;
}

const TGrpcClientConfigPtr&
TBlockstoreConfigImpl::GetComputeClientConfig() const
{
    return ComputeClientConfig;
}

const TRootKmsConfigPtr& TBlockstoreConfigImpl::GetRootKmsConfig() const
{
    return RootKmsConfig;
}

const NCells::TCellsConfigPtr&
TBlockstoreConfigImpl::GetCellsConfig() const
{
    return CellsConfig;
}

const TLocalNVMeConfigPtr&
TBlockstoreConfigImpl::GetLocalNVMeConfig() const
{
    return LocalNVMeConfig;
}

// Merge feature definitions by name so an overridden feature has one runtime
// record. Preserve the existing first-wins behavior within each source layer.
//
// staticFeatures - Feature definitions used as the merge base.
// dynamicFeatures - Overrides that replace complete static records with the
//      same names.
// result - Non-null output message. Replace its repeated Features field;
//      preserve all other fields. Discard its contents if the operation throws.
//
// Result - Keep each static feature's position and append dynamic-only
//      features in source order.
void MergeFeatures(
    const NCloud::NProto::TFeaturesConfig& staticFeatures,
    const NCloud::NProto::TFeaturesConfig& dynamicFeatures,
    NCloud::NProto::TFeaturesConfig* result)
{
    // TFeaturesConfig uses the first record when feature names are duplicated.
    // Preserve this behavior while canonicalizing the static layer.
    TVector<NCloud::NProto::TFeatureConfig> features;
    features.reserve(
        staticFeatures.FeaturesSize() + dynamicFeatures.FeaturesSize());

    THashMap<TString, size_t> featureIndices;
    for (const auto& feature: staticFeatures.GetFeatures()) {
        const bool inserted =
            featureIndices.emplace(feature.GetName(), features.size()).second;
        if (inserted) {
            features.push_back(feature);
        }
    }

    // Ignore duplicate names after the first one in the dynamic layer. The
    // first dynamic record replaces the static record or is appended if none
    // exists.
    THashSet<TString> dynamicFeatureNames;
    for (const auto& feature: dynamicFeatures.GetFeatures()) {
        if (!dynamicFeatureNames.insert(feature.GetName()).second) {
            continue;
        }

        const auto it = featureIndices.find(feature.GetName());
        if (it != featureIndices.end()) {
            // Replace the complete static record at its original position, so
            // fields omitted from the dynamic record do not leak from static.
            features[it->second].CopyFrom(feature);
        } else {
            // Append dynamic-only records in their source order.
            features.push_back(feature);
        }
    }

    // Rebuild only the repeated Features field after the enclosing Features
    // message has been merged, preserving its other and unknown fields.
    result->ClearFeatures();
    for (const auto& feature: features) {
        result->AddFeatures()->CopyFrom(feature);
    }
}

}   // namespace

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

bool IsPrivateDatabaseConfigEnabled(
    bool dynamicYamlConfigurationEnabled,
    bool temporaryServer)
{
    return dynamicYamlConfigurationEnabled && !temporaryServer;
}

NProto::TBlockstoreConfig MergeBlockstoreConfigSources(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig)
{
    auto result = staticConfig;
    result.MergeFrom(dynamicConfig);
    if (staticConfig.GetFeatures().FeaturesSize() ||
        dynamicConfig.GetFeatures().FeaturesSize())
    {
        MergeFeatures(
            staticConfig.GetFeatures(),
            dynamicConfig.GetFeatures(),
            result.MutableFeatures());
    }
    return result;
}

TBlockstoreConfigExtraParameters GetBlockstoreConfigExtraParameters(
    const IBlockstoreConfig& currentConfig)
{
    const auto& diskAgentConfig = currentConfig.GetDiskAgentConfig();
    return {
        .DiskAgent =
            {
                .Rack = diskAgentConfig->GetRack(),
                .NetworkMbitThroughput =
                    diskAgentConfig->GetNetworkMbitThroughput(),
            },
    };
}

IBlockstoreConfigMutablePtr MakeBlockstoreConfig(
    const NProto::TBlockstoreConfig& config,
    NStorage::TStorageConfigControlsPtr controls,
    TBlockstoreConfigExtraParameters extraParameters)
{
    Y_ABORT_UNLESS(controls);
    auto featuresConfig =
        std::make_shared<NFeatures::TFeaturesConfig>(config.GetFeatures());
    auto storageConfig = std::make_shared<NStorage::TStorageConfig>(
        config.GetStorageService(),
        featuresConfig,
        std::move(controls));
    auto diskAgentConfig = std::make_shared<NStorage::TDiskAgentConfig>(
        config.GetDiskAgent(),
        std::move(extraParameters.DiskAgent.Rack),
        extraParameters.DiskAgent.NetworkMbitThroughput);

    return MakeIntrusive<TBlockstoreConfigImpl>(
        config,
        std::move(featuresConfig),
        std::move(storageConfig),
        std::move(diskAgentConfig));
}

IBlockstoreConfigMutablePtr MakeBlockstoreConfig(
    const NProto::TBlockstoreConfig& config,
    const NStorage::TStorageConfig& storageConfig,
    const NStorage::TDiskAgentConfig& diskAgentConfig)
{
    auto featuresConfig =
        std::make_shared<NFeatures::TFeaturesConfig>(config.GetFeatures());
    auto storageConfigCopy =
        std::make_shared<NStorage::TStorageConfig>(storageConfig);
    storageConfigCopy->SetFeaturesConfig(featuresConfig);
    auto diskAgentConfigCopy =
        std::make_shared<NStorage::TDiskAgentConfig>(diskAgentConfig);

    return MakeIntrusive<TBlockstoreConfigImpl>(
        config,
        std::move(featuresConfig),
        std::move(storageConfigCopy),
        std::move(diskAgentConfigCopy));
}

IBlockstoreConfigMutablePtr MakeStartupBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NStorage::TStorageConfigControlsPtr controls,
    TBlockstoreConfigExtraParameters extraParameters)
{
    auto dynamicConfigCopy = dynamicConfig;
    NormalizeDynamicBlockstoreConfig(staticConfig, dynamicConfigCopy);

    return MakeBlockstoreConfig(
        MergeBlockstoreConfigSources(staticConfig, dynamicConfigCopy),
        std::move(controls),
        std::move(extraParameters));
}

NConfig::TRuntimeConfigDiagnostics PrepareRuntimeBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& startupConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NProto::TBlockstoreConfig& runtimeConfig)
{
    auto config = MergeBlockstoreConfigSources(staticConfig, dynamicConfig);
    auto diagnostics =
        NConfig::FilterRuntimeConfig(staticConfig, startupConfig, config);
    runtimeConfig = std::move(config);
    return diagnostics;
}

IBlockstoreConfigMutablePtr MakeRuntimeBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& startupConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NStorage::TStorageConfigControlsPtr controls,
    NConfig::TRuntimeConfigDiagnostics& diagnostics,
    TBlockstoreConfigExtraParameters extraParameters)
{
    // Preserve the received CMS proto for comparison with later notifications.
    auto dynamicConfigCopy = dynamicConfig;
    NormalizeDynamicBlockstoreConfig(staticConfig, dynamicConfigCopy);
    NProto::TBlockstoreConfig runtimeConfig;
    auto runtimeDiagnostics = PrepareRuntimeBlockstoreConfig(
        staticConfig,
        startupConfig,
        dynamicConfigCopy,
        runtimeConfig);

    // Keep the caller's previous report if preparation or adapter construction
    // throws; replace it only for a completed configuration.
    auto config = MakeBlockstoreConfig(
        runtimeConfig,
        std::move(controls),
        std::move(extraParameters));
    diagnostics = std::move(runtimeDiagnostics);
    return config;
}

}   // namespace NCloud::NBlockStore
