/*******************************************************************************

Configuration construction owns immutable runtime wrappers for all merged
sections. Storage configuration copies retain the shared ICB controls and use
the features wrapper created from the merged protobuf configuration.

*******************************************************************************/

#include "blockstore_config.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

// The private IBlockstoreConfig implementation and owner of its runtime
// wrappers and protobuf sections. Only MakeBlockstoreConfig() creates
// instances.
class TBlockstoreConfigImpl final: public IBlockstoreConfig
{
public:
    TBlockstoreConfigImpl(
        const NProto::TBlockstoreConfig& config,
        NFeatures::TFeaturesConfigConstPtr featuresConfig,
        NStorage::TStorageConfigConstPtr storageConfig,
        NStorage::TDiskAgentConfigConstPtr diskAgentConfig);

    const NServer::TServerAppConfigConstPtr& GetServerConfig() const override;

    const NFeatures::TFeaturesConfigConstPtr&
    GetFeaturesConfig() const override;

    const NStorage::TStorageConfigConstPtr& GetStorageConfig() const override;

    const TDiagnosticsConfigConstPtr& GetDiagnosticsConfig() const override;

    const NDiscovery::TDiscoveryConfigConstPtr&
    GetDiscoveryServiceConfig() const override;

    const NClient::TClientAppConfigConstPtr& GetEndpointConfig() const override;

    const NStorage::TDiskAgentConfigConstPtr&
    GetDiskAgentConfig() const override;

    const NStorage::TDiskRegistryProxyConfigConstPtr&
    GetDiskRegistryProxyConfig() const override;

    const NSpdk::TSpdkEnvConfigConstPtr& GetSpdkEnvConfig() const override;

    const NRdma::TRdmaConfigConstPtr& GetRdmaConfig() const override;

    const NYdbStats::TYdbStatsConfigConstPtr&
    GetYdbStatsConfig() const override;

    const NLogbroker::TLogbrokerConfigConstPtr&
    GetLogbrokerConfig() const override;

    const NNotify::TNotifyConfigConstPtr& GetNotifyConfig() const override;

    const NIamClient::TIamClientConfigConstPtr&
    GetIamClientConfig() const override;

    const TGrpcClientConfigConstPtr& GetKmsClientConfig() const override;

    const TGrpcClientConfigConstPtr& GetComputeClientConfig() const override;

    const TRootKmsConfigConstPtr& GetRootKmsConfig() const override;

    const NCells::TCellsConfigConstPtr& GetCellsConfig() const override;

    const TLocalNVMeConfigConstPtr& GetLocalNVMeConfig() const override;

private:
    const NServer::TServerAppConfigConstPtr ServerConfig;
    const NFeatures::TFeaturesConfigConstPtr FeaturesConfig;
    const NStorage::TStorageConfigConstPtr StorageConfig;
    const TDiagnosticsConfigConstPtr DiagnosticsConfig;
    const NDiscovery::TDiscoveryConfigConstPtr DiscoveryServiceConfig;
    const NClient::TClientAppConfigConstPtr EndpointConfig;
    const NStorage::TDiskAgentConfigConstPtr DiskAgentConfig;
    const NStorage::TDiskRegistryProxyConfigConstPtr DiskRegistryProxyConfig;
    const NSpdk::TSpdkEnvConfigConstPtr SpdkEnvConfig;
    const NRdma::TRdmaConfigConstPtr RdmaConfig;
    const NYdbStats::TYdbStatsConfigConstPtr YdbStatsConfig;
    const NLogbroker::TLogbrokerConfigConstPtr LogbrokerConfig;
    const NNotify::TNotifyConfigConstPtr NotifyConfig;
    const NIamClient::TIamClientConfigConstPtr IamClientConfig;
    const TGrpcClientConfigConstPtr KmsClientConfig;
    const TGrpcClientConfigConstPtr ComputeClientConfig;
    const TRootKmsConfigConstPtr RootKmsConfig;
    const NCells::TCellsConfigConstPtr CellsConfig;
    const TLocalNVMeConfigConstPtr LocalNVMeConfig;
};

TBlockstoreConfigImpl::TBlockstoreConfigImpl(
    const NProto::TBlockstoreConfig& config,
    NFeatures::TFeaturesConfigConstPtr featuresConfig,
    NStorage::TStorageConfigConstPtr storageConfig,
    NStorage::TDiskAgentConfigConstPtr diskAgentConfig)
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

const NServer::TServerAppConfigConstPtr&
TBlockstoreConfigImpl::GetServerConfig() const
{
    return ServerConfig;
}

const NFeatures::TFeaturesConfigConstPtr&
TBlockstoreConfigImpl::GetFeaturesConfig() const
{
    return FeaturesConfig;
}

const NStorage::TStorageConfigConstPtr&
TBlockstoreConfigImpl::GetStorageConfig() const
{
    return StorageConfig;
}

const TDiagnosticsConfigConstPtr&
TBlockstoreConfigImpl::GetDiagnosticsConfig() const
{
    return DiagnosticsConfig;
}

const NDiscovery::TDiscoveryConfigConstPtr&
TBlockstoreConfigImpl::GetDiscoveryServiceConfig() const
{
    return DiscoveryServiceConfig;
}

const NClient::TClientAppConfigConstPtr&
TBlockstoreConfigImpl::GetEndpointConfig() const
{
    return EndpointConfig;
}

const NStorage::TDiskAgentConfigConstPtr&
TBlockstoreConfigImpl::GetDiskAgentConfig() const
{
    return DiskAgentConfig;
}

const NStorage::TDiskRegistryProxyConfigConstPtr&
TBlockstoreConfigImpl::GetDiskRegistryProxyConfig() const
{
    return DiskRegistryProxyConfig;
}

const NSpdk::TSpdkEnvConfigConstPtr&
TBlockstoreConfigImpl::GetSpdkEnvConfig() const
{
    return SpdkEnvConfig;
}

const NRdma::TRdmaConfigConstPtr& TBlockstoreConfigImpl::GetRdmaConfig() const
{
    return RdmaConfig;
}

const NYdbStats::TYdbStatsConfigConstPtr&
TBlockstoreConfigImpl::GetYdbStatsConfig() const
{
    return YdbStatsConfig;
}

const NLogbroker::TLogbrokerConfigConstPtr&
TBlockstoreConfigImpl::GetLogbrokerConfig() const
{
    return LogbrokerConfig;
}

const NNotify::TNotifyConfigConstPtr&
TBlockstoreConfigImpl::GetNotifyConfig() const
{
    return NotifyConfig;
}

const NIamClient::TIamClientConfigConstPtr&
TBlockstoreConfigImpl::GetIamClientConfig() const
{
    return IamClientConfig;
}

const TGrpcClientConfigConstPtr&
TBlockstoreConfigImpl::GetKmsClientConfig() const
{
    return KmsClientConfig;
}

const TGrpcClientConfigConstPtr&
TBlockstoreConfigImpl::GetComputeClientConfig() const
{
    return ComputeClientConfig;
}

const TRootKmsConfigConstPtr& TBlockstoreConfigImpl::GetRootKmsConfig() const
{
    return RootKmsConfig;
}

const NCells::TCellsConfigConstPtr&
TBlockstoreConfigImpl::GetCellsConfig() const
{
    return CellsConfig;
}

const TLocalNVMeConfigConstPtr&
TBlockstoreConfigImpl::GetLocalNVMeConfig() const
{
    return LocalNVMeConfig;
}

// Merge feature definitions by name so an overridden feature has one runtime
// record. Preserve the existing first-wins behavior within each source layer.
void MergeFeatures(
    const NCloud::NProto::TFeaturesConfig& staticFeatures,
    const NCloud::NProto::TFeaturesConfig& dynamicFeatures,
    NCloud::NProto::TFeaturesConfig* result)
{
    // Leave the result unchanged when there are no feature records to merge.
    if (!staticFeatures.FeaturesSize() && !dynamicFeatures.FeaturesSize()) {
        return;
    }

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
            featureIndices.emplace(feature.GetName(), features.size());
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

NProto::TBlockstoreConfig MergeBlockstoreConfig(
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

IBlockstoreConfigPtr MakeBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NStorage::TStorageConfigControlsPtr controls,
    TBlockstoreConfigExtraParameters extraParameters)
{
    Y_ABORT_UNLESS(controls);

    auto config = MergeBlockstoreConfig(staticConfig, dynamicConfig);
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

IBlockstoreConfigPtr MakeBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    const NStorage::TStorageConfig& storageConfig,
    const NStorage::TDiskAgentConfig& diskAgentConfig)
{
    const auto config = MergeBlockstoreConfig(staticConfig, dynamicConfig);
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

}   // namespace NCloud::NBlockStore
