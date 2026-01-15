#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/discovery/config.h>
#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/notify/iface/config.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/blockstore/libs/ydbstats/config.h>

#include <cloud/storage/core/config/features.pb.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/grpc/utils.h>
#include <cloud/storage/core/libs/iam/iface/config.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/version/version.h>

#include <contrib/ydb/core/protos/nbs/blockstore.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NServer {

using namespace NCloud::NBlockStore::NDiscovery;

////////////////////////////////////////////////////////////////////////////////

TConfigInitializerYdb::TConfigInitializerYdb(TOptionsYdbPtr options)
    : TConfigInitializerCommon(options)
    , NCloud::NStorage::TConfigInitializerYdbBase(options)
    , Options(options)
{}

void TConfigInitializerYdb::InitStatsUploadConfig()
{
    NProto::TYdbStatsConfig statsConfig;
    if (Options->StatsUploadConfig) {
        ParseProtoTextFromFileRobust(Options->StatsUploadConfig, statsConfig);
    }

    StatsConfig = std::make_shared<NYdbStats::TYdbStatsConfig>(statsConfig);
}

void TConfigInitializerYdb::InitStorageConfig()
{
    NProto::TStorageServiceConfig storageConfig;
    if (Options->StorageConfig) {
        ParseProtoTextFromFileRobust(Options->StorageConfig, storageConfig);
    }

    if (ServerConfig && ServerConfig->GetServerConfig()) {
        NStorage::AdaptNodeRegistrationParams(
            {}, // overriddenNodeType, node type is not passed in cmd line
            *ServerConfig->GetServerConfig(),
            storageConfig);
    }

    SetupStorageConfig(storageConfig);

    if (Options->SchemeShardDir) {
        storageConfig.SetSchemeShardDir(GetFullSchemeShardDir());
    }

    if (Options->TemporaryServer) {
        storageConfig.SetDisableManuallyPreemptedVolumesTracking(true);
    }

    StorageConfig = std::make_shared<NStorage::TStorageConfig>(
        storageConfig,
        FeaturesConfig);
}

void TConfigInitializerYdb::InitFeaturesConfig()
{
    NProto::TFeaturesConfig featuresConfig;

    if (Options->FeaturesConfig) {
        ParseProtoTextFromFileRobust(Options->FeaturesConfig, featuresConfig);
    }

    FeaturesConfig =
        std::make_shared<NFeatures::TFeaturesConfig>(featuresConfig);
}

void TConfigInitializerYdb::InitLogbrokerConfig()
{
    NProto::TLogbrokerConfig config;

    if (Options->LogbrokerConfig) {
        ParseProtoTextFromFileRobust(Options->LogbrokerConfig, config);
    }

    if (!config.GetCaCertFilename()) {
        config.SetCaCertFilename(ServerConfig->GetRootCertsFile());
    }

    LogbrokerConfig = std::make_shared<NLogbroker::TLogbrokerConfig>(config);
}

void TConfigInitializerYdb::InitNotifyConfig()
{
    NProto::TNotifyConfig config;

    if (Options->NotifyConfig) {
        ParseProtoTextFromFileRobust(Options->NotifyConfig, config);

        if (!config.GetCaCertFilename()) {
            config.SetCaCertFilename(ServerConfig->GetRootCertsFile());
        }
    }

    NotifyConfig = std::make_shared<NNotify::TNotifyConfig>(config);
}

void TConfigInitializerYdb::InitIamClientConfig()
{
    NProto::TIamClientConfig config;

    if (Options->IamConfig) {
        ParseProtoTextFromFile(Options->IamConfig, config);
    }

    IamClientConfig = std::make_shared<NIamClient::TIamClientConfig>(
        std::move(config));
}

void TConfigInitializerYdb::InitKmsClientConfig()
{
    NProto::TGrpcClientConfig config;

    if (Options->KmsConfig) {
        ParseProtoTextFromFile(Options->KmsConfig, config);
    }

    KmsClientConfig = std::move(config);
}

void TConfigInitializerYdb::InitRootKmsConfig()
{
    NProto::TRootKmsConfig config;

    if (Options->RootKmsConfig) {
        ParseProtoTextFromFile(Options->RootKmsConfig, config);
    }

    if (!config.GetRootCertsFile()) {
        config.SetRootCertsFile(ServerConfig->GetRootCertsFile());
    }

    RootKmsConfig = std::move(config);
}

void TConfigInitializerYdb::InitComputeClientConfig()
{
    NProto::TGrpcClientConfig config;

    if (Options->ComputeConfig) {
        ParseProtoTextFromFile(Options->ComputeConfig, config);
    }

    ComputeClientConfig = std::move(config);
}

bool TConfigInitializerYdb::GetUseNonreplicatedRdmaActor() const
{
    if (!StorageConfig) {
        return false;
    }

    return StorageConfig->GetUseNonreplicatedRdmaActor();
}

TDuration TConfigInitializerYdb::GetInactiveClientsTimeout() const
{
    if (!StorageConfig) {
        return TDuration::Max();
    }

    return StorageConfig->GetInactiveClientsTimeout();
}


void TConfigInitializerYdb::SetupStorageConfig(NProto::TStorageServiceConfig& config) const
{
    if (Options->TemporaryServer) {
        config.SetRemoteMountOnly(true);
        config.SetInactiveClientsTimeout(Max<ui32>());
        config.SetRejectMountOnAddClientTimeout(true);
        config.SetTabletBootInfoBackupFilePath({});
        config.SetPathDescriptionBackupFilePath({});
    }

    config.SetServiceVersionInfo(GetFullVersionString());
}

void TConfigInitializerYdb::ApplyYdbStatsConfig(const TString& text)
{
    NProto::TYdbStatsConfig statsConfig;
    ParseProtoTextFromStringRobust(text, statsConfig);

    StatsConfig = std::make_shared<NYdbStats::TYdbStatsConfig>(statsConfig);
}

void TConfigInitializerYdb::ApplyFeaturesConfig(const TString& text)
{
    NProto::TFeaturesConfig config;
    ParseProtoTextFromStringRobust(text, config);

    FeaturesConfig =
        std::make_shared<NFeatures::TFeaturesConfig>(config);

    // features config has changed, update storage config
    StorageConfig->SetFeaturesConfig(FeaturesConfig);
}

void TConfigInitializerYdb::ApplyLogbrokerConfig(const TString& text)
{
    NProto::TLogbrokerConfig config;
    ParseProtoTextFromStringRobust(text, config);

    if (!config.GetCaCertFilename()) {
        config.SetCaCertFilename(ServerConfig->GetRootCertsFile());
    }

    LogbrokerConfig = std::make_shared<NLogbroker::TLogbrokerConfig>(config);
}

void TConfigInitializerYdb::ApplyNotifyConfig(const TString& text)
{
    NProto::TNotifyConfig config;
    ParseProtoTextFromStringRobust(text, config);

    if (!config.GetCaCertFilename()) {
        config.SetCaCertFilename(ServerConfig->GetRootCertsFile());
    }

    NotifyConfig = std::make_shared<NNotify::TNotifyConfig>(config);
}

void TConfigInitializerYdb::ApplySpdkEnvConfig(const TString& text)
{
    NProto::TSpdkEnvConfig config;
    ParseProtoTextFromStringRobust(text, config);
    SpdkEnvConfig = std::make_shared<NSpdk::TSpdkEnvConfig>(config);
}

void TConfigInitializerYdb::ApplyServerAppConfig(const TString& text)
{
    NProto::TServerAppConfig appConfig;
    ParseProtoTextFromStringRobust(text, appConfig);

    auto& serverConfig = *appConfig.MutableServerConfig();
    SetupServerPorts(serverConfig);

    ServerConfig = std::make_shared<TServerAppConfig>(appConfig);
    SetGrpcThreadsLimit(ServerConfig->GetGrpcThreadsLimit());
}

void TConfigInitializerYdb::ApplyDiagnosticsConfig(const TString& text)
{
    NProto::TDiagnosticsConfig diagnosticsConfig;
    ParseProtoTextFromStringRobust(text, diagnosticsConfig);

    if (Options->MonitoringPort) {
        diagnosticsConfig.SetNbsMonPort(Options->MonitoringPort);
    }

    DiagnosticsConfig = std::make_shared<TDiagnosticsConfig>(diagnosticsConfig);
}

void TConfigInitializerYdb::ApplyStorageServiceConfig(const TString& text)
{
    NProto::TStorageServiceConfig storageConfig;
    ParseProtoTextFromStringRobust(text, storageConfig);

    if (ServerConfig && ServerConfig->GetServerConfig()) {
        NStorage::AdaptNodeRegistrationParams(
            {}, // overriddenNodeType, node type is not passed in cmd line
            *ServerConfig->GetServerConfig(),
            storageConfig);
    }

    SetupStorageConfig(storageConfig);

    if (Options->TemporaryServer) {
        storageConfig.SetDisableManuallyPreemptedVolumesTracking(true);
    }

    StorageConfig = std::make_shared<NStorage::TStorageConfig>(
        storageConfig,
        FeaturesConfig);

    Y_ENSURE(!Options->SchemeShardDir ||
        GetFullSchemeShardDir() == StorageConfig->GetSchemeShardDir());
}

void TConfigInitializerYdb::ApplyDiscoveryServiceConfig(const TString& text)
{
    NProto::TDiscoveryServiceConfig discoveryConfig;
    ParseProtoTextFromStringRobust(text, discoveryConfig);

    SetupDiscoveryPorts(discoveryConfig);

    DiscoveryConfig = std::make_shared<TDiscoveryConfig>(discoveryConfig);
}

void TConfigInitializerYdb::ApplyDiskAgentConfig(const TString& text)
{
    NProto::TDiskAgentConfig config;
    ParseProtoTextFromStringRobust(text, config);

    if (Options->TemporaryServer || config.GetDedicatedDiskAgent()) {
        config.SetEnabled(false);
    }

    DiskAgentConfig = std::make_shared<NStorage::TDiskAgentConfig>(
        std::move(config),
        Rack,
        HostPerformanceProfile.NetworkMbitThroughput);
}

void TConfigInitializerYdb::ApplyDiskRegistryProxyConfig(const TString& text)
{
    NProto::TDiskRegistryProxyConfig config;
    ParseProtoTextFromStringRobust(text, config);

    DiskRegistryProxyConfig = std::make_shared<NStorage::TDiskRegistryProxyConfig>(
        std::move(config));
}

void TConfigInitializerYdb::ApplyIamClientConfig(const TString& text)
{
    NProto::TIamClientConfig config;
    ParseProtoTextFromString(text, config);

    IamClientConfig = std::make_shared<NIamClient::TIamClientConfig>(
        std::move(config));
}

void TConfigInitializerYdb::ApplyKmsClientConfig(const TString& text)
{
    NProto::TGrpcClientConfig config;
    ParseProtoTextFromString(text, config);

    KmsClientConfig = std::move(config);
}

void TConfigInitializerYdb::ApplyRootKmsConfig(const TString& text)
{
    NProto::TRootKmsConfig config;
    ParseProtoTextFromString(text, config);

    RootKmsConfig = std::move(config);
}

void TConfigInitializerYdb::ApplyComputeClientConfig(const TString& text)
{
    NProto::TGrpcClientConfig config;
    ParseProtoTextFromString(text, config);

    ComputeClientConfig = std::move(config);
}

////////////////////////////////////////////////////////////////////////////////

void TConfigInitializerYdb::ApplyNamedConfigs(
    const NKikimrConfig::TAppConfig& config)
{
    THashMap<TString, ui32> configs;

    for (int i = 0; i < config.GetNamedConfigs().size(); ++i) {
        TStringBuf name = config.GetNamedConfigs(i).GetName();

        if (!name.SkipPrefix("Cloud.NBS.")) {
            continue;
        }

        configs.emplace(name, i);
    }

    using TSelf = TConfigInitializerYdb;
    using TApplyFn = void (TSelf::*)(const TString&);

    const TVector<std::pair<TString, TApplyFn>> configHandlers {
        { "ActorSystemConfig",       &TSelf::ApplyActorSystemConfig       },
        { "AuthConfig",              &TSelf::ApplyAuthConfig              },
        { "DiagnosticsConfig",       &TSelf::ApplyDiagnosticsConfig       },
        { "DiscoveryServiceConfig",  &TSelf::ApplyDiscoveryServiceConfig  },
        { "DiskAgentConfig",         &TSelf::ApplyDiskAgentConfig         },
        { "DiskRegistryProxyConfig", &TSelf::ApplyDiskRegistryProxyConfig },
        { "FeaturesConfig",          &TSelf::ApplyFeaturesConfig          },
        { "InterconnectConfig",      &TSelf::ApplyInterconnectConfig      },
        { "LogbrokerConfig",         &TSelf::ApplyLogbrokerConfig         },
        { "LogConfig",               &TSelf::ApplyLogConfig               },
        { "MonitoringConfig",        &TSelf::ApplyMonitoringConfig        },
        { "NotifyConfig",            &TSelf::ApplyNotifyConfig            },
        { "ServerAppConfig",         &TSelf::ApplyServerAppConfig         },
        { "SpdkEnvConfig",           &TSelf::ApplySpdkEnvConfig           },
        { "StorageServiceConfig",    &TSelf::ApplyStorageServiceConfig    },
        { "YdbStatsConfig",          &TSelf::ApplyYdbStatsConfig          },
        { "IamClientConfig",         &TSelf::ApplyIamClientConfig         },
        { "KmsClientConfig",         &TSelf::ApplyKmsClientConfig         },
        { "RootKmsConfig",           &TSelf::ApplyRootKmsConfig           },
        { "ComputeClientConfig",     &TSelf::ApplyComputeClientConfig     },
    };

    for (const auto& handler: configHandlers) {
        auto it = configs.find(handler.first);
        if (it == configs.end()) {
            continue;
        }

        std::invoke(
            handler.second,
            this,
            config.GetNamedConfigs(it->second).GetConfig());
    }
}

void TConfigInitializerYdb::ApplyBlockstoreConfig(
    const NKikimrConfig::TAppConfig& config)
{
    if (!config.HasBlockstoreConfig()) {
        return;
    }

    const auto& blockstoreConfig = config.GetBlockstoreConfig();

    auto volumePreemptionType = static_cast<NProto::EVolumePreemptionType>(
        blockstoreConfig.GetVolumePreemptionType());
    StorageConfig->SetVolumePreemptionType(volumePreemptionType);
}

////////////////////////////////////////////////////////////////////////////////

void TConfigInitializerYdb::ApplyCustomCMSConfigs(
    const NKikimrConfig::TAppConfig& config)
{
    ApplyNamedConfigs(config);
    ApplyBlockstoreConfig(config);
}

}   // namespace NCloud::NBlockStore::NServer
