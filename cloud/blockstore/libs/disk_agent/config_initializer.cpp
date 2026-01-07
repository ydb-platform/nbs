#include "config_initializer.h"

#include "options.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/rdma/iface/config.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<NJson::TJsonValue> ReadJsonFile(
    TLog& Log,
    const TString& filename)
{
    if (filename.empty()) {
        return std::nullopt;
    }

    try {
        TFileInput in(filename);
        return NJson::ReadJsonTree(&in, true);
    } catch (...) {
        STORAGE_ERROR(
            "Failed to read file: " << filename.Quote() << " with error: "
                                    << CurrentExceptionMessage().c_str());
        return std::nullopt;
    }
}

ui32 ReadNetworkMbitThroughput(
    TLog& Log,
    const NProto::TDiskAgentConfig& diskAgentConfig)
{
    const auto& config = diskAgentConfig.GetThrottlingConfig();
    ui32 networkThroughput = config.GetDefaultNetworkMbitThroughput();

    if (auto json = ReadJsonFile(Log, config.GetInfraThrottlingConfigPath())) {
        try {
            if (auto* value = json->GetValueByPath("interfaces.[0].eth0.speed"))
            {
                networkThroughput = FromString<ui32>(value->GetStringSafe());
            }
        } catch (...) {
            STORAGE_ERROR(
                "Failed to read NetworkMbitThroughput. Error: "
                << CurrentExceptionMessage().c_str());
        }
    }

    return networkThroughput;
}

}   // namespace

void TConfigInitializer::ApplyCMSConfigs(NKikimrConfig::TAppConfig cmsConfig)
{
    if (cmsConfig.HasBlobStorageConfig()) {
        KikimrConfig->MutableBlobStorageConfig()
            ->Swap(cmsConfig.MutableBlobStorageConfig());
    }

    if (cmsConfig.HasDomainsConfig()) {
        KikimrConfig->MutableDomainsConfig()
            ->Swap(cmsConfig.MutableDomainsConfig());
    }

    if (cmsConfig.HasNameserviceConfig()) {
        KikimrConfig->MutableNameserviceConfig()
            ->Swap(cmsConfig.MutableNameserviceConfig());
    }

    if (cmsConfig.HasDynamicNameserviceConfig()) {
        KikimrConfig->MutableDynamicNameserviceConfig()
            ->Swap(cmsConfig.MutableDynamicNameserviceConfig());
    }

    ApplyCustomCMSConfigs(cmsConfig);
}

void TConfigInitializer::InitKikimrConfig()
{
    KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();

    auto& logConfig = *KikimrConfig->MutableLogConfig();
    if (Options->LogConfig) {
        ParseProtoTextFromFileRobust(Options->LogConfig, logConfig);
    }

    SetupLogConfig(logConfig);

    auto& sysConfig = *KikimrConfig->MutableActorSystemConfig();
    if (Options->SysConfig) {
        ParseProtoTextFromFile(Options->SysConfig, sysConfig);
    }

    auto& interconnectConfig = *KikimrConfig->MutableInterconnectConfig();
    if (Options->InterconnectConfig) {
        ParseProtoTextFromFile(Options->InterconnectConfig, interconnectConfig);
    }
    interconnectConfig.SetStartTcp(true);

    auto& domainsConfig = *KikimrConfig->MutableDomainsConfig();
    if (Options->DomainsConfig) {
        ParseProtoTextFromFile(Options->DomainsConfig, domainsConfig);
    }

    auto& monConfig = *KikimrConfig->MutableMonitoringConfig();
    if (Options->MonitoringConfig) {
        ParseProtoTextFromFile(Options->MonitoringConfig, monConfig);
    }
    SetupMonitoringConfig(monConfig);

    auto& restartsCountConfig = *KikimrConfig->MutableRestartsCountConfig();
    if (Options->RestartsCountFile) {
        restartsCountConfig.SetRestartsCountFile(Options->RestartsCountFile);
    }

    auto& nameServiceConfig = *KikimrConfig->MutableNameserviceConfig();
    if (Options->NameServiceConfig) {
        ParseProtoTextFromFile(
            Options->NameServiceConfig,
            nameServiceConfig
        );
    }

    if (Options->SuppressVersionCheck) {
        nameServiceConfig.SetSuppressVersionCheck(true);
    }

    auto& dynamicNameServiceConfig =
        *KikimrConfig->MutableDynamicNameserviceConfig();
    if (Options->DynamicNameServiceConfig) {
        ParseProtoTextFromFile(
            Options->DynamicNameServiceConfig,
            dynamicNameServiceConfig
        );
    }

    if (Options->AuthConfig) {
        auto& authConfig = *KikimrConfig->MutableAuthConfig();
        ParseProtoTextFromFile(Options->AuthConfig, authConfig);
    }

    auto& bsConfig = *KikimrConfig->MutableBlobStorageConfig();
    bsConfig.MutableServiceSet()->AddAvailabilityDomains(1);
}

void TConfigInitializer::InitDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig diagnosticsConfig;
    if (Options->DiagnosticsConfig) {
        ParseProtoTextFromFileRobust(Options->DiagnosticsConfig, diagnosticsConfig);
    }

    if (Options->MonitoringPort) {
        diagnosticsConfig.SetNbsMonPort(Options->MonitoringPort);
    }

    DiagnosticsConfig = std::make_shared<TDiagnosticsConfig>(diagnosticsConfig);
}

void TConfigInitializer::InitStorageConfig()
{
    NProto::TStorageServiceConfig storageConfig;
    if (Options->StorageConfig) {
        ParseProtoTextFromFileRobust(Options->StorageConfig, storageConfig);
    }

    if (ServerConfig && ServerConfig->GetServerConfig()) {
        NStorage::AdaptNodeRegistrationParams(
            Options->NodeType,
            *ServerConfig->GetServerConfig(),
            storageConfig);
    }

    if (Options->SchemeShardDir) {
        storageConfig.SetSchemeShardDir(GetFullSchemeShardDir());
    }

    storageConfig.SetServiceVersionInfo(GetFullVersionString());

    StorageConfig = std::make_shared<NStorage::TStorageConfig>(
        storageConfig,
        FeaturesConfig);
}

void TConfigInitializer::InitDiskAgentConfig()
{
    NProto::TDiskAgentConfig diskAgentConfig;
    if (Options->DiskAgentConfig) {
        ParseProtoTextFromFileRobust(Options->DiskAgentConfig, diskAgentConfig);
    }

    SetupDiskAgentConfig(diskAgentConfig);
    ApplySpdkEnvConfig(diskAgentConfig.GetSpdkEnvConfig());

    const ui32 networkMbitThroughput = ReadNetworkMbitThroughput(Log, diskAgentConfig);
    DiskAgentConfig = std::make_shared<NStorage::TDiskAgentConfig>(
        std::move(diskAgentConfig),
        Rack,
        networkMbitThroughput);
}

void TConfigInitializer::InitDiskRegistryProxyConfig()
{
    NProto::TDiskRegistryProxyConfig config;
    if (Options->DiskRegistryProxyConfig) {
        ParseProtoTextFromFileRobust(Options->DiskRegistryProxyConfig, config);
    }

    DiskRegistryProxyConfig = std::make_shared<NStorage::TDiskRegistryProxyConfig>(
        std::move(config));
}

void TConfigInitializer::InitServerConfig()
{
    NProto::TServerAppConfig appConfig;
    if (Options->ServerConfig) {
        ParseProtoTextFromFileRobust(Options->ServerConfig, appConfig);
    }

    ServerConfig = std::make_shared<TServerAppConfig>(appConfig);
}

void TConfigInitializer::InitSpdkEnvConfig()
{
    NProto::TSpdkEnvConfig config;
    SpdkEnvConfig = std::make_shared<NSpdk::TSpdkEnvConfig>(config);
}

void TConfigInitializer::InitFeaturesConfig()
{
    NCloud::NProto::TFeaturesConfig featuresConfig;

    if (Options->FeaturesConfig) {
        ParseProtoTextFromFileRobust(Options->FeaturesConfig, featuresConfig);
    }

    FeaturesConfig =
        std::make_shared<NFeatures::TFeaturesConfig>(featuresConfig);
}

void TConfigInitializer::InitRdmaConfig()
{
    NProto::TRdmaConfig rdmaConfig;

    if (Options->RdmaConfig) {
        ParseProtoTextFromFileRobust(Options->RdmaConfig, rdmaConfig);

        // inherit the target from DiskAgentConfig to smooth out the transition
        if (DiskAgentConfig->DeprecatedHasRdmaTarget()) {
            rdmaConfig.SetDiskAgentTargetEnabled(true);
        }
    } else {
        // no rdma config file is given fallback to legacy config
        if (DiskAgentConfig->DeprecatedHasRdmaTarget()) {
            const auto& oldTarget = DiskAgentConfig->DeprecatedGetRdmaTarget();
            auto* newTarget = rdmaConfig.MutableDiskAgentTarget();

            rdmaConfig.SetServerEnabled(true);
            rdmaConfig.MutableServer()->CopyFrom(oldTarget.GetServer());

            rdmaConfig.SetDiskAgentTargetEnabled(true);
            newTarget->MutableEndpoint()->CopyFrom(oldTarget.GetEndpoint());
            newTarget->SetWorkerThreads(oldTarget.GetWorkerThreads());
        }
    }

    RdmaConfig =
        std::make_shared<NRdma::TRdmaConfig>(rdmaConfig);
}

NKikimrConfig::TLogConfig TConfigInitializer::GetLogConfig() const
{
    // TODO: move to custom config
    NKikimrConfig::TLogConfig logConfig;
    if (Options->LogConfig) {
        ParseProtoTextFromFileRobust(Options->LogConfig, logConfig);
    }

    SetupLogConfig(logConfig);
    return logConfig;
}

NKikimrConfig::TMonitoringConfig TConfigInitializer::GetMonitoringConfig() const
{
    // TODO: move to custom config
    NKikimrConfig::TMonitoringConfig monConfig;
    if (Options->MonitoringConfig) {
        ParseProtoTextFromFile(Options->MonitoringConfig, monConfig);
    }

    SetupMonitoringConfig(monConfig);
    return monConfig;
}

void TConfigInitializer::SetupMonitoringConfig(NKikimrConfig::TMonitoringConfig& monConfig) const
{
    if (Options->MonitoringAddress) {
        monConfig.SetMonitoringAddress(Options->MonitoringAddress);
    }
    if (Options->MonitoringPort) {
        monConfig.SetMonitoringPort(Options->MonitoringPort);
    }
    if (Options->MonitoringThreads) {
        monConfig.SetMonitoringThreads(Options->MonitoringThreads);
    }
    if (!monConfig.HasMonitoringThreads()) {
        monConfig.SetMonitoringThreads(1);  // reasonable defaults
    }
}

void TConfigInitializer::SetupLogConfig(NKikimrConfig::TLogConfig& logConfig) const
{
    if (Options->SysLogService) {
        logConfig.SetSysLogService(Options->SysLogService);
    }

    if (Options->VerboseLevel) {
        auto level = GetLogLevel(Options->VerboseLevel);
        Y_ENSURE(level, "unknown log level: " << Options->VerboseLevel.Quote());

        logConfig.SetDefaultLevel(*level);
    }
}

void TConfigInitializer::SetupDiskAgentConfig(
    NProto::TDiskAgentConfig& config) const
{
    if (!config.GetDedicatedDiskAgent()) {
        config.SetEnabled(false);
    }

    if (config.GetAgentId().empty()) {
        config.SetAgentId(FQDNHostName());
    }

    if (Options->TemporaryAgent) {
        config.SetTemporaryAgent(true);
    }

    if (Options->RdmaTargetPort != 0) {
        config.MutableRdmaTarget()->MutableEndpoint()->SetPort(
            Options->RdmaTargetPort);
    }

    if (!config.HasDataIntegrityValidationPolicyForDrBasedDisks() &&
        config.GetEnableDataIntegrityValidationForDrBasedDisks())
    {
        config.SetDataIntegrityValidationPolicyForDrBasedDisks(
            NProto::DIVP_ENABLED);
    }
}

TString TConfigInitializer::GetFullSchemeShardDir() const
{
    return "/" + Options->Domain + "/" + Options->SchemeShardDir;
}

void TConfigInitializer::ApplyLogConfig(const TString& text)
{
    auto& logConfig = *KikimrConfig->MutableLogConfig();
    ParseProtoTextFromStringRobust(text, logConfig);

    SetupLogConfig(logConfig);
}

void TConfigInitializer::ApplyAuthConfig(const TString& text)
{
    ParseProtoTextFromString(text, *KikimrConfig->MutableAuthConfig());
}

void TConfigInitializer::ApplyFeaturesConfig(const TString& text)
{
    NCloud::NProto::TFeaturesConfig config;
    ParseProtoTextFromStringRobust(text, config);

    FeaturesConfig =
        std::make_shared<NFeatures::TFeaturesConfig>(config);

    // features config has changed, update storage config
    StorageConfig->SetFeaturesConfig(FeaturesConfig);
}

void TConfigInitializer::ApplySpdkEnvConfig(const NProto::TSpdkEnvConfig& orig)
{
    NProto::TSpdkEnvConfig config;
    config.CopyFrom(orig);
    SpdkEnvConfig = std::make_shared<NSpdk::TSpdkEnvConfig>(std::move(config));
}

void TConfigInitializer::ApplyServerAppConfig(const TString& text)
{
    NProto::TServerAppConfig appConfig;
    ParseProtoTextFromStringRobust(text, appConfig);

    ServerConfig = std::make_shared<TServerAppConfig>(appConfig);
}

void TConfigInitializer::ApplyMonitoringConfig(const TString& text)
{
    auto& monConfig = *KikimrConfig->MutableMonitoringConfig();
    ParseProtoTextFromString(text, monConfig);

    SetupMonitoringConfig(monConfig);
}

void TConfigInitializer::ApplyActorSystemConfig(const TString& text)
{
    ParseProtoTextFromString(text, *KikimrConfig->MutableActorSystemConfig());
}

void TConfigInitializer::ApplyDiagnosticsConfig(const TString& text)
{
    NProto::TDiagnosticsConfig diagnosticsConfig;
    ParseProtoTextFromStringRobust(text, diagnosticsConfig);

    if (Options->MonitoringPort) {
        diagnosticsConfig.SetNbsMonPort(Options->MonitoringPort);
    }

    DiagnosticsConfig = std::make_shared<TDiagnosticsConfig>(diagnosticsConfig);
}

void TConfigInitializer::ApplyInterconnectConfig(const TString& text)
{
    auto& interconnectConfig = *KikimrConfig->MutableInterconnectConfig();
    ParseProtoTextFromString(text, interconnectConfig);

    interconnectConfig.SetStartTcp(true);
}

void TConfigInitializer::ApplyStorageServiceConfig(const TString& text)
{
    NProto::TStorageServiceConfig storageConfig;
    ParseProtoTextFromStringRobust(text, storageConfig);

    if (ServerConfig && ServerConfig->GetServerConfig()) {
        NStorage::AdaptNodeRegistrationParams(
            Options->NodeType,
            *ServerConfig->GetServerConfig(),
            storageConfig);
    }

    storageConfig.SetServiceVersionInfo(GetFullVersionString());
    StorageConfig = std::make_shared<NStorage::TStorageConfig>(
        storageConfig,
        FeaturesConfig);

    Y_ENSURE(!Options->SchemeShardDir ||
        GetFullSchemeShardDir() == StorageConfig->GetSchemeShardDir());
}

void TConfigInitializer::ApplyDiskAgentConfig(const TString& text)
{
    NProto::TDiskAgentConfig config;
    ParseProtoTextFromStringRobust(text, config);

    SetupDiskAgentConfig(config);
    ApplySpdkEnvConfig(config.GetSpdkEnvConfig());

    if (!config.GetStorageDiscoveryConfig().PathConfigsSize()) {
        config.MutableStorageDiscoveryConfig()->CopyFrom(
            DiskAgentConfig->GetStorageDiscoveryConfig());
    }

    const ui32 networkMbitThroughput = ReadNetworkMbitThroughput(Log, config);
    DiskAgentConfig = std::make_shared<NStorage::TDiskAgentConfig>(
        std::move(config),
        Rack,
        networkMbitThroughput);
}

void TConfigInitializer::ApplyDiskRegistryProxyConfig(const TString& text)
{
    NProto::TDiskRegistryProxyConfig config;
    ParseProtoTextFromStringRobust(text, config);

    DiskRegistryProxyConfig = std::make_shared<NStorage::TDiskRegistryProxyConfig>(
        std::move(config));
}

void TConfigInitializer::ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config)
{
    THashMap<TString, ui32> configs;

    for (int i = 0; i < config.GetNamedConfigs().size(); ++i) {
        TStringBuf name = config.GetNamedConfigs(i).GetName();

        if (!name.SkipPrefix("Cloud.NBS.")) {
            continue;
        }

        configs.emplace(name, i);
    }

    using TSelf = TConfigInitializer;
    using TApplyFn = void (TSelf::*)(const TString&);

    const TVector<std::pair<TString, TApplyFn>> configHandlers {
        { "ActorSystemConfig",       &TSelf::ApplyActorSystemConfig       },
        { "AuthConfig",              &TSelf::ApplyAuthConfig              },
        { "DiagnosticsConfig",       &TSelf::ApplyDiagnosticsConfig       },
        { "DiskAgentConfig",         &TSelf::ApplyDiskAgentConfig         },
        { "DiskRegistryProxyConfig", &TSelf::ApplyDiskRegistryProxyConfig },
        { "InterconnectConfig",      &TSelf::ApplyInterconnectConfig      },
        { "LogConfig",               &TSelf::ApplyLogConfig               },
        { "MonitoringConfig",        &TSelf::ApplyMonitoringConfig        },
        { "ServerAppConfig",         &TSelf::ApplyServerAppConfig         },
        { "FeaturesConfig",          &TSelf::ApplyFeaturesConfig          },
        { "StorageServiceConfig",    &TSelf::ApplyStorageServiceConfig    },
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

}   // namespace NCloud::NBlockStore::NServer
