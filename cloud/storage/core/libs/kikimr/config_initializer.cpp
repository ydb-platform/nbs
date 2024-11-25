#include "config_initializer.h"
#include "options.h"

#include <contrib/ydb/core/protos/auth.pb.h>
#include <contrib/ydb/core/protos/blobstorage.pb.h>
#include <contrib/ydb/core/protos/feature_flags.pb.h>
#include <contrib/ydb/core/protos/shared_cache.pb.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TConfigInitializerYdbBase::ApplyCMSConfigs(NKikimrConfig::TAppConfig cmsConfig)
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

void TConfigInitializerYdbBase::InitKikimrConfig()
{
    KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();

    *KikimrConfig->MutableLogConfig() = GetLogConfig();

    auto& sysConfig = *KikimrConfig->MutableActorSystemConfig();
    if (Options->SysConfig) {
        ParseProtoTextFromFile(Options->SysConfig, sysConfig);
    }

    auto& interconnectConfig = *KikimrConfig->MutableInterconnectConfig();
    if (Options->InterconnectConfig) {
        ParseProtoTextFromFile(
            Options->InterconnectConfig,
            interconnectConfig);
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

    if (Options->EnableVersionCheck) {
        nameServiceConfig.SetSuppressVersionCheck(false);
    } else {
        // We would like to have SuppressVersionCheck enabled by default.
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

    auto& kikimrFeaturesConfig = *KikimrConfig->MutableFeatureFlags();
    if (Options->KikimrFeaturesConfig) {
        ParseProtoTextFromFile(
            Options->KikimrFeaturesConfig,
            kikimrFeaturesConfig
        );
    } else {
        // we want to use VPatch by default for EvPatch
        kikimrFeaturesConfig.SetEnableVPatch(true);
    }

    if (Options->SharedCacheConfig) {
        auto& sharedCacheConfig = *KikimrConfig->MutableSharedCacheConfig();
        ParseProtoTextFromFile(
            Options->SharedCacheConfig,
            sharedCacheConfig);
    }
}

NKikimrConfig::TLogConfig TConfigInitializerYdbBase::GetLogConfig() const
{
    NKikimrConfig::TLogConfig logConfig;
    if (Options->LogConfig) {
        ParseProtoTextFromFileRobust(Options->LogConfig, logConfig);
    }

    SetupLogLevel(logConfig);
    logConfig.SetIgnoreUnknownComponents(true);
    return logConfig;
}

NKikimrConfig::TMonitoringConfig TConfigInitializerYdbBase::GetMonitoringConfig() const
{
    NKikimrConfig::TMonitoringConfig monConfig;
    if (Options->MonitoringConfig) {
        ParseProtoTextFromFile(Options->MonitoringConfig, monConfig);
    }

    SetupMonitoringConfig(monConfig);
    return monConfig;
}

void TConfigInitializerYdbBase::SetupLogLevel(NKikimrConfig::TLogConfig& logConfig) const
{
    if (Options->VerboseLevel) {
        auto level = GetLogLevel(Options->VerboseLevel);
        Y_ENSURE(level, "unknown log level: " << Options->VerboseLevel.Quote());

        logConfig.SetDefaultLevel(*level);
    }
}

void TConfigInitializerYdbBase::SetupMonitoringConfig(NKikimrConfig::TMonitoringConfig& monConfig) const
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

TString TConfigInitializerYdbBase::GetFullSchemeShardDir() const
{
    return "/" + Options->Domain + "/" + Options->SchemeShardDir;
}

void TConfigInitializerYdbBase::ApplyMonitoringConfig(const TString& text)
{
    auto& monConfig = *KikimrConfig->MutableMonitoringConfig();
    ParseProtoTextFromString(text, monConfig);

    SetupMonitoringConfig(monConfig);
}

void TConfigInitializerYdbBase::ApplyActorSystemConfig(const TString& text)
{
    ParseProtoTextFromString(text, *KikimrConfig->MutableActorSystemConfig());
}

void TConfigInitializerYdbBase::ApplyInterconnectConfig(const TString& text)
{
    auto& interconnectConfig = *KikimrConfig->MutableInterconnectConfig();
    ParseProtoTextFromString(text, interconnectConfig);

    interconnectConfig.SetStartTcp(true);
}

void TConfigInitializerYdbBase::ApplyLogConfig(const TString& text)
{
    auto& logConfig = *KikimrConfig->MutableLogConfig();
    ParseProtoTextFromStringRobust(text, logConfig);

    SetupLogLevel(logConfig);
    logConfig.SetIgnoreUnknownComponents(true);
}

void TConfigInitializerYdbBase::ApplyAuthConfig(const TString& text)
{
    ParseProtoTextFromString(text, *KikimrConfig->MutableAuthConfig());
}

ui32 TConfigInitializerYdbBase::GetLogDefaultLevel() const
{
    return GetLogConfig().GetDefaultLevel();
}

ui32 TConfigInitializerYdbBase::GetMonitoringPort() const
{
    return GetMonitoringConfig().GetMonitoringPort();
}

TString TConfigInitializerYdbBase::GetMonitoringAddress() const
{
    return GetMonitoringConfig().GetMonitoringAddress();
}

ui32 TConfigInitializerYdbBase::GetMonitoringThreads() const
{
    return GetMonitoringConfig().GetMonitoringThreads();
}

}   // namespace NCloud::NStorage
