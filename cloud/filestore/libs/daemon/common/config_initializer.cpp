#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

TConfigInitializerCommon::TConfigInitializerCommon(TOptionsCommonPtr options)
    : TConfigInitializerYdbBase(options)
    , Options(std::move(options))
{}

void TConfigInitializerCommon::InitDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig config;
    if (Options->DiagnosticsConfig) {
        ParseProtoTextFromFileRobust(Options->DiagnosticsConfig, config);
    }

    DiagnosticsConfig = std::make_shared<TDiagnosticsConfig>(
        std::move(config));
}

void TConfigInitializerCommon::InitStorageConfig()
{
    NProto::TStorageConfig storageConfig;
    if (Options->StorageConfig) {
        ParseProtoTextFromFileRobust(Options->StorageConfig, storageConfig);
    }

    if (Options->SchemeShardDir) {
        storageConfig.SetSchemeShardDir(Options->SchemeShardDir);
    }

    if (Options->DisableLocalService) {
        storageConfig.SetDisableLocalService(true);
    }

    StorageConfig = std::make_shared<NStorage::TStorageConfig>(
        storageConfig);
}

void TConfigInitializerCommon::InitFeaturesConfig()
{
    NCloud::NProto::TFeaturesConfig featuresConfig;
    if (Options->FeaturesConfig) {
        ParseProtoTextFromFileRobust(Options->FeaturesConfig, featuresConfig);
    }

    FeaturesConfig = std::make_shared<NFeatures::TFeaturesConfig>(
        std::move(featuresConfig));
}

TNodeRegistrationSettings
    TConfigInitializerCommon::GetNodeRegistrationSettings()
{
    TNodeRegistrationSettings settings;
    settings.MaxAttempts = Options->NodeRegistrationMaxAttempts;
    settings.RegistrationTimeout = Options->NodeRegistrationTimeout;
    settings.ErrorTimeout = Options->NodeRegistrationErrorTimeout;
    settings.PathToGrpcCaFile = StorageConfig->GetNodeRegistrationRootCertsFile();
    settings.NodeRegistrationToken = StorageConfig->GetNodeRegistrationToken();
    settings.NodeType = StorageConfig->GetNodeType();

    const auto& cert = StorageConfig->GetNodeRegistrationCert();
    settings.PathToGrpcCertFile = cert.CertFile;
    settings.PathToGrpcPrivateKeyFile = cert.CertPrivateKeyFile;

    return settings;
}

void TConfigInitializerCommon::ApplyCustomCMSConfigs(
    const NKikimrConfig::TAppConfig& config)
{
    using TSelf = TConfigInitializerCommon;

    const THashMap<TString, TApplyConfigFn> map {
        { "DiagnosticsConfig", bind_front(&TSelf::ApplyDiagnosticsConfig, this)},
        { "FeaturesConfig",    bind_front(&TSelf::ApplyFeaturesConfig, this)   },
        { "StorageConfig",     bind_front(&TSelf::ApplyStorageConfig, this)    },
    };

    ApplyConfigs(config, map);
}

void TConfigInitializerCommon::ApplyDiagnosticsConfig(const TString& text)
{
    NProto::TDiagnosticsConfig config;
    ParseProtoTextFromStringRobust(text, config);

    DiagnosticsConfig = std::make_shared<TDiagnosticsConfig>(
        std::move(config));
}

void TConfigInitializerCommon::ApplyStorageConfig(const TString& text)
{
    NProto::TStorageConfig config;
    ParseProtoTextFromStringRobust(text, config);

    StorageConfig = std::make_shared<NStorage::TStorageConfig>(
        std::move(config));
}

void TConfigInitializerCommon::ApplyFeaturesConfig(const TString& text)
{
    NCloud::NProto::TFeaturesConfig config;
    ParseProtoTextFromStringRobust(text, config);

    FeaturesConfig = std::make_shared<NFeatures::TFeaturesConfig>(
        std::move(config));
}

void TConfigInitializerCommon::ApplyConfigs(
    const NKikimrConfig::TAppConfig& config,
    const THashMap<TString, TApplyConfigFn>& handlers)
{
    for (const auto& item: config.GetNamedConfigs()) {
        TStringBuf name = item.GetName();
        if (!name.SkipPrefix("Cloud.Filestore.")) {
            continue;
        }

        auto it = handlers.find(name);
        if (it != handlers.end()) {
            std::invoke(it->second, item.GetConfig());
        }
    }
}

}   // namespace NCloud::NFileStore::NDaemon
