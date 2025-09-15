#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

TConfigInitializerCommon::TConfigInitializerCommon(
        TConfigHandlers configHandlers,
        TOptionsCommonPtr options)
    : TConfigInitializerYdbBase(options)
    , ConfigHandlers(std::move(configHandlers))
    , Options(std::move(options))
{
    ConfigHandlers.insert(
        {"DiagnosticsConfig",
         bind_front(&TConfigInitializerCommon::ApplyDiagnosticsConfig, this)});
    ConfigHandlers.insert(
        {"FeaturesConfig",
         bind_front(&TConfigInitializerCommon::ApplyFeaturesConfig, this)});
    ConfigHandlers.insert(
        {"StorageConfig",
         bind_front(&TConfigInitializerCommon::ApplyStorageConfig, this)});
}

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

    ApplyOptionsToStorageConfig(storageConfig);

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
    settings.MaxAttempts = StorageConfig->GetNodeRegistrationMaxAttempts();
    settings.LegacyRegistrationTimeout = StorageConfig->GetNodeRegistrationTimeout();
    settings.LoadConfigsFromCmsRetryMinDelay = StorageConfig->GetLoadConfigsFromCmsRetryMinDelay(),
    settings.LoadConfigsFromCmsRetryMaxDelay = StorageConfig->GetLoadConfigsFromCmsRetryMaxDelay(),
    settings.LoadConfigsFromCmsTotalTimeout = StorageConfig->GetLoadConfigsFromCmsTotalTimeout(),
    settings.ErrorTimeout = StorageConfig->GetNodeRegistrationErrorTimeout();
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
    for (const auto& item: config.GetNamedConfigs()) {
        TStringBuf name = item.GetName();
        if (!name.SkipPrefix("Cloud.Filestore.")) {
            continue;
        }

        if (auto* handler = ConfigHandlers.FindPtr(name)) {
            std::invoke(*handler, item.GetConfig());
        }
    }
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

    ApplyOptionsToStorageConfig(config);

    StorageConfig = std::make_shared<NStorage::TStorageConfig>(
        std::move(config));

    Y_ENSURE(!Options->SchemeShardDir ||
        GetFullSchemeShardDir() == StorageConfig->GetSchemeShardDir());
}

void TConfigInitializerCommon::ApplyFeaturesConfig(const TString& text)
{
    NCloud::NProto::TFeaturesConfig config;
    ParseProtoTextFromStringRobust(text, config);

    FeaturesConfig = std::make_shared<NFeatures::TFeaturesConfig>(
        std::move(config));
}

void TConfigInitializerCommon::ApplyOptionsToStorageConfig(
    NProto::TStorageConfig& storageConfig)
{
    if (Options->SchemeShardDir) {
        storageConfig.SetSchemeShardDir(Options->SchemeShardDir);
    }

    if (Options->DisableLocalService) {
        storageConfig.SetDisableLocalService(true);
    }

    if (Options->NodeRegistrationErrorTimeout) {
        storageConfig.SetNodeRegistrationErrorTimeout(
            Options->NodeRegistrationErrorTimeout.MilliSeconds());
    }

    if (Options->NodeRegistrationMaxAttempts) {
        storageConfig.SetNodeRegistrationMaxAttempts(
            Options->NodeRegistrationMaxAttempts);
    }

    if (Options->NodeRegistrationTimeout) {
        storageConfig.SetNodeRegistrationTimeout(
            Options->NodeRegistrationTimeout.MilliSeconds());
    }
}

}   // namespace NCloud::NFileStore::NDaemon
