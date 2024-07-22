#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/server/config.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NFileStore::NDaemon {

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

void TConfigInitializerCommon::ApplyCustomCMSConfigs(
    const NKikimrConfig::TAppConfig& config)
{
    using TSelf = TConfigInitializerCommon;
    using TApplyFn = void (TSelf::*)(const TString&);

    const THashMap<TString, TApplyFn> map {
        { "DiagnosticsConfig",    &TSelf::ApplyDiagnosticsConfig       },
        { "FeaturesConfig",       &TSelf::ApplyFeaturesConfig          },
        { "StorageConfig",        &TSelf::ApplyStorageConfig           },
    };

    for (auto& item : config.GetNamedConfigs()) {
        TStringBuf name = item.GetName();
        if (!name.SkipPrefix("Cloud.NFS.")) {
            continue;
        }

        auto it = map.find(name);
        if (it != map.end()) {
            std::invoke(it->second, this, item.GetConfig());
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

}   // namespace NCloud::NFileStore::NDaemon
