#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/diagnostics/config.h>
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

}   // namespace NCloud::NFileStore::NDaemon
