#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/server/public.h>
#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/kikimr/config_initializer.h>
#include <cloud/storage/core/libs/kikimr/node_registration_settings.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerCommon
    : public NCloud::NStorage::TConfigInitializerYdbBase
{
    TOptionsCommonPtr Options;

    TDiagnosticsConfigPtr DiagnosticsConfig;
    NStorage::TStorageConfigPtr StorageConfig;
    NFeatures::TFeaturesConfigPtr FeaturesConfig;

    TConfigInitializerCommon(TOptionsCommonPtr options);

    void InitDiagnosticsConfig();
    void InitStorageConfig();
    void InitFeaturesConfig();

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) override;
    void ApplyDiagnosticsConfig(const TString& text);
    void ApplyStorageConfig(const TString& text);
    void ApplyFeaturesConfig(const TString& text);

    virtual NCloud::NStorage::TNodeRegistrationSettings
        GetNodeRegistrationSettings() = 0;
};

}   // namespace NCloud::NFileStore::NDaemon
