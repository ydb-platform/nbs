#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/kikimr/config_initializer.h>
#include <cloud/storage/core/libs/kikimr/node_registration_settings.h>

#include <functional>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

class TConfigInitializerCommon
    : public NCloud::NStorage::TConfigInitializerYdbBase
{
protected:
    using TApplyConfigFn = std::function<void(const TString&)>;

public:
    TOptionsCommonPtr Options;

    TDiagnosticsConfigPtr DiagnosticsConfig;
    NStorage::TStorageConfigPtr StorageConfig;
    NFeatures::TFeaturesConfigPtr FeaturesConfig;

    TConfigInitializerCommon(TOptionsCommonPtr options);

    void InitDiagnosticsConfig();
    void InitStorageConfig();
    void InitFeaturesConfig();

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) override;

    NCloud::NStorage::TNodeRegistrationSettings GetNodeRegistrationSettings();

private:
    void ApplyDiagnosticsConfig(const TString& text);
    void ApplyStorageConfig(const TString& text);
    void ApplyFeaturesConfig(const TString& text);

protected:
    static void ApplyConfigs(
        const NKikimrConfig::TAppConfig& config,
        const THashMap<TString, TApplyConfigFn>& handlers);
};

}   // namespace NCloud::NFileStore::NDaemon
