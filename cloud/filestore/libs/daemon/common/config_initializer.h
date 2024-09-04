#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/kikimr/config_initializer.h>
#include <cloud/storage/core/libs/kikimr/node_registration_settings.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerCommon
    : public NCloud::NStorage::TConfigInitializerYdbBase
{
protected:
    using TConfigHandlerFn = std::function<void(const TString&)>;
    using TConfigHandlers = THashMap<TString, TConfigHandlerFn>;

    TConfigInitializerCommon(
        TConfigHandlers configHandlers,
        TOptionsCommonPtr options);

private:
    TConfigHandlers ConfigHandlers;

public:
    const TOptionsCommonPtr Options;

    TDiagnosticsConfigPtr DiagnosticsConfig;
    NStorage::TStorageConfigPtr StorageConfig;
    NFeatures::TFeaturesConfigPtr FeaturesConfig;

    void InitDiagnosticsConfig();
    void InitStorageConfig();
    void InitFeaturesConfig();

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) final;

    NCloud::NStorage::TNodeRegistrationSettings GetNodeRegistrationSettings();

private:
    void ApplyDiagnosticsConfig(const TString& text);
    void ApplyStorageConfig(const TString& text);
    void ApplyFeaturesConfig(const TString& text);

    void ApplyOptionsToStorageConfig(NProto::TStorageConfig& storageConfig);
};

}   // namespace NCloud::NFileStore::NDaemon
