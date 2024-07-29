#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
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

    NCloud::NStorage::TNodeRegistrationSettings GetNodeRegistrationSettings();
};

}   // namespace NCloud::NFileStore::NDaemon
