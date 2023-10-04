#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/kikimr/config_initializer.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerCommon
    : public NCloud::NStorage::TConfigInitializerYdbBase
{
    TOptionsCommonPtr Options;

    TDiagnosticsConfigPtr DiagnosticsConfig;
    NStorage::TStorageConfigPtr StorageConfig;

    TConfigInitializerCommon(TOptionsCommonPtr options);

    void InitDiagnosticsConfig();
    void InitStorageConfig();
};

}   // namespace NCloud::NFileStore::NDaemon
