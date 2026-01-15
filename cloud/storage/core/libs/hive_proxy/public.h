#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <memory>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct THiveProxyConfig
{
    ui32 PipeClientRetryCount = 0;
    TDuration PipeClientMinRetryTime;
    TDuration HiveLockExpireTimeout;
    int LogComponent = 0;
    TString TabletBootInfoBackupFilePath;
    bool UseBinaryFormatForTabletBootInfoBackup = false;
    bool FallbackMode = false;
    ui64 TenantHiveTabletId = 0;
};

}   // namespace NCloud::NStorage
