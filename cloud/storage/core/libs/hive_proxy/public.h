#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <functional>
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

    // Preferred read-only backup for fallback mode. The regular backup is used
    // when this file is missing or invalid.
    TString GoldenTabletBootInfoBackupFilePath;

    // When set, enables switching from normal to fallback mode at runtime.
    // Switching back to normal mode requires a restart.
    std::function<bool()> FallbackModeProvider;
};

}   // namespace NCloud::NStorage
