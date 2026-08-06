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
    TString GoldenTabletBootInfoBackupFilePath;

    // When set, enables runtime switching to fallback mode via
    // THiveProxyRouter. Switching back to normal mode requires a restart.
    std::function<bool()> FallbackModeProvider = {};
};

}   // namespace NCloud::NStorage
