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
    // Timeout for an external-boot actor that has no waiters left.
    // 0 keeps the worker until HIVE replies.
    TDuration ExternalBootRequestIdleTimeout;
    int LogComponent = 0;
    TString TabletBootInfoBackupFilePath;
    bool UseBinaryFormatForTabletBootInfoBackup = false;
    bool FallbackMode = false;
    ui64 TenantHiveTabletId = 0;
};

}   // namespace NCloud::NStorage
