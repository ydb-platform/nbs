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
    TString TabletBootInfoCacheFilePath;
    bool FallbackMode = false;
};

}   // namespace NCloud::NStorage
