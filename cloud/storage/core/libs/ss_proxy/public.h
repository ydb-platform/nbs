#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TSSProxyConfig
{
    int LogComponent = 0;
    ui32 PipeClientRetryCount = 0;
    TDuration PipeClientMinRetryTime;
    TDuration PipeClientMaxRetryTime;
    TString SchemeShardDir;
    TString PathDescriptionBackupFilePath;
};

}   // namespace NCloud::NStorage
