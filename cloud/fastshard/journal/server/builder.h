#pragma once

#include "public.h"

#include "config.h"
#include "device_manager.h"

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/vector.h>
#include <util/network/socket.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
{
    TNetworkAddress ListenAddress;
    bool JournalEnabled = false;
    ui32 RestoreConcurrency = 4;

    // The executors the connections are spread over, round robin. Empty
    // means the server's own executor serves every connection.
    TVector<TExecutorPtr> ConnectionExecutors;
};

////////////////////////////////////////////////////////////////////////////////

class TServerBuilder
{
private:
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;
    IDeviceManagerPtr DeviceManager;
    TServerConfig ServerConfig;
    TVector<TJournalledDeviceConfig> DeviceConfigs;

public:
    TServerBuilder(
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IDeviceManagerPtr deviceManager,
        TServerConfig serverConfig,
        TVector<TJournalledDeviceConfig> deviceConfigs);

    // The devices are created when the server starts, with at most
    // |RestoreConcurrency| of them restoring at once. Devices that fail to be
    // created are reported and skipped.
    IStartablePtr Build();
};

}   // namespace NCloud::NJournalled
