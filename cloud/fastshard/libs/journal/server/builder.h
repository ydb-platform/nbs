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
    ui32 RestoreConcurrency = 1;

    // The executors that serve the connections: they read the requests, run
    // ReadPages and WriteLogRecord and send the responses. The executor of
    // the builder keeps the listener, the devices and the journals: the
    // restore, the flush, the format and the other rare requests run there.
    // If the list is empty, the executor of the builder does everything.
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

    // Devices that fail to be created are reported and skipped; throws
    // TServiceError if none of them could be created.
    IStartablePtr Build();
};

}   // namespace NCloud::NJournalled
