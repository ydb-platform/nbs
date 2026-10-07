#pragma once

#include "public.h"

#include "config.h"
#include "device_manager.h"

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/network/socket.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

class TServerBuilder
{
private:
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;
    IDeviceManagerPtr DeviceManager;
    const TNetworkAddress ListenAddress;
    const bool JournalEnabled;
    const ui32 RestoreConcurrency;
    TVector<TJournalledDeviceConfig> DeviceConfigs;

public:
    TServerBuilder(
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IDeviceManagerPtr deviceManager,
        const TNetworkAddress& listenAddress,
        bool journalEnabled,
        ui32 restoreConcurrency,
        TVector<TJournalledDeviceConfig> deviceConfigs);

    // Devices that fail to be created are reported and skipped; throws
    // TServiceError if none of them could be created. The server starts the
    // devices with at most |restoreConcurrency| of them restoring at once.
    IStartablePtr Build();
};

}   // namespace NCloud::NJournalled
