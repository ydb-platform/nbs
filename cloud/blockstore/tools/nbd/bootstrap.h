#pragma once

#include "private.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/nbd/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    const TOptionsPtr Options;

    NClient::TClientAppConfigPtr ClientConfig;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

    ILoggingServicePtr Logging;
    TLog GrpcLog;
    IMonitoringServicePtr Monitoring;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    IServerStatsPtr ClientStats;
    IStatsUpdaterPtr StatsUpdater;

    NClient::IClientPtr Client;
    IBlockStorePtr ClientEndpoint;
    NClient::ISessionPtr Session;

    IServerPtr NbdServer;
    IDeviceConnectionPtr NbdDevice;

public:
    TBootstrap(TOptionsPtr options);
    ~TBootstrap();

    void Init();

    void Start();
    void Stop();

private:
    void InitLWTrace();

    void InitClientConfig();
};

}   // namespace NCloud::NBlockStore::NBD
