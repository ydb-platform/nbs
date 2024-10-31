#pragma once

#include "private.h"

#include <cloud/vm/api/blockstore-plugin.h>
#include <cloud/vm/blockstore/lib/public.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/nbd/public.h>
#include <cloud/blockstore/libs/throttling/public.h>
#include <cloud/storage/core/libs/grpc/init.h>

#include <cloud/blockstore/config/plugin.pb.h>

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NPlugin {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    ILoggingServicePtr Logging;
    TLog GrpcLog;
    TLog Log;

    TGrpcInitializer GrpcInitializer;

    BlockPluginHost* Host;
    TString Options;

    NClient::TClientAppConfigPtr ClientConfig;
    NProto::TPluginConfig PluginConfig;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    IMonitoringServicePtr Monitoring;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    IServerStatsPtr ClientStats;
    IStatsUpdaterPtr StatsUpdater;
    TVector<ITraceReaderPtr> TraceReaders;
    ITraceProcessorPtr TraceProcessor;

    IThrottlerPtr Throttler;
    NClient::IClientPtr Client;
    NBD::IClientPtr NbdClient;
    IPluginPtr Plugin;

public:
    TBootstrap(BlockPluginHost* host, TString options);
    ~TBootstrap();

    void Init();

    void Start();
    void Stop();

    IPlugin* GetPlugin() const
    {
        return Plugin.get();
    }

    NProto::TPluginMountConfig GetMountConfig(const char* volumeName);

    NClient::TSessionConfig FillSessionConfig(const BlockPlugin_MountOpts* opts);

private:
    void InitLWTrace();

    void InitClientConfig();
};

}   // namespace NCloud::NBlockStore::NPlugin
