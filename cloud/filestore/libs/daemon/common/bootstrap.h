#pragma once

#include "options.h"
#include "public.h"

#include <cloud/filestore/config/server.pb.h>
#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/metrics/public.h>
#include <cloud/filestore/libs/diagnostics/user_counter.h>
#include <cloud/filestore/libs/server/public.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/kikimr/node_registration_settings.h>
#include <cloud/storage/core/libs/kikimr/public.h>

#include <ydb/library/actors/util/should_continue.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/lwtrace/probes.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>


namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_LOG_START_COMPONENT(c)                                       \
    if (c) {                                                                   \
        c->Start();                                                            \
        STORAGE_INFO("Started " << #c);                                        \
    }                                                                          \
// FILESTORE_LOG_START_COMPONENT

#define FILESTORE_LOG_STOP_COMPONENT(c)                                        \
    if (c) {                                                                   \
        c->Stop();                                                             \
        STORAGE_INFO("Stopped " << #c);                                        \
    }                                                                          \
// FILESTORE_LOG_STOP_COMPONENT

////////////////////////////////////////////////////////////////////////////////

using IUserCounterSupplier = NCloud::NStorage::NUserStats::IUserCounterSupplier;

class TBootstrapCommon
{
private:
    const TString MetricsComponent;
    const TString LogComponent;

    std::shared_ptr<NKikimr::TModuleFactories> ModuleFactories;

    TConfigInitializerCommonPtr Configs;
    ILoggingServicePtr BootstrapLogging;
    TProgramShouldContinue ProgramShouldContinue;
    std::shared_ptr<IUserCounterSupplier> UserCounters;

protected:
    TLog Log;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ISchedulerPtr BackgroundScheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    IFileIOServicePtr FileIOService;
    NMetrics::IMetricsServicePtr Metrics;
    IRequestStatsRegistryPtr StatsRegistry;
    IStatsUpdaterPtr RequestStatsUpdater;
    ITraceSerializerPtr TraceSerializer;
    ITraceProcessorPtr TraceProcessor;
    ITaskQueuePtr BackgroundThreadPool;
    IProfileLogPtr ProfileLog;
    IActorSystemPtr ActorSystem;
    NCloud::NStorage::IStatsFetcherPtr StatsFetcher;

public:
    TBootstrapCommon(
        std::shared_ptr<NKikimr::TModuleFactories> moduleFactories,
        TString logComponent,
        TString metricsComponent,
        std::shared_ptr<IUserCounterSupplier> userCounters);
    virtual ~TBootstrapCommon();

    void ParseOptions(int argc, char** argv);

    void Init();
    void Start();
    void Stop();

    TProgramShouldContinue& GetShouldContinue();

protected:
    virtual TConfigInitializerCommonPtr InitConfigs(int argc, char** argv) = 0;

    virtual void InitComponents() = 0;
    virtual void StartComponents() = 0;
    virtual void Drain() = 0;
    virtual void StopComponents() = 0;

    void RegisterServer(NServer::IServerPtr server);

    void InitLWTrace(
        const TVector<NLWTrace::TProbe**>& probes,
        const TVector<std::tuple<TString, TString>>& probesToTrace);

private:
    void InitCommonConfigs();
    void InitActorSystem();
    void InitDiagnostics();
    void InitLogs();
};

} // namespace NCloud::NFileStore::NDaemon
