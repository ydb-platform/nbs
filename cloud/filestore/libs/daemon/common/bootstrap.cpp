#include "bootstrap.h"

#include "config_initializer.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/metrics/service.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/server/probes.h>
#include <cloud/filestore/libs/server/server.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/init/actorsystem.h>
#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/daemon/mlock.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor_mon.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/kikimr/node.h>
#include <cloud/storage/core/libs/kikimr/proxy.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NActors;
using namespace NKikimr;

using namespace NLWTrace;
using namespace NMonitoring;

using namespace NServer;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString TraceLoggerId = "st_trace_logger";
const TString SlowRequestsFilterId = "st_slow_requests_filter";

} // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrapCommon::TBootstrapCommon(
        std::shared_ptr<NKikimr::TModuleFactories> moduleFactories,
        TString logComponent,
        TString metricsComponent,
        std::shared_ptr<NUserStats::IUserCounterSupplier> userCounters)
    : MetricsComponent(std::move(metricsComponent))
    , ModuleFactories(std::move(moduleFactories))
    , UserCounters(std::move(userCounters))
{
    BootstrapLogging = CreateLoggingService("console", TLogSettings{});
    BootstrapLogging->Start();

    Log = BootstrapLogging->CreateLog(logComponent);
    SetCriticalEventsLog(Log);
}

TBootstrapCommon::~TBootstrapCommon()
{}

void TBootstrapCommon::Start()
{
    FILESTORE_LOG_START_COMPONENT(Logging);
    FILESTORE_LOG_START_COMPONENT(Monitoring);
    FILESTORE_LOG_START_COMPONENT(FileIOService);
    FILESTORE_LOG_START_COMPONENT(Metrics);
    FILESTORE_LOG_START_COMPONENT(TraceProcessor);
    FILESTORE_LOG_START_COMPONENT(TraceSerializer);
    FILESTORE_LOG_START_COMPONENT(ActorSystem);
    FILESTORE_LOG_START_COMPONENT(BackgroundThreadPool);
    FILESTORE_LOG_START_COMPONENT(ProfileLog);
    FILESTORE_LOG_START_COMPONENT(RequestStatsUpdater);
    FILESTORE_LOG_START_COMPONENT(StatsFetcher);

    StartComponents();

    // we need to start scheduler after all other components for 2 reasons:
    // 1) any component can schedule a task that uses a dependency that hasn't
    // started yet
    // 2) we have loops in our dependencies, so there is no 'correct' starting
    // order
    FILESTORE_LOG_START_COMPONENT(Scheduler);
    FILESTORE_LOG_START_COMPONENT(BackgroundScheduler);

    if (Configs->Options->MemLock) {
        LockProcessMemory(Log);
        STORAGE_INFO("Process memory locked");
    }
}

void TBootstrapCommon::Stop()
{
    Drain();

    // stopping scheduler before all other components to avoid races between
    // scheduled tasks and shutting down of component dependencies
    FILESTORE_LOG_STOP_COMPONENT(BackgroundScheduler);
    FILESTORE_LOG_STOP_COMPONENT(Scheduler);

    StopComponents();

    FILESTORE_LOG_STOP_COMPONENT(StatsFetcher);
    FILESTORE_LOG_STOP_COMPONENT(RequestStatsUpdater);
    FILESTORE_LOG_STOP_COMPONENT(ProfileLog);
    FILESTORE_LOG_STOP_COMPONENT(BackgroundThreadPool);
    FILESTORE_LOG_STOP_COMPONENT(ActorSystem);
    FILESTORE_LOG_STOP_COMPONENT(TraceSerializer);
    FILESTORE_LOG_STOP_COMPONENT(TraceProcessor);
    FILESTORE_LOG_STOP_COMPONENT(Metrics);
    FILESTORE_LOG_STOP_COMPONENT(FileIOService);
    FILESTORE_LOG_STOP_COMPONENT(Monitoring);
    FILESTORE_LOG_STOP_COMPONENT(Logging);
}

TProgramShouldContinue& TBootstrapCommon::GetShouldContinue()
{
    if (ActorSystem) {
        return ActorSystem->GetProgramShouldContinue();
    }

    return ProgramShouldContinue;
}

void TBootstrapCommon::ParseOptions(int argc, char** argv)
{
    Y_ABORT_UNLESS(!Configs);
    Configs = InitConfigs(argc, argv);
}

void TBootstrapCommon::Init()
{
    InitCommonConfigs();

    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();
    BackgroundThreadPool = CreateThreadPool("Background", 1);
    BackgroundScheduler = CreateBackgroundScheduler(
        Scheduler,
        BackgroundThreadPool);

    if (Configs->Options->ProfileFile) {
        ProfileLog = CreateProfileLog(
            {
                Configs->Options->ProfileFile,
                Configs->DiagnosticsConfig->GetProfileLogTimeThreshold()
            },
            Timer,
            BackgroundScheduler);
    } else {
        ProfileLog = CreateProfileLogStub();
    }

    STORAGE_INFO("ProfileLog initialized");

    Metrics = NMetrics::CreateMetricsService(
        NMetrics::TMetricsServiceConfig{
            .UpdateInterval = Configs->DiagnosticsConfig->GetMetricsUpdateInterval()
        },
        Timer,
        BackgroundScheduler);
    STORAGE_INFO("Metrics initialized");

    if (Configs->Options->Service == EServiceKind::Kikimr) {
        InitActorSystem();
    }

    FileIOService = CreateAIOService();

    InitDiagnostics();
    InitComponents();

    STORAGE_INFO("Init completed");
}

void TBootstrapCommon::InitCommonConfigs()
{
    Configs->InitKikimrConfig();
    Configs->InitStorageConfig();
    Configs->InitDiagnosticsConfig();
    Configs->InitFeaturesConfig();
}

void TBootstrapCommon::InitDiagnostics()
{
    if (!ActorSystem) {
        TLogSettings logSettings;

        if (Configs->Options->VerboseLevel) {
            auto level = GetLogLevel(Configs->Options->VerboseLevel);
            Y_ENSURE(level, "unknown log level: " << Configs->Options->VerboseLevel.Quote());

            logSettings.FiltrationLevel = *level;
        }

        Logging = CreateLoggingService("console", logSettings);

        if (Configs->Options->MonitoringPort) {
            Monitoring = CreateMonitoringService(
                Configs->Options->MonitoringPort,
                Configs->Options->MonitoringAddress,
                Configs->Options->MonitoringThreads);
        } else {
            Monitoring = CreateMonitoringServiceStub();
        }

        Metrics->SetupCounters(Monitoring->GetCounters());
    }

    StatsRegistry = CreateRequestStatsRegistry(
        MetricsComponent,
        Configs->DiagnosticsConfig,
        FILESTORE_COUNTERS_ROOT(Monitoring->GetCounters()),
        Timer,
        UserCounters);

    STORAGE_INFO("Stats initialized");
}

void TBootstrapCommon::InitActorSystem()
{
    Y_ABORT_UNLESS(Configs->KikimrConfig);
    Y_ABORT_UNLESS(Configs->StorageConfig);

    NCloud::NStorage::TRegisterDynamicNodeOptions registerOpts;
    registerOpts.Domain = Configs->Options->Domain;
    registerOpts.SchemeShardDir = Configs->StorageConfig->GetSchemeShardDir();
    registerOpts.NodeBrokerAddress = Configs->Options->NodeBrokerAddress;
    registerOpts.NodeBrokerPort = Configs->Options->NodeBrokerPort;
    registerOpts.NodeBrokerSecurePort = Configs->Options->NodeBrokerSecurePort;
    registerOpts.InterconnectPort = Configs->Options->InterconnectPort;
    registerOpts.LoadCmsConfigs = Configs->Options->LoadCmsConfigs;
    registerOpts.UseNodeBrokerSsl = Configs->Options->UseNodeBrokerSsl
        || Configs->StorageConfig->GetNodeRegistrationUseSsl();
    registerOpts.Settings = Configs->GetNodeRegistrationSettings();

    auto registrant =
        CreateNodeRegistrant(Configs->KikimrConfig, registerOpts, Log);

    auto [nodeId, scopeId, cmsConfig] = RegisterDynamicNode(
        Configs->KikimrConfig,
        registerOpts,
        std::move(registrant),
        Log);

    if (cmsConfig) {
        Configs->ApplyCMSConfigs(std::move(*cmsConfig));
    }

    auto logging = std::make_shared<TLoggingProxy>();
    auto monitoring = std::make_shared<TMonitoringProxy>();

    TraceSerializer = CreateTraceSerializer(
        logging,
        "NFS_TRACE",
        NLwTraceMonPage::TraceManager(false));

    STORAGE_INFO("TraceSerializer initialized");

    auto cpuWaitFilename = Configs->DiagnosticsConfig->GetCpuWaitFilename();
    StatsFetcher = NCloud::NStorage::BuildStatsFetcher(
        Configs->DiagnosticsConfig->GetStatsFetcherType(),
        cpuWaitFilename.empty()
            ? NCloud::NStorage::BuildCpuWaitStatsFilename(
                  Configs->DiagnosticsConfig->GetCpuWaitServiceName())
            : std::move(cpuWaitFilename),
        Log,
        logging);

    STORAGE_INFO("StatsFetcher initialized");

    NStorage::TActorSystemArgs args;
    args.NodeId = nodeId;
    args.ScopeId = scopeId;
    args.AppConfig = Configs->KikimrConfig;
    args.StorageConfig = Configs->StorageConfig;
    args.ProfileLog = ProfileLog;
    args.TraceSerializer = TraceSerializer;
    args.DiagnosticsConfig = Configs->DiagnosticsConfig;
    args.Metrics = Metrics;
    args.UserCounters = UserCounters;
    args.StatsFetcher = StatsFetcher;
    args.ModuleFactories = ModuleFactories;

    ActorSystem = NStorage::CreateActorSystem(args);
    STORAGE_INFO("ActorSystem initialized");

    logging->Init(ActorSystem);
    Logging = logging;

    monitoring->Init(ActorSystem);
    Monitoring = monitoring;
}

void TBootstrapCommon::RegisterServer(IServerPtr server)
{
    StatsRegistry->GetRequestStats()->RegisterIncompleteRequestProvider(std::move(server));
    RequestStatsUpdater = CreateStatsUpdater(
        Timer,
        BackgroundScheduler,
        StatsRegistry);

    STORAGE_INFO("Server registered");
}

void TBootstrapCommon::InitLWTrace(
    const TVector<TProbe**>& probes,
    const TVector<std::tuple<TString, TString>>& probesToTrace)
{
    auto& probesRegistry = NLwTraceMonPage::ProbeRegistry();
    for (auto probe: probes) {
        probesRegistry.AddProbesList(probe);
    }

    auto& lwManager = NLwTraceMonPage::TraceManager(false);
    auto traceLog = CreateUnifiedAgentLoggingService(
        Logging,
        Configs->DiagnosticsConfig->GetTracesUnifiedAgentEndpoint(),
        Configs->DiagnosticsConfig->GetTracesSyslogIdentifier()
    );

    TVector<ITraceReaderPtr> traceReaders;
    if (auto samplingRate = Configs->DiagnosticsConfig->GetSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(
            probesToTrace,
            samplingRate,
            Configs->DiagnosticsConfig->GetLWTraceShuttleCount());
        lwManager.New(TraceLoggerId, query);
        traceReaders.push_back(CreateTraceLogger(TraceLoggerId, traceLog, "NFS_TRACE"));
    }

    auto slowRequestSamplingRate =
        Configs->DiagnosticsConfig->GetSlowRequestSamplingRate();
    if (slowRequestSamplingRate && !probesToTrace.empty()) {
        NLWTrace::TQuery query = ProbabilisticQuery(
            probesToTrace,
            slowRequestSamplingRate,
            Configs->DiagnosticsConfig->GetLWTraceShuttleCount());

        lwManager.New(SlowRequestsFilterId, query);
        traceReaders.push_back(CreateSlowRequestsFilter(
            SlowRequestsFilterId,
            traceLog,
            "NFS_TRACE",
            Configs->DiagnosticsConfig->GetRequestThresholds()));
    }

    if (traceReaders.size()) {
        TraceProcessor = CreateTraceProcessorMon(
            Monitoring,
            CreateTraceProcessor(
                Timer,
                BackgroundScheduler,
                Logging,
                "NFS_TRACE",
                NLwTraceMonPage::TraceManager(false),
                std::move(traceReaders)));

        STORAGE_INFO("TraceProcessor initialized");
    } else {
        TraceProcessor = CreateTraceProcessorStub();
    }

    STORAGE_INFO("LWTrace initialized");
}

} // namespace NCloud::NFileStore::NDaemon
