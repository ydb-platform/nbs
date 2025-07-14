#include "bootstrap.h"

#include "logging.h"

#include <cloud/vm/blockstore/lib/helper.h>
#include <cloud/vm/blockstore/lib/plugin.h>

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_request_processor.h>
#include <cloud/blockstore/libs/diagnostics/probes.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/common/random.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor_mon.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
#include <cloud/storage/core/libs/grpc/utils.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/lwtrace/control.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NPlugin {

using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultClientConfigPath = "/etc/yc/nbs/client/client.txt";
const TString FallbackClientConfigPath = "/etc/yc/nbs/client/client-default.txt";
const TString TraceLoggerId = "st_trace_logger";
const TString SlowRequestsFilterId = "st_slow_requests_filter";

constexpr const TDuration InactiveClientsTimeout = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

struct TLWTraceSafeManager
{
    NLWTrace::TManager Manager;

    TLWTraceSafeManager()
        : Manager(*Singleton<NLWTrace::TProbeRegistry>(), true)
    {
    }

    static NLWTrace::TManager& Instance()
    {
        return Singleton<TLWTraceSafeManager>()->Manager;
    }
};

bool IsHostVersionNewer(const BlockPluginHost* host, ui32 major, ui32 minor)
{
    return
        (host->version_major > major) ||
        (host->version_major == major && host->version_minor >= minor);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(BlockPluginHost* host, TString options)
    : Host(host)
    , Options(std::move(options))
{}

TBootstrap::~TBootstrap()
{}

void TBootstrap::Init()
{
    InitClientConfig();

    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();

    const auto& logConfig = ClientConfig->GetLogConfig();
    const auto& monConfig = ClientConfig->GetMonitoringConfig();

    TLogSettings logSettings;
    logSettings.SuppressNewLine = true;   // host will do it for us

    if (logConfig.HasLogLevel()) {
        logSettings.FiltrationLevel = static_cast<ELogPriority>(
            logConfig.GetLogLevel());
    }

    std::shared_ptr<TLogBackend> logBackend;
    if (logConfig.HasSysLogService()) {
        logBackend = CreateMultiLogBackend({
            CreateHostLogBackend(Host),
            CreateSysLogBackend(logConfig.GetSysLogService())
        });
    } else {
        logBackend = CreateHostLogBackend(Host);
    }

    Logging = CreateLoggingService(std::move(logBackend), logSettings);
    Log = Logging->CreateLog("BLOCKSTORE_PLUGIN");
    GrpcLog = Logging->CreateLog("GRPC");

    InitLWTrace();

    GrpcLoggerInit(GrpcLog, logConfig.GetEnableGrpcTracing());

    ui32 maxThreads = ClientConfig->GetGrpcThreadsLimit();
    SetExecutorThreadsLimit(maxThreads);
    SetDefaultThreadPoolLimit(maxThreads);

    ui32 monPort = monConfig.GetPort();
    if (monPort) {
        const TString& monAddress = monConfig.GetAddress();
        ui32 threadsCount = monConfig.GetThreadsCount();
        Monitoring = CreateMonitoringService(monPort, monAddress, threadsCount);
    } else {
        Monitoring = CreateMonitoringServiceStub();
    }

    if (TraceReaders.size()) {
        TraceProcessor = CreateTraceProcessorMon(
            Monitoring,
            CreateTraceProcessor(
                Timer,
                Scheduler,
                Logging,
                "BLOCKSTORE_TRACE",
                TLWTraceSafeManager::Instance(),
                TraceReaders));
    }

    auto rootGroup = Monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore");

    auto clientGroup = rootGroup->GetSubgroup("component", "client");
    auto revisionGroup = rootGroup->GetSubgroup("revision", GetFullVersionString());
    auto versionCounter = revisionGroup->GetCounter(
        "version",
        false);
    *versionCounter = 1;

    RequestStats = CreateClientRequestStats(
        clientGroup,
        Timer,
        EHistogramCounterOption::ReportMultipleCounters);

    VolumeStats = CreateVolumeStats(
        Monitoring,
        InactiveClientsTimeout,
        EVolumeStatsType::EClientStats,
        Timer);

    ClientStats = CreateClientStats(
        ClientConfig,
        Monitoring,
        RequestStats,
        VolumeStats,
        ClientConfig->GetInstanceId());

    auto [client, error] = CreateClient(
        ClientConfig,
        Timer,
        Scheduler,
        Logging,
        Monitoring,
        ClientStats);

    Y_ABORT_UNLESS(!HasError(error));
    Client = std::move(client);

    if (ClientConfig->GetIpcType() == NProto::IPC_NBD) {
        NbdClient = NBD::CreateClient(
            Logging,
            ClientConfig->GetNbdThreadsCount());
    }

    IThrottlerPolicyPtr throttlerPolicy;
    auto performanceProfile = PluginConfig.GetClientPerformanceProfile();
    if (PreparePerformanceProfile(
            THostPerformanceProfile{},
            ClientConfig->GetClientConfig(),
            PluginConfig.GetClientProfile(),
            performanceProfile))
    {
        STORAGE_INFO(
            "Throttler configured: " << performanceProfile.DebugString().Quote()
        );

        Throttler = CreateThrottler(
            CreateClientThrottlerLogger(RequestStats, Logging),
            CreateThrottlerMetrics(
                Timer,
                rootGroup,
                "client"),
            CreateClientThrottlerPolicy(std::move(performanceProfile)),
            CreateClientThrottlerTracker(),
            Timer,
            Scheduler,
            VolumeStats);
    }

    Plugin = CreatePlugin(
        Host,
        Timer,
        Scheduler,
        Logging,
        Monitoring,
        RequestStats,
        VolumeStats,
        ClientStats,
        Throttler,
        Client,
        NbdClient,
        ClientConfig);

    StatsUpdater = CreateStatsUpdater(
        Timer,
        Scheduler,
        CreateIncompleteRequestProcessor(
            ClientStats,
            {Plugin}));

    STORAGE_INFO("NBS plugin version: " << GetFullVersionString());
}

void TBootstrap::InitLWTrace()
{
    auto& probes = *Singleton<NLWTrace::TProbeRegistry>();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_SERVER_PROVIDER));

    const auto& tracingConfig =
        ClientConfig->GetClientConfig().GetTracingConfig();

    const TVector<std::tuple<TString, TString>> desc = {
        {"RequestReceived", "BLOCKSTORE_SERVER_PROVIDER"},
    };

    auto traceLog = CreateUnifiedAgentLoggingService(
        Logging,
        tracingConfig.GetTracesUnifiedAgentEndpoint(),
        tracingConfig.GetTracesSyslogIdentifier()
    );


    if (auto samplingRate = tracingConfig.GetSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(desc, samplingRate);
        TLWTraceSafeManager::Instance().New(TraceLoggerId, query);
        TraceReaders.push_back(SetupTraceReaderWithLog(
            TraceLoggerId,
            traceLog,
            "BLOCKSTORE_TRACE",
            "AllRequests"
        ));
    }

    if (auto samplingRate = tracingConfig.GetSlowRequestSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(desc, samplingRate);
        TLWTraceSafeManager::Instance().New(SlowRequestsFilterId, query);
        TraceReaders.push_back(SetupTraceReaderForSlowRequests(
            SlowRequestsFilterId,
            traceLog,
            "BLOCKSTORE_TRACE",
            ClientConfig->GetRequestThresholds()
        ));
    }
}

void TBootstrap::InitClientConfig()
{
    NProto::TClientAppConfig appConfig;
    bool clientConfigParsed = false;

    if (Options) {
        if (!TryParseProtoTextFromString(Options, PluginConfig)) {
            // Legacy branch: parsing Options as config file path
            // see CLOUD-42448
            if (!TryParseFromTextFormat(Options, PluginConfig)) {
                // Legacy branch: parsing config as TClientConfig/TClientAppConfig
                // see NBS-503
                if (TryParseFromTextFormat(Options, appConfig)) {
                    clientConfigParsed = true;
                } else if (TryParseFromTextFormat(Options, *appConfig.MutableClientConfig())) {
                    clientConfigParsed = true;
                } else {
                    ythrow yexception() << "invalid config specified";
                }
            }
        }
    }

    if (!clientConfigParsed) {
        appConfig = ParseClientAppConfig(
            PluginConfig,
            DefaultClientConfigPath,
            FallbackClientConfigPath);
    }

    if (IsHostVersionNewer(Host, 0x5, 0x4)) {
        appConfig.MutableClientConfig()->SetInstanceId(Host->instance_id);
    }

    auto& clientConfig = *appConfig.MutableClientConfig();
    if (PluginConfig.HasClientId()) {
        clientConfig.SetClientId(PluginConfig.GetClientId());
    }

    if (!clientConfig.GetClientId()) {
        clientConfig.SetClientId(SafeCreateGuidAsString());
    }

    ClientConfig = std::make_shared<TClientAppConfig>(appConfig);
}

NProto::TPluginMountConfig TBootstrap::GetMountConfig(const char* volumeName)
{
    NProto::TPluginMountConfig config;
    if (!TryParseProtoTextFromStringWithoutError(volumeName, config)) {
        STORAGE_INFO("Try parse volume_name as a file path");
        if (!TryParseProtoTextFromFile(volumeName, config)) {
            STORAGE_INFO("Legacy branch: parsing volume_name as a disk id");
            config.SetDiskId(volumeName);
        }
    }

    return config;
}

TSessionConfig TBootstrap::FillSessionConfig(const BlockPlugin_MountOpts* opts)
{
    auto mountConfig = GetMountConfig(opts->volume_name);

    TSessionConfig sessionConfig;
    sessionConfig.DiskId = mountConfig.GetDiskId();
    sessionConfig.MountToken = opts->mount_token;
    sessionConfig.InstanceId = opts->instance_id;
    sessionConfig.ClientVersionInfo = GetFullVersionString();
    sessionConfig.IpcType = ClientConfig->GetIpcType();

    if (opts->access_mode == BLOCK_PLUGIN_ACCESS_READ_WRITE) {
        sessionConfig.AccessMode = NProto::VOLUME_ACCESS_READ_WRITE;
    } else {
        sessionConfig.AccessMode = NProto::VOLUME_ACCESS_READ_ONLY;
    }

    if (opts->mount_mode == BLOCK_PLUGIN_MOUNT_LOCAL) {
        sessionConfig.MountMode = NProto::VOLUME_MOUNT_LOCAL;
    } else {
        sessionConfig.MountMode = NProto::VOLUME_MOUNT_REMOTE;
    }

    if (IsHostVersionNewer(Host, 0x5, 0x3)) {
        sessionConfig.MountSeqNumber = opts->mount_seq_number;
    }

    return sessionConfig;
}

void TBootstrap::Start()
{
    STORAGE_INFO("Starting plugin...");

    if (Logging) {
        Logging->Start();
    }

    if (Monitoring) {
        Monitoring->Start();
    }

    if (TraceProcessor) {
        TraceProcessor->Start();
    }

    if (Client) {
        Client->Start();
    }

    if (NbdClient) {
        NbdClient->Start();
    }

    if (Throttler) {
        Throttler->Start();
    }

    if (StatsUpdater) {
        StatsUpdater->Start();
    }

    if (Scheduler) {
        Scheduler->Start();
    }

    STORAGE_INFO("Started plugin.");
}

void TBootstrap::Stop()
{
    STORAGE_INFO("Stopping plugin...");

    if (Scheduler) {
        Scheduler->Stop();
    }

    if (StatsUpdater) {
        StatsUpdater->Stop();
    }

    if (Throttler) {
        Throttler->Stop();
    }

    if (NbdClient) {
        NbdClient->Stop();
    }

    if (Client) {
        Client->Stop();
    }

    if (TraceProcessor) {
        TraceProcessor->Stop();
    }

    if (Monitoring) {
        Monitoring->Stop();
    }

    if (Logging) {
        Logging->Stop();
    }

    STORAGE_INFO("Stopped plugin.");
}

}   // namespace NCloud::NBlockStore::NPlugin
