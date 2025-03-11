#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/fault_injection.h>
#include <cloud/blockstore/libs/diagnostics/probes.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/rdma/iface/config.h>
#include <cloud/blockstore/libs/rdma/iface/probes.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>
#include <cloud/blockstore/libs/service_local/storage_aio.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_generator.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_scanner.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/probes.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/blockstore/libs/storage/init/disk_agent/actorsystem.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/daemon/mlock.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor_mon.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/kikimr/node.h>
#include <cloud/storage/core/libs/version/version.h>

#include <contrib/ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/tablet_flat/probes.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/lwtrace/probes.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/svnversion/svnversion.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;
using namespace NNvme;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString TraceLoggerId = "st_trace_logger";
const TString SlowRequestsFilterId = "st_slow_requests_filter";

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ParseProtoTextFromString(const TString& text, T& dst)
{
    TStringInput in(text);
    ParseFromTextFormat(in, dst);
}

template <typename T>
void ParseProtoTextFromFile(const TString& fileName, T& dst)
{
    TFileInput in(fileName);
    ParseFromTextFormat(in, dst);
}

bool AgentHasDevices(
    TLog log,
    const NStorage::TStorageConfigPtr& storageConfig,
    const NStorage::TDiskAgentConfigPtr& agentConfig)
{
    if (!agentConfig->GetFileDevices().empty()) {
        return true;
    }

    const TString storagePath = storageConfig->GetCachedDiskAgentConfigPath();
    const TString diskAgentPath = agentConfig->GetCachedConfigPath();
    const TString& path = diskAgentPath.empty() ? storagePath : diskAgentPath;

    if (auto [config, _] = NStorage::LoadDiskAgentConfig(path);
        config.FileDevicesSize() != 0)
    {
        return true;
    }

    NStorage::TDeviceGenerator gen{std::move(log), agentConfig->GetAgentId()};
    auto error =
        FindDevices(agentConfig->GetStorageDiscoveryConfig(), std::ref(gen));
    if (HasError(error)) {
        return false;
    }

    return !gen.ExtractResult().empty();
}

////////////////////////////////////////////////////////////////////////////////

class TLoggingProxy final
    : public ILoggingService
{
private:
    IActorSystemPtr ActorSystem;

public:
    void Init(IActorSystemPtr actorSystem)
    {
        ActorSystem = std::move(actorSystem);
    }

    void Start() override
    {
        Y_ABORT_UNLESS(ActorSystem);
    }

    void Stop() override
    {
        ActorSystem.Reset();
    }

    TLog CreateLog(const TString& component) override
    {
        return ActorSystem->CreateLog(component);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMonitoringProxy final
    : public IMonitoringService
{
private:
    IActorSystemPtr ActorSystem;

public:
    void Init(IActorSystemPtr actorSystem)
    {
        ActorSystem = std::move(actorSystem);
    }

    void Start() override
    {
        Y_ABORT_UNLESS(ActorSystem);
    }

    void Stop() override
    {
        ActorSystem.Reset();
    }

    IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override
    {
        return ActorSystem->RegisterIndexPage(path, title);
    }

    void RegisterMonPage(IMonPagePtr page) override
    {
        ActorSystem->RegisterMonPage(page);
    }

    IMonPagePtr GetMonPage(const TString& path) override
    {
        return ActorSystem->GetMonPage(path);
    }

    TDynamicCountersPtr GetCounters() override
    {
        return ActorSystem->GetCounters();
    }
};

NRdma::TServerConfigPtr CreateRdmaServerConfig(NRdma::TRdmaConfig& config)
{
    return std::make_shared<NRdma::TServerConfig>(config.GetServer());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
        std::shared_ptr<NKikimr::TModuleFactories> moduleFactories,
        std::shared_ptr<TServerModuleFactories> serverModuleFactories)
    : ModuleFactories(std::move(moduleFactories))
    , ServerModuleFactories(std::move(serverModuleFactories))
{}

TBootstrap::~TBootstrap()
{}

void TBootstrap::ParseOptions(int argc, char** argv)
{
    auto options = std::make_shared<TOptions>();
    options->Parse(argc, argv);
    Configs = std::make_unique<TConfigInitializer>(std::move(options));
}

void TBootstrap::InitHTTPServer()
{
    Y_DEBUG_ABORT_UNLESS(!Initialized);

    TStringBuilder stubMonPageBuilder;

    stubMonPageBuilder
        << R"(<html><head></head><body>)"
           R"(<h3>This node is not registered in the NodeBroker.)"
           R"(See "DisableNodeBrokerRegistrationOnDevicelessAgent")"
           R"( in the disk agent config.</h3><br>)"
           R"(<div class="container"><h2>Version</h2><pre>)";

    stubMonPageBuilder << GetProgramSvnVersion();
    if (!stubMonPageBuilder.EndsWith("\n")) {
        stubMonPageBuilder << "\n";
    }

    stubMonPageBuilder << R"(</pre></div></body></html>)";

    StubMonPageServer = std::make_unique<NCloud::NStorage::TSimpleHttpServer>(
        Configs->Options->MonitoringAddress,
        Configs->DiagnosticsConfig->GetNbsMonPort(),
        std::move(stubMonPageBuilder));
}

void TBootstrap::Init()
{
    BootstrapLogging = CreateLoggingService("console", TLogSettings{});
    Log = BootstrapLogging->CreateLog("BLOCKSTORE_SERVER");
    STORAGE_INFO("NBS server version: " << GetFullVersionString());

    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();

    if (!InitKikimrService()) {
        InitHTTPServer();
        return;
    }

    Initialized = true;
    STORAGE_INFO("Kikimr service initialized");

    auto diagnosticsConfig = Configs->DiagnosticsConfig;
    if (TraceReaders.size()) {
        TraceProcessor = CreateTraceProcessorMon(
            Monitoring,
            CreateTraceProcessor(
                Timer,
                Scheduler,
                Logging,
                "BLOCKSTORE_TRACE",
                NLwTraceMonPage::TraceManager(diagnosticsConfig->GetUnsafeLWTrace()),
                TraceReaders));

        STORAGE_INFO("TraceProcessor initialized");
    }

    auto rootGroup = Monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore");

    auto serverGroup = rootGroup->GetSubgroup("component", "server");
    auto revisionGroup = serverGroup->GetSubgroup("revision", GetFullVersionString());

    auto versionCounter = revisionGroup->GetCounter(
        "version",
        false);
    *versionCounter = 1;

    InitCriticalEventsCounter(serverGroup);

    for (auto& event: PostponedCriticalEvents) {
        ReportCriticalEvent(
            event,
            "",     // message
            false); // verifyDebug
    }
    PostponedCriticalEvents.clear();
}

void TBootstrap::InitProfileLog()
{
    if (Configs->Options->ProfileFile) {
        ProfileLog = CreateProfileLog(
            {
                Configs->Options->ProfileFile,
                Configs->DiagnosticsConfig->GetProfileLogTimeThreshold(),
            },
            Timer,
            Scheduler
        );
    } else {
        ProfileLog = CreateProfileLogStub();
    }
}

void TBootstrap::InitRdmaServer(NRdma::TRdmaConfig& config)
{
    try {
        if (config.GetServerEnabled()) {
            RdmaServer = ServerModuleFactories->RdmaServerFactory(
                Logging,
                Monitoring,
                CreateRdmaServerConfig(config));

            STORAGE_INFO("RDMA server initialized");
        }
    } catch (...) {
        STORAGE_ERROR("Failed to initialize RDMA server: "
            << CurrentExceptionMessage().c_str());

        RdmaServer = nullptr;
        PostponedCriticalEvents.push_back("AppCriticalEvents/RdmaError");
    }
}

bool TBootstrap::InitKikimrService()
{
    Configs->Log = Log;
    Configs->InitKikimrConfig();
    Configs->InitServerConfig();
    Configs->InitFeaturesConfig();
    Configs->InitStorageConfig();
    Configs->InitDiskRegistryProxyConfig();
    Configs->InitDiagnosticsConfig();
    Configs->InitSpdkEnvConfig();

    const auto& cert = Configs->StorageConfig->GetNodeRegistrationCert();

    NCloud::NStorage::TNodeRegistrationSettings settings {
        .MaxAttempts =
            Configs->StorageConfig->GetNodeRegistrationMaxAttempts(),
        .ErrorTimeout = Configs->StorageConfig->GetNodeRegistrationErrorTimeout(),
        .RegistrationTimeout = Configs->StorageConfig->GetNodeRegistrationTimeout(),
        .PathToGrpcCaFile = Configs->StorageConfig->GetNodeRegistrationRootCertsFile(),
        .PathToGrpcCertFile = cert.CertFile,
        .PathToGrpcPrivateKeyFile = cert.CertPrivateKeyFile,
        .NodeRegistrationToken = Configs->StorageConfig->GetNodeRegistrationToken(),
        .NodeType = Configs->StorageConfig->GetNodeType(),
    };

    NCloud::NStorage::TRegisterDynamicNodeOptions registerOpts {
        .Domain = Configs->Options->Domain,
        .SchemeShardDir = Configs->StorageConfig->GetSchemeShardDir(),
        .NodeBrokerAddress = Configs->Options->NodeBrokerAddress,
        .NodeBrokerPort = Configs->Options->NodeBrokerPort,
        .NodeBrokerSslPort = Configs->Options->NodeBrokerSslPort,
        .UseNodeBrokerSsl = Configs->Options->UseNodeBrokerSsl
            || Configs->StorageConfig->GetNodeRegistrationUseSsl(),
        .InterconnectPort = Configs->Options->InterconnectPort,
        .LoadCmsConfigs = Configs->Options->LoadCmsConfigs,
        .Settings = std::move(settings)
    };

    if (Configs->Options->LocationFile) {
        NProto::TLocation location;
        ParseProtoTextFromFile(Configs->Options->LocationFile, location);

        registerOpts.DataCenter = location.GetDataCenter();
        Configs->Rack = location.GetRack();
    }

    Configs->InitDiskAgentConfig();

    STORAGE_INFO("Configs initialized");

    if (const auto& agentConfig = Configs->DiskAgentConfig;
        agentConfig->GetDisableNodeBrokerRegistrationOnDevicelessAgent())
    {
        if (!agentConfig->GetEnabled()) {
            STORAGE_INFO(
                "Agent is disabled. Skipping the node broker registration.");
            return false;
        }

        if (!AgentHasDevices(Log, Configs->StorageConfig, agentConfig)) {
            STORAGE_INFO(
                "Devices were not found. Skipping the node broker "
                "registration.");
            return false;
        }
    }

    auto [nodeId, scopeId, cmsConfig] = RegisterDynamicNode(
        Configs->KikimrConfig,
        registerOpts,
        Log);

    if (cmsConfig) {
        Configs->ApplyCMSConfigs(std::move(*cmsConfig));
    }

    STORAGE_INFO("CMS configs initialized");

    // InitRdmaConfig should be called after InitDiskAgentConfig
    // and ApplyCMSConfigs to backport legacy RDMA config
    Configs->InitRdmaConfig();

    STORAGE_INFO("RDMA config initialized");

    auto logging = std::make_shared<TLoggingProxy>();
    auto monitoring = std::make_shared<TMonitoringProxy>();

    Logging = logging;
    Monitoring = monitoring;

    if (Configs->DiagnosticsConfig->GetUseAsyncLogger()) {
        AsyncLogger = CreateAsyncLogger();

        STORAGE_INFO("AsyncLogger initialized");
    }

    if (auto& config = *Configs->DiskAgentConfig; config.GetEnabled()) {
        switch (config.GetBackend()) {
            case NProto::DISK_AGENT_BACKEND_SPDK: {
                auto spdkParts =
                    ServerModuleFactories->SpdkFactory(Configs->SpdkEnvConfig);
                Spdk = std::move(spdkParts.Env);
                SpdkLogInitializer = std::move(spdkParts.LogInitializer);

                STORAGE_INFO("Spdk backend initialized");
                break;
            }

            case NProto::DISK_AGENT_BACKEND_AIO: {
                NvmeManager = CreateNvmeManager(config.GetSecureEraseTimeout());

                auto factory = [events = config.GetMaxAIOContextEvents()] {
                    return CreateAIOService(events);
                };

                FileIOServiceProvider =
                    config.GetPathsPerFileIOService()
                        ? CreateFileIOServiceProvider(
                              config.GetPathsPerFileIOService(),
                              factory)
                        : CreateSingleFileIOServiceProvider(factory());

                AioStorageProvider = CreateAioStorageProvider(
                    FileIOServiceProvider,
                    NvmeManager,
                    !config.GetDirectIoFlagDisabled(),
                    EAioSubmitQueueOpt::Use
                );

                STORAGE_INFO("Aio backend initialized");
                break;
            }
            case NProto::DISK_AGENT_BACKEND_NULL:
                NvmeManager = CreateNvmeManager(config.GetSecureEraseTimeout());
                AioStorageProvider = CreateNullStorageProvider();
                STORAGE_INFO("Null backend initialized");
                break;
        }

        if (Configs->RdmaConfig) {
            InitRdmaServer(*Configs->RdmaConfig);
        }
    }

    Allocator = CreateCachingAllocator(
        Spdk ? Spdk->GetAllocator() : TDefaultAllocator::Instance(),
        Configs->DiskAgentConfig->GetPageSize(),
        Configs->DiskAgentConfig->GetMaxPageCount(),
        Configs->DiskAgentConfig->GetPageDropSize());

    STORAGE_INFO("Allocator initialized");

    InitProfileLog();

    STORAGE_INFO("ProfileLog initialized");

    if (Configs->StorageConfig->GetBlockDigestsEnabled()) {
        BlockDigestGenerator = CreateExt4BlockDigestGenerator(
            Configs->StorageConfig->GetDigestedBlocksPercentage());
    } else {
        BlockDigestGenerator = CreateBlockDigestGeneratorStub();
    }

    STORAGE_INFO("DigestGenerator initialized");

    NStorage::TDiskAgentActorSystemArgs args;
    args.ModuleFactories = ModuleFactories;
    args.NodeId = nodeId;
    args.ScopeId = scopeId;
    args.AppConfig = Configs->KikimrConfig;
    args.StorageConfig = Configs->StorageConfig;
    args.DiskAgentConfig = Configs->DiskAgentConfig;
    args.RdmaConfig = Configs->RdmaConfig;
    args.DiskRegistryProxyConfig = Configs->DiskRegistryProxyConfig;
    args.AsyncLogger = AsyncLogger;
    args.Spdk = Spdk;
    args.Allocator = Allocator;
    args.AioStorageProvider = AioStorageProvider;
    args.ProfileLog = ProfileLog;
    args.BlockDigestGenerator = BlockDigestGenerator;
    args.RdmaServer = RdmaServer;
    args.Logging = logging;
    args.NvmeManager = NvmeManager;

    ActorSystem = NStorage::CreateDiskAgentActorSystem(args);

    STORAGE_INFO("ActorSystem initialized");

    logging->Init(ActorSystem);
    monitoring->Init(ActorSystem);

    InitLWTrace();

    STORAGE_INFO("LWTrace initialized");

    auto spdkLog = Logging->CreateLog("BLOCKSTORE_SPDK");
    if (SpdkLogInitializer) {
        SpdkLogInitializer(spdkLog);
    }

    return true;
}

void TBootstrap::InitLWTrace()
{
    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_SERVER_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_STORAGE_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(LWTRACE_INTERNAL_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOBSTORAGE_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(TABLET_FLAT_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_RDMA_PROVIDER));

    if (Configs->DiskAgentConfig->GetEnabled()) {
        probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_DISK_AGENT_PROVIDER));
    }

    auto diagnosticsConfig = Configs->DiagnosticsConfig;
    auto& lwManager = NLwTraceMonPage::TraceManager(diagnosticsConfig->GetUnsafeLWTrace());

    const TVector<std::tuple<TString, TString>> desc = {
        {"RequestStarted",                  "BLOCKSTORE_SERVER_PROVIDER"},
        {"BackgroundTaskStarted_Partition", "BLOCKSTORE_STORAGE_PROVIDER"},
        {"RequestReceived_DiskAgent",       "BLOCKSTORE_STORAGE_PROVIDER"},
    };

    auto traceLog = CreateUnifiedAgentLoggingService(
        Logging,
        diagnosticsConfig->GetTracesUnifiedAgentEndpoint(),
        diagnosticsConfig->GetTracesSyslogIdentifier()
    );

    if (auto samplingRate = diagnosticsConfig->GetSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(
            desc,
            samplingRate,
            diagnosticsConfig->GetLWTraceShuttleCount());
        lwManager.New(TraceLoggerId, query);
        TraceReaders.push_back(CreateTraceLogger(
            TraceLoggerId,
            traceLog,
            "BLOCKSTORE_TRACE"
        ));
    }

    if (auto samplingRate = diagnosticsConfig->GetSlowRequestSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(
            desc,
            samplingRate,
            diagnosticsConfig->GetLWTraceShuttleCount());
        lwManager.New(SlowRequestsFilterId, query);
        TraceReaders.push_back(CreateSlowRequestsFilter(
            SlowRequestsFilterId,
            traceLog,
            "BLOCKSTORE_TRACE",
            diagnosticsConfig->GetRequestThresholds()));
    }

    lwManager.RegisterCustomAction(
        "ServiceErrorAction", &CreateServiceErrorActionExecutor);

    if (diagnosticsConfig->GetLWTraceDebugInitializationQuery()) {
        NLWTrace::TQuery query;
        ParseProtoTextFromFile(
            diagnosticsConfig->GetLWTraceDebugInitializationQuery(),
            query);

        lwManager.New("diagnostics", query);
    }
}

void TBootstrap::Start()
{
    if (!Initialized) {
        if (StubMonPageServer) {
            StubMonPageServer->Start();
        }
        return;
    }
#define START_COMPONENT(c)                                                     \
    if (c) {                                                                   \
        STORAGE_INFO("Starting " << #c << " ...");                             \
        c->Start();                                                            \
        STORAGE_INFO("Started " << #c);                                        \
    }                                                                          \
// START_COMPONENT

    START_COMPONENT(AsyncLogger);
    START_COMPONENT(Logging);
    START_COMPONENT(Monitoring);
    START_COMPONENT(ProfileLog);
    START_COMPONENT(TraceProcessor);
    START_COMPONENT(Spdk);
    START_COMPONENT(RdmaServer);
    START_COMPONENT(FileIOServiceProvider);
    START_COMPONENT(ActorSystem);

    // we need to start scheduler after all other components for 2 reasons:
    // 1) any component can schedule a task that uses a dependency that hasn't
    // started yet
    // 2) we have loops in our dependencies, so there is no 'correct' starting
    // order
    START_COMPONENT(Scheduler);

    if (Configs->Options->MemLock) {
        LockProcessMemory(Log);
        STORAGE_INFO("Process memory locked");
    }

#undef START_COMPONENT
}

void TBootstrap::Stop()
{
    if (!Initialized) {
        if (StubMonPageServer) {
            StubMonPageServer->Stop();
        }
        return;
    }
#define STOP_COMPONENT(c)                                                      \
    if (c) {                                                                   \
        STORAGE_INFO("Stopping " << #c << " ...");                             \
        c->Stop();                                                             \
        STORAGE_INFO("Stopped " << #c);                                        \
    }                                                                          \
// STOP_COMPONENT

    // stopping scheduler before all other components to avoid races between
    // scheduled tasks and shutting down of component dependencies
    STOP_COMPONENT(Scheduler);

    STOP_COMPONENT(ActorSystem);
    // stop FileIOServiceProvider after ActorSystem to ensure that there are no
    // in-flight I/O requests from TDiskAgentActor
    STOP_COMPONENT(FileIOServiceProvider);

    STOP_COMPONENT(Spdk);
    STOP_COMPONENT(RdmaServer);
    STOP_COMPONENT(TraceProcessor);
    STOP_COMPONENT(ProfileLog);
    STOP_COMPONENT(Monitoring);
    STOP_COMPONENT(Logging);
    STOP_COMPONENT(AsyncLogger);

#undef STOP_COMPONENT
}

TProgramShouldContinue& TBootstrap::GetShouldContinue()
{
    return ActorSystem
        ? ActorSystem->GetProgramShouldContinue()
        : ShouldContinue;
}

}   // namespace NCloud::NBlockStore::NServer
