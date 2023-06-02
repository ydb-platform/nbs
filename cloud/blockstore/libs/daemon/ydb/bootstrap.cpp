#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/cgroup_stats_fetcher.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/discovery/balancing.h>
#include <cloud/blockstore/libs/discovery/ban.h>
#include <cloud/blockstore/libs/discovery/discovery.h>
#include <cloud/blockstore/libs/discovery/fetch.h>
#include <cloud/blockstore/libs/discovery/healthcheck.h>
#include <cloud/blockstore/libs/discovery/ping.h>
#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/notify/config.h>
#include <cloud/blockstore/libs/notify/notify.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/rdma/probes.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/service_auth.h>
#include <cloud/blockstore/libs/service_kikimr/auth_provider_kikimr.h>
#include <cloud/blockstore/libs/service_kikimr/service_kikimr.h>
#include <cloud/blockstore/libs/service_local/storage_aio.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/spdk/impl/env.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/manually_preempted_volumes.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/init/server/actorsystem.h>
#include <cloud/blockstore/libs/ydbstats/ydbscheme.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>
#include <cloud/blockstore/libs/ydbstats/ydbstorage.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/config.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/kikimr/node.h>
#include <cloud/storage/core/libs/kikimr/proxy.h>

#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/tablet_flat/probes.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/mon_page.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NServer {

using namespace NMonitoring;
using namespace NNvme;

using namespace NCloud::NBlockStore::NDiscovery;

using namespace NCloud::NIamClient;

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

TBootstrapYdb::TBootstrapYdb(
        std::shared_ptr<NKikimr::TModuleFactories> moduleFactories,
        std::shared_ptr<TServerModuleFactories> serverModuleFactories,
        IDeviceHandlerFactoryPtr deviceHandlerFactory)
    : TBootstrapBase(std::move(deviceHandlerFactory))
    , ModuleFactories(std::move(moduleFactories))
    , ServerModuleFactories(std::move(serverModuleFactories))
{}

TBootstrapYdb::~TBootstrapYdb()
{}

TConfigInitializerCommonPtr TBootstrapYdb::InitConfigs(int argc, char** argv)
{
    auto options = std::make_shared<TOptionsYdb>();
    options->Parse(argc, argv);
    Configs = std::make_shared<TConfigInitializerYdb>(std::move(options));
    return Configs;
}

TProgramShouldContinue& TBootstrapYdb::GetShouldContinue()
{
    return ActorSystem
        ? ActorSystem->GetProgramShouldContinue()
        : ShouldContinue;
}

IStartable* TBootstrapYdb::GetActorSystem()        { return ActorSystem.Get(); }
IStartable* TBootstrapYdb::GetAsyncLogger()        { return AsyncLogger.get(); }
IStartable* TBootstrapYdb::GetStatsAggregator()    { return StatsAggregator.get(); }
IStartable* TBootstrapYdb::GetClientPercentiles()  { return ClientPercentiles.get(); }
IStartable* TBootstrapYdb::GetStatsUploader()      { return StatsUploader.get(); }
IStartable* TBootstrapYdb::GetYdbStorage()         { return YdbStorage.get(); }
IStartable* TBootstrapYdb::GetTraceSerializer()    { return TraceSerializer.get(); }
IStartable* TBootstrapYdb::GetLogbrokerService()   { return LogbrokerService.get(); }
IStartable* TBootstrapYdb::GetNotifyService()      { return NotifyService.get(); }
IStartable* TBootstrapYdb::GetCgroupStatsFetcher() { return CgroupStatsFetcher.get(); }
IStartable* TBootstrapYdb::GetIamTokenClient()     { return IamTokenClient.get(); }

void TBootstrapYdb::InitConfigs()
{
    Configs->InitKikimrConfig();
    Configs->InitServerConfig();
    Configs->InitEndpointConfig();
    Configs->InitHostPerformanceProfile();
    Configs->InitFeaturesConfig();
    Configs->InitStorageConfig();
    Configs->InitDiskRegistryProxyConfig();
    Configs->InitDiagnosticsConfig();
    Configs->InitStatsUploadConfig();
    Configs->InitDiscoveryConfig();
    Configs->InitSpdkEnvConfig();
    Configs->InitLogbrokerConfig();
    Configs->InitNotifyConfig();
    Configs->InitIamClientConfig();
}

void TBootstrapYdb::InitKikimrService()
{
    InitConfigs();

    auto preemptedVolumes = NStorage::CreateManuallyPreemptedVolumes(
        Configs->StorageConfig,
        Log,
        PostponedCriticalEvents);

    NCloud::NStorage::TRegisterDynamicNodeOptions registerOpts;
    registerOpts.Domain = Configs->Options->Domain;
    registerOpts.SchemeShardDir = Configs->StorageConfig->GetSchemeShardDir();
    registerOpts.NodeType = Configs->ServerConfig->GetNodeType();
    registerOpts.NodeBrokerAddress = Configs->Options->NodeBrokerAddress;
    registerOpts.NodeBrokerPort = Configs->Options->NodeBrokerPort;
    registerOpts.InterconnectPort = Configs->Options->InterconnectPort;
    registerOpts.LoadCmsConfigs = Configs->ServerConfig->GetLoadCmsConfigs();
    registerOpts.MaxAttempts =
        Configs->ServerConfig->GetNodeRegistrationMaxAttempts();
    registerOpts.RegistrationTimeout =
        Configs->ServerConfig->GetNodeRegistrationTimeout();
    registerOpts.ErrorTimeout =
        Configs->ServerConfig->GetNodeRegistrationErrorTimeout();

    if (Configs->Options->LocationFile) {
        NProto::TLocation location;
        ParseProtoTextFromFile(Configs->Options->LocationFile, location);

        registerOpts.DataCenter = location.GetDataCenter();
        Configs->Rack = location.GetRack();
    }

    Configs->InitDiskAgentConfig();

    STORAGE_INFO("Configs initialized");

    auto [nodeId, scopeId, cmsConfig] = RegisterDynamicNode(
        Configs->KikimrConfig,
        registerOpts,
        Log);

    if (cmsConfig) {
        Configs->ApplyCMSConfigs(std::move(*cmsConfig));
    }

    STORAGE_INFO("CMS configs initialized");

    auto logging = std::make_shared<TLoggingProxy>();
    auto monitoring = std::make_shared<TMonitoringProxy>();

    Logging = logging;
    Monitoring = monitoring;

    InitRdmaClient();

    VolumeStats = CreateVolumeStats(
        monitoring,
        Configs->DiagnosticsConfig,
        Configs->StorageConfig->GetInactiveClientsTimeout(),
        EVolumeStatsType::EServerStats,
        Timer);

    ClientPercentiles = CreateClientPercentileCalculator(logging);

    STORAGE_INFO("ClientPercentiles initialized");

    StatsAggregator = CreateStatsAggregator(
        Timer,
        BackgroundScheduler,
        logging,
        monitoring,
        [=] (
            NMonitoring::TDynamicCountersPtr updatedCounters,
            NMonitoring::TDynamicCountersPtr baseCounters)
        {
            ClientPercentiles->CalculatePercentiles(updatedCounters);
            UpdateClientStats(updatedCounters, baseCounters);
        });

    STORAGE_INFO("StatsAggregator initialized");

    IamTokenClient = ServerModuleFactories->IamClientFactory(
        Configs->IamClientConfig,
        logging,
        Scheduler,
        Timer);

    auto statsConfig = Configs->StatsConfig;
    if (statsConfig->IsValid()) {
        YdbStorage = NYdbStats::CreateYdbStorage(
            statsConfig,
            logging,
            IamTokenClient);
        StatsUploader = NYdbStats::CreateYdbVolumesStatsUploader(
            statsConfig,
            logging,
            YdbStorage,
            NYdbStats::CreateStatsTableScheme(),
            NYdbStats::CreateHistoryTableScheme(),
            NYdbStats::CreateArchiveStatsTableScheme(),
            NYdbStats::CreateBlobLoadMetricsTableScheme());
    } else {
        StatsUploader = NYdbStats::CreateVolumesStatsUploaderStub();
    }

    STORAGE_INFO("StatsUploader initialized");

    auto discoveryConfig = Configs->DiscoveryConfig;
    if (discoveryConfig->GetConductorGroups()
            || discoveryConfig->GetInstanceListFile())
    {
        auto banList = discoveryConfig->GetBannedInstanceListFile()
            ? CreateBanList(discoveryConfig, logging, monitoring)
            : CreateBanListStub();
        auto staticFetcher = discoveryConfig->GetInstanceListFile()
            ? CreateStaticInstanceFetcher(
                discoveryConfig,
                logging,
                monitoring
            )
            : CreateInstanceFetcherStub();
        auto conductorFetcher = discoveryConfig->GetConductorGroups()
            ? CreateConductorInstanceFetcher(
                discoveryConfig,
                logging,
                monitoring,
                Timer,
                Scheduler
            )
            : CreateInstanceFetcherStub();

        auto healthChecker = CreateHealthChecker(
                discoveryConfig,
                logging,
                monitoring,
                CreateInsecurePingClient(),
                CreateSecurePingClient(Configs->ServerConfig->GetRootCertsFile())
        );

        auto balancingPolicy = CreateBalancingPolicy();

        DiscoveryService = CreateDiscoveryService(
            discoveryConfig,
            Timer,
            Scheduler,
            logging,
            monitoring,
            std::move(banList),
            std::move(staticFetcher),
            std::move(conductorFetcher),
            std::move(healthChecker),
            std::move(balancingPolicy)
        );
    } else {
        DiscoveryService = CreateDiscoveryServiceStub(
            FQDNHostName(),
            discoveryConfig->GetConductorInstancePort(),
            discoveryConfig->GetConductorSecureInstancePort()
        );
    }

    STORAGE_INFO("DiscoveryService initialized");

    TraceSerializer = CreateTraceSerializer(
        logging,
        "BLOCKSTORE_TRACE",
        NLwTraceMonPage::TraceManager(false));

    STORAGE_INFO("TraceSerializer initialized");

    if (Configs->DiagnosticsConfig->GetUseAsyncLogger()) {
        AsyncLogger = CreateAsyncLogger();

        STORAGE_INFO("AsyncLogger initialized");
    }

    InitSpdk();

    FileIOService = CreateAIOService();

    if (Configs->DiskAgentConfig->GetEnabled() &&
        Configs->DiskAgentConfig->GetBackend() == NProto::DISK_AGENT_BACKEND_AIO &&
        !AioStorageProvider)
    {
        Y_VERIFY(FileIOService);

        NvmeManager = CreateNvmeManager(
            Configs->DiskAgentConfig->GetSecureEraseTimeout());

        AioStorageProvider = CreateAioStorageProvider(
            FileIOService,
            NvmeManager,
            !Configs->DiskAgentConfig->GetDirectIoFlagDisabled());

        STORAGE_INFO("AioStorageProvider initialized");
    }

    if (Configs->DiskAgentConfig->GetEnabled() &&
        Configs->DiskAgentConfig->GetBackend() == NProto::DISK_AGENT_BACKEND_NULL &&
        !AioStorageProvider)
    {
        AioStorageProvider = CreateNullStorageProvider();

        STORAGE_INFO("AioStorageProvider (null) initialized");
    }

    Y_VERIFY(FileIOService);

    Allocator = CreateCachingAllocator(
        Spdk ? Spdk->GetAllocator() : TDefaultAllocator::Instance(),
        Configs->DiskAgentConfig->GetPageSize(),
        Configs->DiskAgentConfig->GetMaxPageCount(),
        Configs->DiskAgentConfig->GetPageDropSize());

    STORAGE_INFO("Allocator initialized");

    InitProfileLog();

    STORAGE_INFO("ProfileLog initialized");

    CgroupStatsFetcher = CreateCgroupStatsFetcher(
        logging,
        monitoring,
        Configs->DiagnosticsConfig->GetCpuWaitFilename());

    if (Configs->StorageConfig->GetBlockDigestsEnabled()) {
        if (Configs->StorageConfig->GetUseTestBlockDigestGenerator()) {
            BlockDigestGenerator = CreateTestBlockDigestGenerator();
        } else {
            BlockDigestGenerator = CreateExt4BlockDigestGenerator(
                Configs->StorageConfig->GetDigestedBlocksPercentage());
        }
    } else {
        BlockDigestGenerator = CreateBlockDigestGeneratorStub();
    }

    STORAGE_INFO("DigestGenerator initialized");

    LogbrokerService = ServerModuleFactories->LogbrokerServiceFactory(
        Configs->LogbrokerConfig,
        logging);

    STORAGE_INFO("LogbrokerService initialized");

    NotifyService = Configs->NotifyConfig->GetEndpoint()
        ? NNotify::CreateService(Configs->NotifyConfig)
        : NNotify::CreateNullService(logging);

    STORAGE_INFO("NotifyService initialized");

    NStorage::TServerActorSystemArgs args;
    args.ModuleFactories = ModuleFactories;
    args.NodeId = nodeId;
    args.ScopeId = scopeId;
    args.AppConfig = Configs->KikimrConfig;
    args.DiagnosticsConfig = Configs->DiagnosticsConfig;
    args.StorageConfig = Configs->StorageConfig;
    args.DiskAgentConfig = Configs->DiskAgentConfig;
    args.DiskRegistryProxyConfig = Configs->DiskRegistryProxyConfig;
    args.AsyncLogger = AsyncLogger;
    args.StatsAggregator = StatsAggregator;
    args.StatsUploader = StatsUploader;
    args.DiscoveryService = DiscoveryService;
    args.Spdk = Spdk;
    args.Allocator = Allocator;
    args.FileIOService = FileIOService;
    args.AioStorageProvider = AioStorageProvider;
    args.ProfileLog = ProfileLog;
    args.BlockDigestGenerator = BlockDigestGenerator;
    args.TraceSerializer = TraceSerializer;
    args.LogbrokerService = LogbrokerService;
    args.NotifyService = NotifyService;
    args.VolumeStats = VolumeStats;
    args.CgroupStatsFetcher = CgroupStatsFetcher;
    args.RdmaServer = nullptr;
    args.RdmaClient = RdmaClient;
    args.Logging = logging;
    args.PreemptedVolumes = std::move(preemptedVolumes);
    args.NvmeManager = NvmeManager;

    ActorSystem = NStorage::CreateActorSystem(args);

    STORAGE_INFO("ActorSystem initialized");

    logging->Init(ActorSystem);
    monitoring->Init(ActorSystem);

    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_STORAGE_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOBSTORAGE_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(TABLET_FLAT_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_RDMA_PROVIDER));
    InitLWTrace();

    STORAGE_INFO("LWTrace initialized");

    SpdkLog = Logging->CreateLog("BLOCKSTORE_SPDK");
    NSpdk::InitLogging(SpdkLog);

    const auto& config = Configs->ServerConfig->GetKikimrServiceConfig()
        ? *Configs->ServerConfig->GetKikimrServiceConfig()
        : NProto::TKikimrServiceConfig();

    Service = CreateKikimrService(ActorSystem, config);
}

void TBootstrapYdb::InitAuthService()
{
    if (ActorSystem) {
        Service = CreateAuthService(
            std::move(Service),
            CreateKikimrAuthProvider(ActorSystem));

        STORAGE_INFO("AuthService initialized");
    }
}

}   // namespace NCloud::NBlockStore::NServer
