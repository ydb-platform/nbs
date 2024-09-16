#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/discovery/balancing.h>
#include <cloud/blockstore/libs/discovery/ban.h>
#include <cloud/blockstore/libs/discovery/discovery.h>
#include <cloud/blockstore/libs/discovery/fetch.h>
#include <cloud/blockstore/libs/discovery/healthcheck.h>
#include <cloud/blockstore/libs/discovery/ping.h>
#include <cloud/blockstore/libs/endpoints/endpoint_events.h>
#include <cloud/blockstore/libs/kms/iface/compute_client.h>
#include <cloud/blockstore/libs/kms/iface/key_provider.h>
#include <cloud/blockstore/libs/kms/iface/kms_client.h>
#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/notify/config.h>
#include <cloud/blockstore/libs/notify/notify.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/rdma/iface/probes.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/config.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/service_auth.h>
#include <cloud/blockstore/libs/service_kikimr/auth_provider_kikimr.h>
#include <cloud/blockstore/libs/service_kikimr/service_kikimr.h>
#include <cloud/blockstore/libs/service_local/storage_aio.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
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
#include <cloud/storage/core/libs/diagnostics/cgroup_stats_fetcher.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/config.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/kikimr/node_registration_settings.h>
#include <cloud/storage/core/libs/kikimr/node.h>
#include <cloud/storage/core/libs/kikimr/proxy.h>

#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/tablet_flat/probes.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/mon_page.h>

#include <util/digest/city.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NServer {

using namespace NMonitoring;
using namespace NNvme;

using namespace NCloud::NBlockStore::NDiscovery;

using namespace NCloud::NIamClient;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

NRdma::TClientConfigPtr CreateRdmaClientConfig(
    const NRdma::TRdmaConfigPtr config)
{
    return std::make_shared<NRdma::TClientConfig>(config->GetClient());
}

}   // namespace

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
IStartable* TBootstrapYdb::GetComputeClient()      { return ComputeClient.get(); }
IStartable* TBootstrapYdb::GetKmsClient()          { return KmsClient.get(); }

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
    Configs->InitKmsClientConfig();
    Configs->InitComputeClientConfig();
}

void TBootstrapYdb::InitSpdk()
{
    const bool needSpdkForInitiator =
        Configs->ServerConfig->GetNvmfInitiatorEnabled();

    const bool needSpdkForTarget =
        Configs->ServerConfig->GetNVMeEndpointEnabled() ||
        Configs->ServerConfig->GetSCSIEndpointEnabled();

    const bool needSpdkForDiskAgent =
        Configs->DiskAgentConfig->GetEnabled() &&
        Configs->DiskAgentConfig->GetBackend() == NProto::DISK_AGENT_BACKEND_SPDK;

    if (needSpdkForInitiator || needSpdkForTarget || needSpdkForDiskAgent) {
        auto spdkParts =
            ServerModuleFactories->SpdkFactory(Configs->SpdkEnvConfig);
        Spdk = std::move(spdkParts.Env);
        VhostCallbacks = std::move(spdkParts.VhostCallbacks);
        SpdkLogInitializer = std::move(spdkParts.LogInitializer);

        STORAGE_INFO("Spdk initialized");
    }
}

void TBootstrapYdb::InitRdmaClient()
{
    try {
        if (Configs->RdmaConfig->GetClientEnabled()) {
            RdmaClient = ServerModuleFactories->RdmaClientFactory(
                Logging,
                Monitoring,
                CreateRdmaClientConfig(Configs->RdmaConfig));

            STORAGE_INFO("RDMA client initialized");
        }
    } catch (...) {
        STORAGE_ERROR("Failed to initialize RDMA client: "
            << CurrentExceptionMessage().c_str());

        RdmaClient = nullptr;
        PostponedCriticalEvents.push_back(GetCriticalEventForRdmaError());
    }
}

void TBootstrapYdb::InitRdmaServer()
{
    // TODO: read config
    auto rdmaConfig = std::make_shared<NRdma::TServerConfig>();

    RdmaServer = ServerModuleFactories->RdmaServerFactory(
        Logging,
        Monitoring,
        std::move(rdmaConfig));
}

void TBootstrapYdb::InitKikimrService()
{
    InitConfigs();

    auto preemptedVolumes = NStorage::CreateManuallyPreemptedVolumes(
        Configs->StorageConfig,
        Log,
        PostponedCriticalEvents);

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

    bool loadCmsConfigs = Configs->Options->LoadCmsConfigs;
    if (loadCmsConfigs &&
        (Configs->StorageConfig->GetHiveProxyFallbackMode() ||
        Configs->StorageConfig->GetSSProxyFallbackMode()))
    {
        STORAGE_INFO("Disable loading configs from CMS in emergency mode");
        loadCmsConfigs = false;
    }

    NCloud::NStorage::TRegisterDynamicNodeOptions registerOpts {
        .Domain = Configs->Options->Domain,
        .SchemeShardDir = Configs->StorageConfig->GetSchemeShardDir(),
        .NodeBrokerAddress = Configs->Options->NodeBrokerAddress,
        .NodeBrokerPort = Configs->Options->NodeBrokerPort,
        .UseNodeBrokerSsl = Configs->Options->UseNodeBrokerSsl,
        .InterconnectPort = Configs->Options->InterconnectPort,
        .LoadCmsConfigs = loadCmsConfigs,
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

    auto [nodeId, scopeId, cmsConfig] = RegisterDynamicNode(
        Configs->KikimrConfig,
        registerOpts,
        Log);

    if (cmsConfig) {
        Configs->ApplyCMSConfigs(std::move(*cmsConfig));
    }

    STORAGE_INFO("CMS configs initialized");

    // InitRdmaConfig should be called after InitDiskAgentConfig,
    // InitServerConfig and ApplyCMSConfigs to backport legacy
    // RDMA config
    Configs->InitRdmaConfig();

    STORAGE_INFO("RDMA config initialized");

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
    if (statsConfig->IsValid() && !Configs->Options->TemporaryServer) {
        YdbStorage = NYdbStats::CreateYdbStorage(
            statsConfig,
            logging,
            IamTokenClient);
        StatsUploader = NYdbStats::CreateYdbVolumesStatsUploader(
            statsConfig,
            logging,
            YdbStorage,
            NYdbStats::CreateStatsTableScheme(statsConfig->GetStatsTableTtl()),
            NYdbStats::CreateHistoryTableScheme(),
            NYdbStats::CreateArchiveStatsTableScheme(statsConfig->GetArchiveStatsTableTtl()),
            NYdbStats::CreateBlobLoadMetricsTableScheme());
    } else {
        StatsUploader = NYdbStats::CreateVolumesStatsUploaderStub();
    }

    STORAGE_INFO("StatsUploader initialized");

    ComputeClient = ServerModuleFactories->ComputeClientFactory(
        Configs->ComputeClientConfig,
        logging);

    KmsClient = ServerModuleFactories->KmsClientFactory(
        Configs->KmsClientConfig,
        logging);

    KmsKeyProvider = CreateKmsKeyProvider(
        Executor,
        IamTokenClient,
        ComputeClient,
        KmsClient);

    STORAGE_INFO("KmsKeyProvider initialized");

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
        Y_ABORT_UNLESS(FileIOService);

        NvmeManager = CreateNvmeManager(
            Configs->DiskAgentConfig->GetSecureEraseTimeout());

        AioStorageProvider = CreateAioStorageProvider(
            FileIOService,
            NvmeManager,
            !Configs->DiskAgentConfig->GetDirectIoFlagDisabled(),
            EAioSubmitQueueOpt::DontUse
        );

        STORAGE_INFO("AioStorageProvider initialized");
    }

    if (Configs->DiskAgentConfig->GetEnabled() &&
        Configs->DiskAgentConfig->GetBackend() == NProto::DISK_AGENT_BACKEND_NULL &&
        !AioStorageProvider)
    {
        NvmeManager = CreateNvmeManager(
            Configs->DiskAgentConfig->GetSecureEraseTimeout());
        AioStorageProvider = CreateNullStorageProvider();

        STORAGE_INFO("AioStorageProvider (null) initialized");
    }

    Y_ABORT_UNLESS(FileIOService);

    Allocator = CreateCachingAllocator(
        Spdk ? Spdk->GetAllocator() : TDefaultAllocator::Instance(),
        Configs->DiskAgentConfig->GetPageSize(),
        Configs->DiskAgentConfig->GetMaxPageCount(),
        Configs->DiskAgentConfig->GetPageDropSize());

    STORAGE_INFO("Allocator initialized");

    InitProfileLog();

    STORAGE_INFO("ProfileLog initialized");

    auto cgroupStatsFetcherMonitoringSettings =
        TCgroupStatsFetcherMonitoringSettings{
            .CountersGroupName = "blockstore",
            .ComponentGroupName = "server",
            .CounterName = "CpuWaitFailure",
        };

    CgroupStatsFetcher = CreateCgroupStatsFetcher(
        "BLOCKSTORE_CGROUPS",
        logging,
        monitoring,
        Configs->DiagnosticsConfig->GetCpuWaitFilename(),
        std::move(cgroupStatsFetcherMonitoringSettings));

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
        ? NNotify::CreateService(Configs->NotifyConfig, IamTokenClient)
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
    args.RdmaConfig = Configs->RdmaConfig;
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
    args.UserCounterProviders = {VolumeStats->GetUserCounters()};
    args.IsDiskRegistrySpareNode = [&] {
            if (!Configs->StorageConfig->GetDisableLocalService()) {
                return false;
            }

            const auto& nodes = Configs->StorageConfig->GetKnownSpareNodes();
            const auto& fqdn = FQDNHostName();
            const ui32 p = Configs->StorageConfig->GetSpareNodeProbability();

            return FindPtr(nodes, fqdn) || CityHash64(fqdn) % 100 < p;
        }();
    args.VolumeBalancerSwitch = VolumeBalancerSwitch;
    args.EndpointEventHandler = EndpointEventHandler;

    ActorSystem = NStorage::CreateActorSystem(args);

    if (args.IsDiskRegistrySpareNode) {
        STORAGE_INFO("The host configured as a spare node for Disk Registry");
    }

    STORAGE_INFO("ActorSystem initialized");

    logging->Init(ActorSystem);
    monitoring->Init(ActorSystem);

    if (args.IsDiskRegistrySpareNode) {
        auto rootGroup = Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto serverGroup = rootGroup->GetSubgroup("component", "server");
        auto isSpareNode = serverGroup->GetCounter("IsSpareNode", false);
        *isSpareNode = 1;
    }

    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_STORAGE_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOBSTORAGE_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(TABLET_FLAT_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_RDMA_PROVIDER));
    InitLWTrace();

    STORAGE_INFO("LWTrace initialized");

    SpdkLog = Logging->CreateLog("BLOCKSTORE_SPDK");
    if (SpdkLogInitializer) {
        SpdkLogInitializer(SpdkLog);
    }

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
