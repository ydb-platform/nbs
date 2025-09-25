#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/impl/cell_manager.h>
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
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kms/iface/compute_client.h>
#include <cloud/blockstore/libs/kms/iface/key_provider.h>
#include <cloud/blockstore/libs/kms/iface/kms_client.h>
#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/notify/iface/config.h>
#include <cloud/blockstore/libs/notify/iface/notify.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/rdma/fake/client.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/config.h>
#include <cloud/blockstore/libs/rdma/iface/probes.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/server.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/root_kms/iface/client.h>
#include <cloud/blockstore/libs/root_kms/iface/key_provider.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/service_auth.h>
#include <cloud/blockstore/libs/service_kikimr/auth_provider_kikimr.h>
#include <cloud/blockstore/libs/service_kikimr/service_kikimr.h>
#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>
#include <cloud/blockstore/libs/service_local/storage_local.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>
#include <cloud/blockstore/libs/storage/core/manually_preempted_volumes.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/bootstrap.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/init/server/actorsystem.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/config.h>
#include <cloud/storage/core/libs/io_uring/service.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/kikimr/config_dispatcher_helpers.h>
#include <cloud/storage/core/libs/kikimr/node.h>
#include <cloud/storage/core/libs/kikimr/node_registration_settings.h>
#include <cloud/storage/core/libs/kikimr/proxy.h>
#include <cloud/storage/core/libs/opentelemetry/iface/trace_service_client.h>

#include <contrib/ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <contrib/ydb/core/tablet_flat/probes.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/mon_page.h>

#include <util/digest/city.h>
#include <util/system/hostname.h>

#include <ranges>

namespace NCloud::NBlockStore::NServer {

using namespace NMonitoring;
using namespace NNvme;

using namespace NCloud::NBlockStore::NDiscovery;

using namespace NCloud::NIamClient;

using namespace NCloud::NStorage;

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

NRdma::TClientConfigPtr CreateRdmaClientConfig(
    const NRdma::TRdmaConfig& config)
{
    return std::make_shared<NRdma::TClientConfig>(config.GetClient());
}

////////////////////////////////////////////////////////////////////////////////

class TFakeRdmaClientProxy
    : public NRdma::IClient
{
private:
    IActorSystemPtr ActorSystem;
    NRdma::IClientPtr Impl;

public:
    void Init(IActorSystemPtr actorSystem)
    {
        ActorSystem = std::move(actorSystem);
    }

    void Start() override
    {
        Y_ABORT_UNLESS(ActorSystem);

        Impl = CreateFakeRdmaClient(std::move(ActorSystem));
        Impl->Start();
    }

    void Stop() override
    {
        Y_ABORT_UNLESS(Impl);
        Impl->Stop();
        Impl.reset();
    }

    auto StartEndpoint(TString host, ui32 port)
        -> NThreading::TFuture<NRdma::IClientEndpointPtr> override
    {
        return Impl->StartEndpoint(std::move(host), port);
    }

    void DumpHtml(IOutputStream& out) const override
    {
        Impl->DumpHtml(out);
    }

    [[nodiscard]] bool IsAlignedDataEnabled() const override
    {
        return Impl ? Impl->IsAlignedDataEnabled() : false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWarmupBSGroupConnectionsActor final
    : public NActors::TActorBootstrapped<TWarmupBSGroupConnectionsActor>
{
private:
    using TRequest = typename NCloud::NStorage::TEvHiveProxy::
        TEvListTabletBootInfoBackupsRequest;
    using TResponse = typename NCloud::NStorage::TEvHiveProxy::
        TEvListTabletBootInfoBackupsResponse;

private:
    NThreading::TPromise<void> Promise;
    const TDuration Timeout;
    const ui32 GroupsPerChannelToWarmup;

public:
    TWarmupBSGroupConnectionsActor(
            NThreading::TPromise<void> promise,
            TDuration timeout,
            ui32 groupsPerChannelToWarmup)
        : Promise(std::move(promise))
        , Timeout(timeout)
        , GroupsPerChannelToWarmup(groupsPerChannelToWarmup)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        TThis::Become(&TThis::StateWork);

        auto req = std::make_unique<TRequest>();

        NCloud::Send(ctx, MakeHiveProxyServiceId(), std::move(req));
        ctx.Schedule(Timeout, new TEvents::TEvWakeup());
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
            HFunc(TEvents::TEvWakeup, HandleTimeout);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "TWarmupBSGroupConnectionsActor timed out while waiting for "
            "the TEvListTabletBootInfoBackupResponse");
        Promise.SetValue();
        Die(ctx);
    }

    void HandleResponse(const TResponse::TPtr& ev, const TActorContext& ctx)
    {
        Y_DEFER
        {
            Promise.SetValue();
            Die(ctx);
        };

        if (HasError(ev->Get()->GetError())) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::SERVICE,
                "Can't list tablet boot info backups to warm up BS group "
                "connections: %s",
                FormatError(ev->Get()->GetError()).c_str());
            return;
        }

        auto tabletBootInfos = std::move(ev->Get()->TabletBootInfos);
        THashSet<ui64> groupIds;
        for (const auto& tabletBootInfo: tabletBootInfos) {
            for (const auto& channel: tabletBootInfo.StorageInfo->Channels) {
                auto historyEntries =
                    channel.History | std::views::reverse |
                    std::views::filter(
                        [&](const auto& el)
                        { return groupIds.insert(el.GroupID).second; }) |
                    std::views::take(GroupsPerChannelToWarmup);
                for (const auto& historyEntry: historyEntries) {
                    NCloud::Send(
                        ctx,
                        NKikimr::MakeBlobStorageProxyID(historyEntry.GroupID),
                        std::make_unique<NKikimr::TEvBlobStorage::TEvStatus>(
                            TInstant::Max()));
                }
            }
        }

        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Sent status messages to %zu groups in order to warm them up",
            groupIds.size());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TServerModuleFactories::TServerModuleFactories()
{
    LogbrokerServiceFactory = [](auto...)
    {
        return NLogbroker::CreateServiceStub();
    };

    IamClientFactory = [](auto...)
    {
        return NIamClient::CreateIamTokenClientStub();
    };

    ComputeClientFactory = [](auto...)
    {
        return CreateComputeClientStub();
    };

    KmsClientFactory = [](auto...)
    {
        return CreateKmsClientStub();
    };

    RootKmsClientFactory = [](auto...)
    {
        return CreateRootKmsClientStub();
    };

    TraceServiceClientFactory = [](auto...)
    {
        return CreateTraceServiceClientStub();
    };

    SpdkFactory = [](auto...)
    {
        return TSpdkParts{
            .Env = NSpdk::CreateEnvStub(),
            .VhostCallbacks = {},
            .LogInitializer = {},
        };
    };

    RdmaClientFactory = [] (
        NCloud::ILoggingServicePtr logging,
        NCloud::IMonitoringServicePtr monitoring,
        NRdma::TClientConfigPtr config)
    {
        return NRdma::CreateClient(
            NRdma::NVerbs::CreateVerbs(),
            std::move(logging),
            std::move(monitoring),
            std::move(config));
    };

    RdmaServerFactory = [] (
        NCloud::ILoggingServicePtr logging,
        NCloud::IMonitoringServicePtr monitoring,
        NRdma::TServerConfigPtr config)
    {
        return NRdma::CreateServer(
            NRdma::NVerbs::CreateVerbs(),
            std::move(logging),
            std::move(monitoring),
            std::move(config));
    };

    NotifyServiceFactory = [](auto...)
    {
        return NNotify::CreateServiceStub();
    };
}

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
IStartable* TBootstrapYdb::GetYdbStorage()         { return AsStartable(YdbStorage); }
IStartable* TBootstrapYdb::GetLogbrokerService()   { return LogbrokerService.get(); }
IStartable* TBootstrapYdb::GetNotifyService()      { return NotifyService.get(); }
IStartable* TBootstrapYdb::GetStatsFetcher()       { return StatsFetcher.get(); }
IStartable* TBootstrapYdb::GetIamTokenClient()     { return IamTokenClient.get(); }
IStartable* TBootstrapYdb::GetComputeClient()      { return ComputeClient.get(); }
IStartable* TBootstrapYdb::GetKmsClient()          { return KmsClient.get(); }
IStartable* TBootstrapYdb::GetRootKmsClient()      { return RootKmsClient.get(); }
ITraceSerializerPtr TBootstrapYdb::GetTraceSerializer()
{
    return TraceSerializer;
}

ITraceServiceClientPtr TBootstrapYdb::GetTraceServiceClient()
{
    return TraceServiceClient;
}

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
    Configs->InitRootKmsConfig();
    Configs->InitComputeClientConfig();
    Configs->InitCellsConfig();
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
                CreateRdmaClientConfig(*Configs->RdmaConfig));

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
    auto rdmaConfig = std::make_shared<NRdma::TServerConfig>();

    RdmaServer = ServerModuleFactories->RdmaServerFactory(
        Logging,
        Monitoring,
        std::move(rdmaConfig));
}

void TBootstrapYdb::InitDiskAgentBackend()
{
    const auto& config = *Configs->DiskAgentConfig;
    if (!config.GetEnabled()) {
        return;
    }

    if (config.GetBackend() == NProto::DISK_AGENT_BACKEND_SPDK) {
        Y_ABORT_UNLESS(Spdk, "SPDK backend should be already initialized");
    }

    Y_ABORT_IF(NvmeManager);
    Y_ABORT_IF(FileIOServiceProvider);
    Y_ABORT_IF(LocalStorageProvider);

    auto r = CreateDiskAgentBackendComponents(Logging, config);
    NvmeManager = std::move(r.NvmeManager);
    FileIOServiceProvider = std::move(r.FileIOServiceProvider);
    LocalStorageProvider = std::move(r.StorageProvider);

    STORAGE_INFO(
        "Disk Agent backend ("
        << NProto::EDiskAgentBackendType_Name(config.GetBackend())
        << ") initialized");
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
        .LegacyRegistrationTimeout = Configs->StorageConfig->GetNodeRegistrationTimeout(),
        .DynamicNodeRegistrationTimeout = Configs->StorageConfig->GetDynamicNodeRegistrationTimeout(),
        .LoadConfigsFromCmsRetryMinDelay = Configs->StorageConfig->GetLoadConfigsFromCmsRetryMinDelay(),
        .LoadConfigsFromCmsRetryMaxDelay = Configs->StorageConfig->GetLoadConfigsFromCmsRetryMaxDelay(),
        .LoadConfigsFromCmsTotalTimeout = Configs->StorageConfig->GetLoadConfigsFromCmsTotalTimeout(),
        .PathToGrpcCaFile = Configs->StorageConfig->GetNodeRegistrationRootCertsFile(),
        .PathToGrpcCertFile = cert.CertFile,
        .PathToGrpcPrivateKeyFile = cert.CertPrivateKeyFile,
        .NodeRegistrationToken = Configs->StorageConfig->GetNodeRegistrationToken(),
        .NodeType = Configs->StorageConfig->GetNodeType(),
    };

    auto nodeLabels = GetLabels(
        Configs->StorageConfig->GetConfigDispatcherSettings(),
        Configs->StorageConfig->GetSchemeShardDir(),
        Configs->StorageConfig->GetNodeType());
    NCloud::NStorage::TRegisterDynamicNodeOptions registerOpts{
        .Domain = Configs->Options->Domain,
        .SchemeShardDir = Configs->StorageConfig->GetSchemeShardDir(),
        .NodeBrokerAddress = Configs->Options->NodeBrokerAddress,
        .NodeBrokerPort = Configs->Options->NodeBrokerPort,
        .NodeBrokerSecurePort = Configs->Options->NodeBrokerSecurePort,
        .UseNodeBrokerSsl = Configs->Options->UseNodeBrokerSsl ||
                            Configs->StorageConfig->GetNodeRegistrationUseSsl(),
        .InterconnectPort = Configs->Options->InterconnectPort,
        .LoadCmsConfigs = Configs->Options->LoadCmsConfigs,
        .Settings = std::move(settings),
        .Labels = std::move(nodeLabels),
    };

    const bool emergencyMode =
        Configs->StorageConfig->GetHiveProxyFallbackMode() ||
        Configs->StorageConfig->GetSSProxyFallbackMode();
    if (emergencyMode &&
        Configs->StorageConfig
            ->GetDontPassSchemeShardDirWhenRegisteringNodeInEmergencyMode())
    {
        registerOpts.SchemeShardDir = "";
    }

    if (Configs->Options->LocationFile) {
        NProto::TLocation location;
        ParseProtoTextFromFile(Configs->Options->LocationFile, location);

        registerOpts.DataCenter = location.GetDataCenter();
        Configs->Rack = location.GetRack();
    }

    Configs->InitDiskAgentConfig();

    STORAGE_INFO("Configs initialized");

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

    STORAGE_INFO("CMS configs initialized");

    // InitRdmaConfig should be called after InitDiskAgentConfig,
    // InitServerConfig and ApplyCMSConfigs to backport legacy
    // RDMA config
    Configs->InitRdmaConfig();

    STORAGE_INFO("RDMA config initialized");

    auto logging = std::make_shared<TLoggingProxy>();
    TraceSerializer = CreateTraceSerializer(
        logging,
        "BLOCKSTORE_TRACE",
        NLwTraceMonPage::TraceManager(false));

    STORAGE_INFO("TraceSerializer initialized");

    auto monitoring = std::make_shared<TMonitoringProxy>();
    Logging = logging;
    Monitoring = monitoring;

    std::shared_ptr<TFakeRdmaClientProxy> fakeRdmaClientProxy;
    if (Configs->ServerConfig->GetUseFakeRdmaClient() &&
        Configs->RdmaConfig->GetClientEnabled())
    {
        fakeRdmaClientProxy = std::make_shared<TFakeRdmaClientProxy>();
        RdmaClient = fakeRdmaClientProxy;

        STORAGE_INFO("Fake RDMA client initialized");
    } else {
        InitRdmaClient();
    }

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
        [percentiles = ClientPercentiles](
            NMonitoring::TDynamicCountersPtr updatedCounters,
            NMonitoring::TDynamicCountersPtr baseCounters)
        {
            percentiles->CalculatePercentiles(updatedCounters);
            UpdateClientStats(
                std::move(updatedCounters),
                std::move(baseCounters));
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
            Scheduler,
            IamTokenClient);
        StatsUploader = NYdbStats::CreateYdbVolumesStatsUploader(
            statsConfig,
            logging,
            YdbStorage,
            NYdbStats::TYDBTableSchemes(
                statsConfig->GetStatsTableTtl(),
                statsConfig->GetArchiveStatsTableTtl()));
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

    RootKmsClient = ServerModuleFactories->RootKmsClientFactory(
        Configs->RootKmsConfig,
        logging);

    RootKmsKeyProvider = CreateRootKmsKeyProvider(
        RootKmsClient,
        Configs->RootKmsConfig.GetKeyId());

    STORAGE_INFO("RootKmsKeyProvider initialized");

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

    if (Configs->DiagnosticsConfig->GetUseAsyncLogger()) {
        AsyncLogger = CreateAsyncLogger();

        STORAGE_INFO("AsyncLogger initialized");
    }

    InitSpdk();

    InitDiskAgentBackend();

    Allocator = CreateCachingAllocator(
        Spdk ? Spdk->GetAllocator() : TDefaultAllocator::Instance(),
        Configs->DiskAgentConfig->GetPageSize(),
        Configs->DiskAgentConfig->GetMaxPageCount(),
        Configs->DiskAgentConfig->GetPageDropSize());

    STORAGE_INFO("Allocator initialized");

    InitProfileLog();

    STORAGE_INFO("ProfileLog initialized");

    StatsFetcher = NCloud::NStorage::BuildStatsFetcher(
        Configs->DiagnosticsConfig->GetStatsFetcherType(),
        Configs->DiagnosticsConfig->GetCpuWaitFilename(),
        Log,
        logging);

    STORAGE_INFO("StatsFetcher initialized");

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

    NotifyService = ServerModuleFactories->NotifyServiceFactory(
        Configs->NotifyConfig,
        IamTokenClient,
        logging);

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
    args.LocalStorageProvider = LocalStorageProvider;
    args.ProfileLog = ProfileLog;
    args.BlockDigestGenerator = BlockDigestGenerator;
    args.TraceSerializer = TraceSerializer;
    args.LogbrokerService = LogbrokerService;
    args.NotifyService = NotifyService;
    args.VolumeStats = VolumeStats;
    args.StatsFetcher = StatsFetcher;
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
    args.RootKmsKeyProvider = RootKmsKeyProvider;
    args.TemporaryServer = Configs->Options->TemporaryServer;

    ActorSystem = NStorage::CreateActorSystem(args);

    if (args.IsDiskRegistrySpareNode) {
        STORAGE_INFO("The host configured as a spare node for Disk Registry");
    }

    STORAGE_INFO("ActorSystem initialized");

    logging->Init(ActorSystem);
    monitoring->Init(ActorSystem);

    if (fakeRdmaClientProxy) {
        fakeRdmaClientProxy->Init(ActorSystem);
    }

    if (args.IsDiskRegistrySpareNode) {
        auto rootGroup = Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto serverGroup = rootGroup->GetSubgroup("component", "server");
        auto isSpareNode = serverGroup->GetCounter("IsSpareNode", false);
        *isSpareNode = 1;
    }

    TraceServiceClient = ServerModuleFactories->TraceServiceClientFactory(
        Configs->DiagnosticsConfig->GetOpentelemetryTraceConfig()
            .GetClientConfig(),
        logging);

    STORAGE_INFO("Trace service client initialized");

    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_STORAGE_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOBSTORAGE_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(TABLET_FLAT_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_RDMA_PROVIDER));
    InitLWTrace(Configs->DiagnosticsConfig->GetOpentelemetryTraceConfig()
                    .GetServiceName());

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

void TBootstrapYdb::WarmupBSGroupConnections()
{
    if (!Configs->StorageConfig ||
        !Configs->StorageConfig->GetTabletBootInfoBackupFilePath())
    {
        return;
    }

    auto promise = NThreading::NewPromise<void>();
    auto future = promise.GetFuture();

    ActorSystem->Register(std::make_unique<TWarmupBSGroupConnectionsActor>(
        std::move(promise),
        Configs->StorageConfig->GetWarmupBSGroupConnectionsTimeout(),
        Configs->StorageConfig->GetBSGroupsPerChannelToWarmup()));

    future.Wait();
}

void TBootstrapYdb::InitRdmaRequestServer()
{
    auto rdmaConfig = std::make_shared<NRdma::TServerConfig>(
        Configs->RdmaConfig->GetServer());

    RdmaRequestServer = ServerModuleFactories->RdmaServerFactory(
        Logging,
        Monitoring,
        std::move(rdmaConfig));
}

void TBootstrapYdb::SetupCellManager()
{
    if (Configs->CellsConfig->GetCellsEnabled()) {
        CellManager = CreateCellManager(
            Configs->CellsConfig,
            Timer,
            Scheduler,
            Logging,
            Monitoring,
            GetTraceSerializer(),
            ServerStats,
            RdmaClient);
    } else {
        CellManager = NCells::CreateCellManagerStub();
    }
}

}   // namespace NCloud::NBlockStore::NServer
