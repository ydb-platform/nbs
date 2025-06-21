#include "actorsystem.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/api/volume_balancer.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_actor.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/blockstore/libs/storage/init/common/actorsystem.h>
#include <cloud/blockstore/libs/storage/partition/part_actor.h>
#include <cloud/blockstore/libs/storage/partition2/part2_actor.h>
#include <cloud/blockstore/libs/storage/service/service.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service.h>
#include <cloud/blockstore/libs/storage/ss_proxy/ss_proxy.h>
#include <cloud/blockstore/libs/storage/undelivered/undelivered.h>
#include <cloud/blockstore/libs/storage/volume/volume.h>
#include <cloud/blockstore/libs/storage/volume/volume_actor.h>
#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer.h>
#include <cloud/blockstore/libs/storage/volume_proxy/volume_proxy.h>

#include <cloud/storage/core/libs/api/authorizer.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/api/user_stats.h>
#include <cloud/storage/core/libs/auth/authorizer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/hive_proxy/hive_proxy.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/kikimr/config_dispatcher_helpers.h>
#include <cloud/storage/core/libs/kikimr/kikimr_initializer.h>
#include <cloud/storage/core/libs/kikimr/tenant.h>
#include <cloud/storage/core/libs/user_stats/user_stats.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/driver_lib/run/kikimr_services_initializers.h>
#include <contrib/ydb/core/driver_lib/run/run.h>
#include <contrib/ydb/core/grpc_services/grpc_request_proxy.h>
#include <contrib/ydb/core/load_test/service_actor.h>
#include <contrib/ydb/core/mind/labels_maintainer.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/core/mind/tenant_pool.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/tablet/node_tablet_monitor.h>
#include <contrib/ydb/core/tablet/tablet_list_renderer.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoring;

using namespace NKikimr;
using namespace NKikimr::NKikimrServicesInitializers;
using namespace NKikimr::NNodeTabletMonitor;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCustomTabletStateClassifier final
    : public TTabletStateClassifier
{
};

////////////////////////////////////////////////////////////////////////////////

struct TCustomTabletListRenderer final
    : public TTabletListRenderer
{
    TString GetUserStateName(const TTabletListElement& elem) override
    {
        if (elem.TabletStateInfo->HasUserState()) {
            ui32 state = elem.TabletStateInfo->GetUserState();
            switch (elem.TabletStateInfo->GetType()) {
                case TTabletTypes::BlockStoreVolume:
                    return TVolumeActor::GetStateName(state);
                case TTabletTypes::BlockStorePartition:
                    return NPartition::TPartitionActor::GetStateName(state);
                case TTabletTypes::BlockStorePartition2:
                    return NPartition2::TPartitionActor::GetStateName(state);
                case TTabletTypes::BlockStoreDiskRegistry:
                    return TDiskRegistryActor::GetStateName(state);
                default:
                    break;
            }
        }
        return TTabletListRenderer::GetUserStateName(elem);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStorageServicesInitializer final
    : public IServiceInitializer
{
private:
    const TServerActorSystemArgs Args;

public:
    TStorageServicesInitializer(const TServerActorSystemArgs& args)
        : Args(args)
    {}

    void InitializeServices(
        TActorSystemSetup* setup,
        const TAppData* appData) override
    {
        Args.StorageConfig->Register(*appData->Icb);

        //
        // SSProxy
        //

        auto ssProxy = CreateSSProxy(Args.StorageConfig);

        setup->LocalServices.emplace_back(
            MakeSSProxyServiceId(),
            TActorSetupCmd(
                ssProxy.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // HiveProxy
        //

        auto hiveProxy = CreateHiveProxy(
            {
                .PipeClientRetryCount =
                    Args.StorageConfig->GetPipeClientRetryCount(),
                .PipeClientMinRetryTime =
                    Args.StorageConfig->GetPipeClientMinRetryTime(),
                .HiveLockExpireTimeout =
                    Args.StorageConfig->GetHiveLockExpireTimeout(),
                .LogComponent = TBlockStoreComponents::HIVE_PROXY,
                .TabletBootInfoBackupFilePath =
                    Args.TemporaryServer
                        ? ""
                        : Args.StorageConfig->GetTabletBootInfoBackupFilePath(),
                .FallbackMode = Args.StorageConfig->GetHiveProxyFallbackMode(),
                .TenantHiveTabletId =
                    Args.StorageConfig->GetTenantHiveTabletId(),
            },
            appData->Counters->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service"));

        setup->LocalServices.emplace_back(
            MakeHiveProxyServiceId(),
            TActorSetupCmd(
                hiveProxy.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // VolumeProxy
        //

        auto volumeProxy = CreateVolumeProxy(
            Args.StorageConfig,
            Args.TraceSerializer,
            Args.TemporaryServer);

        setup->LocalServices.emplace_back(
            MakeVolumeProxyServiceId(),
            TActorSetupCmd(
                volumeProxy.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // DiskRegistryProxy
        //

        auto diskRegistryProxy = CreateDiskRegistryProxy(
            Args.StorageConfig,
            Args.DiskRegistryProxyConfig);

        setup->LocalServices.emplace_back(
            MakeDiskRegistryProxyServiceId(),
            TActorSetupCmd(
                diskRegistryProxy.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // StorageStatsService
        //

        auto storageStatsService = CreateStorageStatsService(
            Args.StorageConfig,
            Args.DiagnosticsConfig,
            Args.StatsUploader,
            Args.StatsAggregator);

        setup->LocalServices.emplace_back(
            MakeStorageStatsServiceId(),
            TActorSetupCmd(
                storageStatsService.release(),
                TMailboxType::Revolving,
                appData->BatchPoolId));

        //
        // StorageService
        //

        auto storageService = CreateStorageService(
            Args.StorageConfig,
            Args.DiagnosticsConfig,
            Args.ProfileLog,
            Args.BlockDigestGenerator,
            Args.DiscoveryService,
            Args.TraceSerializer,
            Args.EndpointEventHandler,
            Args.RdmaClient,
            Args.VolumeStats,
            Args.PreemptedVolumes,
            Args.RootKmsKeyProvider,
            Args.TemporaryServer);

        setup->LocalServices.emplace_back(
            MakeStorageServiceId(),
            TActorSetupCmd(
                storageService.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // StorageUserStats
        //

        auto storageUserStats =
            NCloud::NStorage::NUserStats::CreateStorageUserStats(
                TBlockStoreComponents::USER_STATS,
                "blockstore",
                "BlockStore",
                Args.UserCounterProviders);

        setup->LocalServices.emplace_back(
            MakeStorageUserStatsId(),
            TActorSetupCmd(
                storageUserStats.release(),
                TMailboxType::Revolving,
                appData->BatchPoolId));

        //
        // UndeliveredHandler
        //

        auto undeliveredHandler = CreateUndeliveredHandler();

        setup->LocalServices.emplace_back(
            MakeUndeliveredHandlerServiceId(),
            TActorSetupCmd(
                undeliveredHandler.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // Authorizer
        //

        auto authorizer = CreateAuthorizerActor(
            TBlockStoreComponents::AUTH,
            "blockstore",
            Args.StorageConfig->GetFolderId(),
            Args.StorageConfig->GetAuthorizationMode(),
            Args.AppConfig->HasAuthConfig());

        setup->LocalServices.emplace_back(
            MakeAuthorizerServiceId(),
            TActorSetupCmd(
                authorizer.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // DiskAgent
        //

        if (Args.DiskAgentConfig->GetEnabled()) {
            auto diskAgent = CreateDiskAgent(
                Args.StorageConfig,
                Args.DiskAgentConfig,
                Args.RdmaConfig,
                Args.Spdk,
                Args.Allocator,
                Args.AioStorageProvider,
                Args.ProfileLog,
                Args.BlockDigestGenerator,
                Args.Logging,
                Args.RdmaServer,
                Args.NvmeManager);

            setup->LocalServices.emplace_back(
                MakeDiskAgentServiceId(Args.NodeId),
                TActorSetupCmd(
                    diskAgent.release(),
                    TMailboxType::Revolving,
                    appData->UserPoolId));
        }

        //
        // VolumeBindingService
        //

        auto volumeBalancerService = CreateVolumeBalancerActor(
            Args.StorageConfig,
            Args.VolumeStats,
            Args.StatsFetcher,
            Args.VolumeBalancerSwitch,
            MakeStorageServiceId());

        setup->LocalServices.emplace_back(
            MakeVolumeBalancerServiceId(),
            TActorSetupCmd(
                volumeBalancerService.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // BlobStorage LoadActorService
        //

        if (Args.StorageConfig->GetEnableLoadActor()) {
            IActorPtr loadActorService(CreateLoadTestActor(appData->Counters));

            setup->LocalServices.emplace_back(
                MakeLoadServiceID(Args.NodeId),
                TActorSetupCmd(
                    loadActorService.release(),
                    TMailboxType::HTSwap,
                    appData->UserPoolId));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCustomLocalServiceInitializer final
    : public IServiceInitializer
{
private:
    const NKikimrConfig::TAppConfig& AppConfig;
    const ILoggingServicePtr Logging;
    const TStorageConfigPtr StorageConfig;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ITraceSerializerPtr TraceSerializer;
    const NLogbroker::IServicePtr LogbrokerService;
    const NNotify::IServicePtr NotifyService;
    const NRdma::IClientPtr RdmaClient;
    const NServer::IEndpointEventHandlerPtr EndpointEventHandler;
    const bool IsDiskRegistrySpareNode;

public:
    TCustomLocalServiceInitializer(
            const NKikimrConfig::TAppConfig& appConfig,
            ILoggingServicePtr logging,
            TStorageConfigPtr storageConfig,
            TDiagnosticsConfigPtr diagnosticsConfig,
            IProfileLogPtr profileLog,
            IBlockDigestGeneratorPtr blockDigestGenerator,
            ITraceSerializerPtr traceSerializer,
            NLogbroker::IServicePtr logbrokerService,
            NNotify::IServicePtr notifyService,
            NRdma::IClientPtr rdmaClient,
            NServer::IEndpointEventHandlerPtr endpointEventHandler,
            bool isDiskRegistrySpareNode)
        : AppConfig(appConfig)
        , Logging(std::move(logging))
        , StorageConfig(std::move(storageConfig))
        , DiagnosticsConfig(std::move(diagnosticsConfig))
        , ProfileLog(std::move(profileLog))
        , BlockDigestGenerator(std::move(blockDigestGenerator))
        , TraceSerializer(std::move(traceSerializer))
        , LogbrokerService(std::move(logbrokerService))
        , NotifyService(std::move(notifyService))
        , RdmaClient(std::move(rdmaClient))
        , EndpointEventHandler(std::move(endpointEventHandler))
        , IsDiskRegistrySpareNode(isDiskRegistrySpareNode)
    {}

    void InitializeServices(
        TActorSystemSetup* setup,
        const TAppData* appData) override
    {
        auto storageConfig = StorageConfig;
        auto diagnosticsConfig = DiagnosticsConfig;
        auto profileLog = ProfileLog;
        auto blockDigestGenerator = BlockDigestGenerator;
        auto traceSerializer = TraceSerializer;
        auto logbrokerService = LogbrokerService;
        auto notifyService = NotifyService;
        auto rdmaClient = RdmaClient;
        auto endpointEventHandler = EndpointEventHandler;
        auto logging = Logging;

        auto volumeFactory = [=] (const TActorId& owner, TTabletStorageInfo* storage) {
            Y_ABORT_UNLESS(storage->TabletType == TTabletTypes::BlockStoreVolume);

            auto actor = CreateVolumeTablet(
                owner,
                storage,
                storageConfig,
                diagnosticsConfig,
                profileLog,
                blockDigestGenerator,
                traceSerializer,
                rdmaClient,
                endpointEventHandler,
                EVolumeStartMode::ONLINE,
                {}   // diskId
            );
            return actor.release();
        };

        auto diskRegistryFactory = [=] (const TActorId& owner, TTabletStorageInfo* storage) {
            Y_ABORT_UNLESS(storage->TabletType == TTabletTypes::BlockStoreDiskRegistry);

            auto tablet = CreateDiskRegistry(
                owner,
                logging,
                storage,
                storageConfig,
                diagnosticsConfig,
                logbrokerService,
                notifyService);
            return tablet.release();
        };

        const bool enableLocal = !StorageConfig->GetDisableLocalService();

        if (enableLocal || IsDiskRegistrySpareNode) {
            auto localConfig = MakeIntrusive<TLocalConfig>();

            if (enableLocal) {
                localConfig->TabletClassInfo[TTabletTypes::BlockStoreVolume] =
                    TLocalConfig::TTabletClassInfo(
                        MakeIntrusive<TTabletSetupInfo>(
                            volumeFactory,
                            TMailboxType::ReadAsFilled,
                            appData->UserPoolId,
                            TMailboxType::ReadAsFilled,
                            appData->SystemPoolId));
            }

            const i32 priority { IsDiskRegistrySpareNode ? -1 : 0 };

            localConfig->TabletClassInfo[TTabletTypes::BlockStoreDiskRegistry] =
                TLocalConfig::TTabletClassInfo(
                    MakeIntrusive<TTabletSetupInfo>(
                        diskRegistryFactory,
                        TMailboxType::ReadAsFilled,
                        appData->UserPoolId,
                        TMailboxType::ReadAsFilled,
                        appData->SystemPoolId),
                    priority);

            ConfigureTenantSystemTablets(
                *appData,
                *localConfig,
                StorageConfig->GetAllowAdditionalSystemTablets()
            );

            auto tenantPoolConfig = MakeIntrusive<TTenantPoolConfig>(localConfig);
            tenantPoolConfig->AddStaticSlot(StorageConfig->GetSchemeShardDir());

            setup->LocalServices.emplace_back(
                MakeTenantPoolRootID(),
                TActorSetupCmd(
                    CreateTenantPool(tenantPoolConfig),
                    TMailboxType::ReadAsFilled,
                    appData->SystemPoolId));
        }

        setup->LocalServices.emplace_back(
            TActorId(),
            TActorSetupCmd(
                CreateLabelsMaintainer(AppConfig.GetMonitoringConfig()),
                TMailboxType::ReadAsFilled,
                appData->SystemPoolId));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorSystemPtr CreateActorSystem(const TServerActorSystemArgs& sArgs)
{
    auto prepareKikimrRunConfig = [&] (TKikimrRunConfig& runConfig) {
        if (sArgs.StorageConfig->GetConfigsDispatcherServiceEnabled()) {
            SetupConfigDispatcher(
                sArgs.StorageConfig->GetConfigDispatcherSettings(),
                sArgs.StorageConfig->GetSchemeShardDir(),
                sArgs.StorageConfig->GetNodeType(),
                &runConfig.ConfigsDispatcherInitInfo);
            runConfig.ConfigsDispatcherInitInfo.InitialConfig = runConfig.AppConfig;
        }
    };
    auto onInitialize = [&] (
        TKikimrRunConfig& runConfig,
        TServiceInitializersList& initializers)
    {
        initializers.AddServiceInitializer(
            new NStorage::TKikimrServicesInitializer(sArgs.AppConfig));
        initializers.AddServiceInitializer(new TTabletMonitorInitializer(
            runConfig,
            MakeIntrusive<TCustomTabletStateClassifier>(),
            MakeIntrusive<TCustomTabletListRenderer>()));
        initializers.AddServiceInitializer(new TStorageServicesInitializer(
            sArgs));
        initializers.AddServiceInitializer(new TCustomLocalServiceInitializer(
            *sArgs.AppConfig,
            sArgs.Logging,
            sArgs.StorageConfig,
            sArgs.DiagnosticsConfig,
            sArgs.ProfileLog,
            sArgs.BlockDigestGenerator,
            sArgs.TraceSerializer,
            sArgs.LogbrokerService,
            sArgs.NotifyService,
            sArgs.RdmaClient,
            sArgs.EndpointEventHandler,
            sArgs.IsDiskRegistrySpareNode));
    };

    auto storageConfig = sArgs.StorageConfig;
    TBasicKikimrServicesMask servicesMask;
    servicesMask.DisableAll();
    servicesMask.EnableBasicServices = 1;
    servicesMask.EnableLogger = 1;
    servicesMask.EnableSchedulerActor = 1;
    servicesMask.EnableProfiler = 1;
    servicesMask.EnableSelfPing = 1;
    servicesMask.EnableRestartsCountPublisher = 1;
    servicesMask.EnableStateStorageService = 1;
    servicesMask.EnableTabletResolver = 1;
    servicesMask.EnableTabletMonitor = 0;   // configured manually
    servicesMask.EnableTabletMonitoringProxy = 1;
    servicesMask.EnableTabletCountersAggregator = 1;
    servicesMask.EnableBSNodeWarden = 1;
    servicesMask.EnableWhiteBoard = 1;
    servicesMask.EnableResourceBroker = 1;
    servicesMask.EnableSharedCache = 1;
    servicesMask.EnableTxProxy = 1;
    servicesMask.EnableIcbService = 1;
    servicesMask.EnableLocalService = 0;    // configured manually
    servicesMask.EnableSchemeBoardMonitoring = 1;
    servicesMask.EnableConfigsDispatcher =
        storageConfig->GetConfigsDispatcherServiceEnabled();
    servicesMask.EnableViewerService =
        storageConfig->GetYdbViewerServiceEnabled();

    auto nodeId = sArgs.NodeId;
    auto onStart = [=] (IActorSystem& actorSystem) {
        if (storageConfig->GetDisableLocalService()) {
            using namespace NNodeWhiteboard;
            const TActorId wb(MakeNodeWhiteboardServiceId(nodeId));
            actorSystem.Send(
                wb,
                std::make_unique<TEvWhiteboard::TEvSystemStateAddRole>(
                    "Tenant"));
            actorSystem.Send(
                wb,
                std::make_unique<TEvWhiteboard::TEvSystemStateSetTenant>(
                    storageConfig->GetSchemeShardDir()));
        }
    };

    TActorSystemArgs args{
        .ModuleFactories = sArgs.ModuleFactories,
        .NodeId = sArgs.NodeId,
        .ScopeId = sArgs.ScopeId,
        .AppConfig = sArgs.AppConfig,
        .AsyncLogger = sArgs.AsyncLogger,
        .OnInitialize = std::move(onInitialize),
        .PrepareKikimrRunConfig = std::move(prepareKikimrRunConfig),
        .ServicesMask = servicesMask,
        .OnStart = std::move(onStart),
    };

    auto actorSystem = MakeIntrusive<TActorSystem>(args);
    actorSystem->Init();
    return actorSystem;
}

}   // namespace NCloud::NBlockStore::NStorage
