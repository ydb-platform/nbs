#include "actorsystem.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/blockstore/libs/storage/init/common/actorsystem.h>
#include <cloud/blockstore/libs/storage/stats_fetcher/stats_fetcher.h>
#include <cloud/blockstore/libs/storage/undelivered/undelivered.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/hive_proxy/hive_proxy.h>
#include <cloud/storage/core/libs/kikimr/config_dispatcher_helpers.h>
#include <cloud/storage/core/libs/kikimr/kikimr_initializer.h>

#include <contrib/ydb/core/grpc_services/grpc_request_proxy.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoring;

using namespace NKikimr;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageServicesInitializer final
    : public IServiceInitializer
{
private:
    const TDiskAgentActorSystemArgs Args;

public:
    TStorageServicesInitializer(const TDiskAgentActorSystemArgs& args)
        : Args(args)
    {}

    void InitializeServices(
        TActorSystemSetup* setup,
        const TAppData* appData) override
    {
        Args.StorageConfig->Register(*appData->Icb);

        //
        // HiveProxy
        //

        auto hiveProxy = CreateHiveProxy(
            {
                .PipeClientRetryCount = Args.StorageConfig->GetPipeClientRetryCount(),
                .PipeClientMinRetryTime = Args.StorageConfig->GetPipeClientMinRetryTime(),
                .HiveLockExpireTimeout = Args.StorageConfig->GetHiveLockExpireTimeout(),
                .LogComponent = TBlockStoreComponents::HIVE_PROXY,
                .TabletBootInfoBackupFilePath = {},
                .FallbackMode = false,
                .TenantHiveTabletId = Args.StorageConfig->GetTenantHiveTabletId(),
            },
            appData
                ->Counters
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service"));

        setup->LocalServices.emplace_back(
            MakeHiveProxyServiceId(),
            TActorSetupCmd(
                hiveProxy.release(),
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
        // DiskAgent
        //

        if (Args.DiskAgentConfig->GetEnabled()) {
            auto diskAgent = CreateDiskAgent(
                Args.StorageConfig,
                Args.DiskAgentConfig,
                Args.RdmaConfig,
                Args.Spdk,
                Args.Allocator,
                Args.LocalStorageProvider,
                Args.ProfileLog,
                Args.BlockDigestGenerator,
                Args.Logging,
                Args.RdmaServer,
                Args.NvmeManager);

            setup->LocalServices.emplace_back(
                MakeDiskAgentServiceId(Args.NodeId),
                TActorSetupCmd(
                    diskAgent.release(),
                    TMailboxType::TinyReadAsFilled,
                    appData->UserPoolId));
        }

        //
        // StatsFetcher
        //

        auto statsFetcher = CreateStatsFetcherActor(
            Args.StorageConfig,
            Args.StatsFetcher);

        setup->LocalServices.emplace_back(
            TActorId(),
            TActorSetupCmd(
                statsFetcher.release(),
                TMailboxType::TinyReadAsFilled,
                appData->BatchPoolId));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorSystemPtr CreateDiskAgentActorSystem(const TDiskAgentActorSystemArgs& daArgs)
{
    TVector<TIntrusivePtr<IServiceInitializer>> initializers = {
        new TStorageServicesInitializer(daArgs)
    };
    auto prepareKikimrRunConfig = [&] (TKikimrRunConfig& runConfig) {
        if (daArgs.StorageConfig->GetConfigsDispatcherServiceEnabled()) {
            SetupConfigDispatcher(
                daArgs.StorageConfig->GetConfigDispatcherSettings(),
                daArgs.StorageConfig->GetSchemeShardDir(),
                daArgs.StorageConfig->GetNodeType(),
                &runConfig.ConfigsDispatcherInitInfo);
            runConfig.ConfigsDispatcherInitInfo.InitialConfig = runConfig.AppConfig;
        }
    };
    auto onInitialize = [&] (
        TKikimrRunConfig& runConfig,
        TServiceInitializersList& initializers)
    {
        Y_UNUSED(runConfig);
        initializers.AddServiceInitializer(
            new NStorage::TKikimrServicesInitializer(daArgs.AppConfig));
        initializers.AddServiceInitializer(new TStorageServicesInitializer(
            daArgs));
    };

    // TODO: disable the services that are not needed by disk agent
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
    servicesMask.EnableConfigsDispatcher =
        daArgs.StorageConfig->GetConfigsDispatcherServiceEnabled();
    servicesMask.EnableViewerService =
        daArgs.StorageConfig->GetYdbViewerServiceEnabled();

    TActorSystemArgs args{
        .ModuleFactories = daArgs.ModuleFactories,
        .NodeId = daArgs.NodeId,
        .ScopeId = daArgs.ScopeId,
        .AppConfig = daArgs.AppConfig,
        .AsyncLogger = daArgs.AsyncLogger,
        .OnInitialize = std::move(onInitialize),
        .PrepareKikimrRunConfig = std::move(prepareKikimrRunConfig),
        .ServicesMask = servicesMask,
        .OnStart = [] (IActorSystem&) {},
    };

    auto actorSystem = MakeIntrusive<TActorSystem>(args);
    actorSystem->Init();
    return actorSystem;
}

}   // namespace NCloud::NBlockStore::NStorage
