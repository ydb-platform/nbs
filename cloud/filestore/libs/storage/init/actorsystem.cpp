#include "actorsystem.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/diagnostics/metrics/service.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/service/service.h>
#include <cloud/filestore/libs/storage/ss_proxy/ss_proxy.h>
#include <cloud/filestore/libs/storage/tablet/tablet.h>
#include <cloud/filestore/libs/storage/tablet_proxy/tablet_proxy.h>

#include <cloud/storage/core/libs/api/authorizer.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/api/user_stats.h>
#include <cloud/storage/core/libs/auth/authorizer.h>
#include <cloud/storage/core/libs/common/timer.h>
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
#include <contrib/ydb/core/mind/labels_maintainer.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/core/mind/tenant_pool.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/tablet/node_tablet_monitor.h>
#include <contrib/ydb/core/tablet/tablet_list_renderer.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NMonitoring;

using namespace NKikimr;
using namespace NKikimr::NKikimrServicesInitializers;
using namespace NKikimr::NNodeTabletMonitor;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageServicesInitializer final
    : public IServiceInitializer
{
private:
    const TActorSystemArgs Args;
    IRequestStatsRegistryPtr StatsRegistry;

public:
    TStorageServicesInitializer(
            const TActorSystemArgs& args,
            IRequestStatsRegistryPtr statsRegistry)
        : Args{args}
        , StatsRegistry{std::move(statsRegistry)}
    {}

    void InitializeServices(
        TActorSystemSetup* setup,
        const TAppData* appData) override
    {
        //
        // StorageService
        //

        auto indexService = CreateStorageService(
            Args.StorageConfig,
            StatsRegistry,
            Args.ProfileLog,
            Args.TraceSerializer,
            Args.StatsFetcher);

        setup->LocalServices.emplace_back(
            MakeStorageServiceId(),
            TActorSetupCmd(
                indexService.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // IndexTabletProxy
        //

        auto tabletProxy = CreateIndexTabletProxy(Args.StorageConfig);

        setup->LocalServices.emplace_back(
            MakeIndexTabletProxyServiceId(),
            TActorSetupCmd(
                tabletProxy.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

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
                .PipeClientRetryCount = Args.StorageConfig->GetPipeClientRetryCount(),
                .PipeClientMinRetryTime = Args.StorageConfig->GetPipeClientMinRetryTime(),
                // HiveLockExpireTimeout, used by NBS, doesn't matter
                .HiveLockExpireTimeout = TDuration::Seconds(1),
                .LogComponent = TFileStoreComponents::HIVE_PROXY,
                .TabletBootInfoBackupFilePath = Args.StorageConfig->GetTabletBootInfoBackupFilePath(),
                .FallbackMode = Args.StorageConfig->GetHiveProxyFallbackMode(),
                .TenantHiveTabletId = Args.StorageConfig->GetTenantHiveTabletId(),
            },
            appData
                ->Counters
                ->GetSubgroup("counters", "filestore")
                ->GetSubgroup("component", "service"));

        setup->LocalServices.emplace_back(
            MakeHiveProxyServiceId(),
            TActorSetupCmd(
                hiveProxy.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));

        //
        // StorageUserStats
        //

        auto storageUserStats = NUserStats::CreateStorageUserStats(
            TFileStoreComponents::USER_STATS,
            "filestore",
            "FileStore",
            {Args.UserCounters});

        setup->LocalServices.emplace_back(
            NCloud::NStorage::MakeStorageUserStatsId(),
            TActorSetupCmd(
                storageUserStats.release(),
                TMailboxType::Revolving,
                appData->BatchPoolId));

        //
        // Authorizer
        //

        auto authorizer = CreateAuthorizerActor(
            TFileStoreComponents::AUTH,
            "filestore",
            Args.StorageConfig->GetFolderId(),
            Args.StorageConfig->GetAuthorizationMode(),
            Args.AppConfig->HasAuthConfig());

        setup->LocalServices.emplace_back(
            MakeAuthorizerServiceId(),
            TActorSetupCmd(
                authorizer.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCustomLocalServiceInitializer final
    : public IServiceInitializer
{
private:
    const TActorSystemArgs Args;
    const NMetrics::IMetricsRegistryPtr MetricsRegistry;

public:
    TCustomLocalServiceInitializer(
            const TActorSystemArgs& args,
            NMetrics::IMetricsRegistryPtr metricsRegistry)
        : Args(args)
        , MetricsRegistry(std::move(metricsRegistry))
    {}

    void InitializeServices(
        TActorSystemSetup* setup,
        const TAppData* appData) override
    {
        auto config = Args.StorageConfig;
        auto diagConfig = Args.DiagnosticsConfig;

        auto tabletFactory =
            [config,
             diagConfig,
             profileLog = Args.ProfileLog,
             traceSerializer = Args.TraceSerializer,
             metricsRegistry = MetricsRegistry] (
                const TActorId& owner,
                TTabletStorageInfo* storage)
            {
                Y_ABORT_UNLESS(storage->TabletType == TTabletTypes::FileStore);
                bool useNoneCompactionPolicy = true;
                if (config->GetNewLocalDBCompactionPolicyEnabled()) {
                    useNoneCompactionPolicy = false;
                }
                auto actor = CreateIndexTablet(
                    owner,
                    storage,
                    config,
                    diagConfig,
                    std::move(profileLog),
                    std::move(traceSerializer),
                    std::move(metricsRegistry),
                    useNoneCompactionPolicy);
                return actor.release();
            };

        TLocalConfig::TPtr localConfig = new TLocalConfig();

        TTabletTypes::EType tabletType = TTabletTypes::FileStore;
        if (config->GetDisableLocalService()) {
            // prevent tablet start via tablet types filter inside local service
            // empty filter == all, so configure invalid tablet type to prevent any
            // it allows to properly register in system and not to break things e.g. Viewer
            tabletType = TTabletTypes::TypeInvalid;
        } else {
            ConfigureTenantSystemTablets(*appData, *localConfig, false);
        }

        localConfig->TabletClassInfo[tabletType] =
            TLocalConfig::TTabletClassInfo(
                new TTabletSetupInfo(
                    std::move(tabletFactory),
                    TMailboxType::ReadAsFilled,
                    appData->UserPoolId,
                    TMailboxType::ReadAsFilled,
                    appData->SystemPoolId));

        TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
        tenantPoolConfig->AddStaticSlot(Args.StorageConfig->GetSchemeShardDir());

        setup->LocalServices.emplace_back(
            MakeTenantPoolRootID(),
            TActorSetupCmd(
                CreateTenantPool(tenantPoolConfig),
                TMailboxType::ReadAsFilled,
                appData->SystemPoolId));

        setup->LocalServices.emplace_back(
            TActorId(),
            TActorSetupCmd(
                CreateLabelsMaintainer(Args.AppConfig->GetMonitoringConfig()),
                TMailboxType::ReadAsFilled,
                appData->SystemPoolId));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TActorSystem final
    : public TKikimrRunner
    , public IActorSystem
{
private:
    const TActorSystemArgs Args;

    // in case KiKiMR monitoring not configured
    TIntrusivePtr<TIndexMonPage> IndexMonPage = new TIndexMonPage("", "");

public:
    TActorSystem(const TActorSystemArgs& args)
        : TKikimrRunner(args.ModuleFactories)
        , Args(args)
    {}

    void Init();

    void Start() override;
    void Stop() override;

    TActorId Register(IActorPtr actor, TStringBuf executorName) override;
    bool Send(const TActorId& recipient, IEventBasePtr event) override;

    TLog CreateLog(const TString& component) override;

    IMonPagePtr RegisterIndexPage(const TString& path, const TString& title) override;
    void RegisterMonPage(IMonPagePtr page) override;
    IMonPagePtr GetMonPage(const TString& path) override;
    TDynamicCountersPtr GetCounters() override;

    TProgramShouldContinue& GetProgramShouldContinue() override;
};

////////////////////////////////////////////////////////////////////////////////

void TActorSystem::Init()
{
    TKikimrRunConfig runConfig(
        *Args.AppConfig,
        Args.NodeId,
        TKikimrScopeId(Args.ScopeId));

    runConfig.AppConfig.MutableMonitoringConfig()->SetRedirectMainPageTo("");

    InitializeRegistries(runConfig);
    InitializeMonitoring(runConfig);
    InitializeAppData(runConfig);
    InitializeLogSettings(runConfig);

    if (Args.StorageConfig->GetConfigsDispatcherServiceEnabled()) {
        SetupConfigDispatcher(
            Args.StorageConfig->GetConfigDispatcherSettings(),
            Args.StorageConfig->GetSchemeShardDir(),
            Args.StorageConfig->GetNodeType(),
            &runConfig.ConfigsDispatcherInitInfo);
        runConfig.ConfigsDispatcherInitInfo.InitialConfig = runConfig.AppConfig;
    }

    LogSettings->Append(
        TFileStoreComponents::START,
        TFileStoreComponents::END,
        GetComponentName);

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
    servicesMask.EnableTabletMonitor = 1;
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
        Args.StorageConfig->GetConfigsDispatcherServiceEnabled();
    servicesMask.EnableViewerService =
        Args.StorageConfig->GetYdbViewerServiceEnabled();

    if (Args.AppConfig->HasAuthConfig()) {
        servicesMask.EnableSecurityServices = 1;
    }

#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
    servicesMask.EnableStatsCollector = 1;
#endif

    Args.Metrics->SetupCounters(AppData->Counters);

    auto statsRegistry = CreateRequestStatsRegistry(
        "service",
        Args.DiagnosticsConfig,
        FILESTORE_COUNTERS_ROOT(AppData->Counters),
        CreateWallClockTimer(),
        NUserStats::CreateUserCounterSupplierStub());
    auto services = CreateServiceInitializersList(runConfig, servicesMask);
    services->AddServiceInitializer(
        new NStorage::TKikimrServicesInitializer(Args.AppConfig));
    services->AddServiceInitializer(
        new TStorageServicesInitializer(Args, std::move(statsRegistry)));
    services->AddServiceInitializer(
        new TCustomLocalServiceInitializer(Args, Args.Metrics->GetRegistry()));

    InitializeActorSystem(runConfig, services, servicesMask);
}

void TActorSystem::Start()
{
    KikimrStart();
}

void TActorSystem::Stop()
{
    KikimrStop(false);
}

TActorId TActorSystem::Register(IActorPtr actor, TStringBuf executorName)
{
    ui32 id = AppData->UserPoolId;
    if (executorName) {
        if (auto it = AppData->ServicePools.find(executorName);
            it != AppData->ServicePools.end())
        {
            id = it->second;
        }
    }

    return ActorSystem->Register(
        actor.release(),
        TMailboxType::Simple,
        id);
}

bool TActorSystem::Send(const TActorId& recipient, IEventBasePtr event)
{
    return ActorSystem->Send(recipient, event.release());
}

TLog TActorSystem::CreateLog(const TString& componentName)
{
    if (LogBackend) {
        TLogSettings logSettings;
        logSettings.UseLocalTimestamps = LogSettings->UseLocalTimestamps;
        logSettings.SuppressNewLine = true;   // kikimr will do it for us

        auto component = LogSettings->FindComponent(componentName);
        if (component != NLog::InvalidComponent) {
            auto settings = LogSettings->GetComponentSettings(component);
            logSettings.FiltrationLevel = static_cast<ELogPriority>(settings.Raw.X.Level);
        } else {
            logSettings.FiltrationLevel = static_cast<ELogPriority>(LogSettings->DefPriority);
        }

        return CreateComponentLog(componentName, LogBackend, Args.AsyncLogger, logSettings);
    }

    return {};
}

IMonPagePtr TActorSystem::RegisterIndexPage(
    const TString& path,
    const TString& title)
{
    if (Monitoring) {
        return Monitoring->RegisterIndexPage(path, title);
    } else {
        return IndexMonPage->RegisterIndexPage(path, title);
    }
}

void TActorSystem::RegisterMonPage(IMonPagePtr page)
{
    if (Monitoring) {
        Monitoring->Register(page.Release());
    } else {
        IndexMonPage->Register(page.Release());
    }
}

IMonPagePtr TActorSystem::GetMonPage(const TString& path)
{
    if (Monitoring) {
        return Monitoring->FindPage(path);
    } else {
        return IndexMonPage->FindPage(path);
    }
}

TDynamicCountersPtr TActorSystem::GetCounters()
{
    return Counters;
}

TProgramShouldContinue& TActorSystem::GetProgramShouldContinue()
{
    return TKikimrRunner::KikimrShouldContinue;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorSystemPtr CreateActorSystem(const TActorSystemArgs& args)
{
    auto actorSystem = MakeIntrusive<TActorSystem>(args);
    actorSystem->Init();
    return actorSystem;
}

}   // namespace NCloud::NFileStore::NStorage
