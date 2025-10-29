#include "test_env.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/service/service.h>
#include <cloud/filestore/libs/storage/ss_proxy/ss_proxy.h>
#include <cloud/filestore/libs/storage/tablet/tablet.h>
#include <cloud/filestore/libs/storage/tablet_proxy/tablet_proxy.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/hive_proxy/hive_proxy.h>

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSettings(
    const TString& name)
{
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    subdomain.SetCoordinators(0);
    subdomain.SetMediators(0);
    subdomain.SetPlanResolution(50);
    subdomain.SetTimeCastBucketsPerMediator(2);
    return subdomain;
}

ui64 ChangeDomain(ui64 tabletId, ui32 domainUid)
{
    return MakeTabletID(domainUid, domainUid, UniqPartFromTabletID(tabletId));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTestEnv::TTestEnv(
        const TTestEnvConfig& config,
        NProto::TStorageConfig storageConfig,
        NKikimr::NFake::TCaches cachesConfig,
        IProfileLogPtr profileLog,
        NProto::TDiagnosticsConfig diagConfig)
    : Config(config)
    , Logging(CreateLoggingService("console", { TLOG_DEBUG }))
    , ProfileLog(std::move(profileLog))
    , TraceSerializer(
        CreateTraceSerializer(
            Logging,
            "NFS_TRACE",
            NLwTraceMonPage::TraceManager(false)))
    , Runtime(Config.StaticNodes + Config.DynamicNodes, false)
    , NextDynamicNode(Config.StaticNodes)
    , Counters(MakeIntrusive<NMonitoring::TDynamicCounters>())
{
    storageConfig.SetSchemeShardDir(Config.DomainName);

    StorageConfig = CreateTestStorageConfig(std::move(storageConfig));

    DiagConfig = std::make_shared<TDiagnosticsConfig>(std::move(diagConfig));

    TAppPrepare app;
    // app.SetEnableSchemeBoard(true);
    SetupLogging();
    SetupDomain(app);
    app.AddHive(Config.DomainUid, GetHive());
    SetupChannelProfiles(app);
    SetupTabletServices(Runtime, &app, false, {}, std::move(cachesConfig));
    BootTablets();
    SetupStorage();
    SetupLocalServices();
    SetupProxies();

    InitSchemeShard();

    StatsRegistry = CreateRequestStatsRegistry(
        "service",
        DiagConfig,
        Counters,
        CreateWallClockTimer(),
        NCloud::NStorage::NUserStats::CreateUserCounterSupplierStub());

    Registry = NMetrics::CreateMetricsRegistry(
        {NMetrics::CreateLabel("counters", "filestore")},
        Runtime.GetAppData().Counters);
}

ui64 TTestEnv::GetHive()
{
    return ChangeDomain(Tests::Hive, Config.DomainUid);
}

ui64 TTestEnv::GetSchemeShard()
{
    return ChangeDomain(Tests::SchemeRoot, Config.DomainUid);
}

TLog TTestEnv::CreateLog()
{
    return Logging->CreateLog("NFS_TABLET_TEST");
}

ui64 TTestEnv::AllocateTxId()
{
    ui64 tabletId = ChangeDomain(Tests::TxAllocator, Config.DomainUid);

    auto sender = Runtime.AllocateEdgeActor();

    Runtime.SendToPipe(
        tabletId,
        sender,
        new TEvTxAllocator::TEvAllocate(1));

    TAutoPtr<IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<TEvTxAllocator::TEvAllocateResult>(handle);
    UNIT_ASSERT(event);
    UNIT_ASSERT_EQUAL(event->Record.GetStatus(), NKikimrTx::TEvTxAllocateResult::SUCCESS);
    return event->Record.GetRangeBegin();
}

void TTestEnv::CreateSubDomain(const TString& name)
{
    ui64 tabletId = ChangeDomain(Tests::SchemeRoot, Config.DomainUid);
    ui64 txId = AllocateTxId();

    auto evTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(
        txId,
        tabletId);
    auto* tx = evTx->Record.AddTransaction();
    tx->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain);
    tx->SetWorkingDir("/" + Config.DomainName);
    tx->MutableSubDomain()->CopyFrom(GetSubDomainDefaultSettings(name));
    for (const auto& [kind, pool] : Runtime.GetAppData().DomainsInfo->GetDomain(Config.DomainUid).StoragePoolTypes) {
        auto* pbPool = tx->MutableSubDomain()->AddStoragePools();
        pbPool->SetKind(kind);
        pbPool->SetName(pool.GetName());
    }

    auto sender = Runtime.AllocateEdgeActor();
    Runtime.SendToPipe(
        tabletId,
        sender,
        evTx.release());

    TAutoPtr<IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
    UNIT_ASSERT(event);
    UNIT_ASSERT_EQUAL(event->Record.GetTxId(), txId);

    if (event->Record.GetStatus() == NKikimrScheme::StatusAccepted) {
        WaitForSchemeShardTx(txId);
    } else {
        UNIT_ASSERT_EQUAL(event->Record.GetStatus(), NKikimrScheme::StatusAlreadyExists);
    }
}

ui32 TTestEnv::CreateNode(const TString& name)
{
    ui32 nodeIdx = NextDynamicNode++;
    UNIT_ASSERT(nodeIdx < Config.StaticNodes + Config.DynamicNodes);

    auto& appData = Runtime.GetAppData(nodeIdx);

    auto txProxyId = Runtime.Register(
        CreateTxProxy(Runtime.GetTxAllocatorTabletIds()),
        nodeIdx);
    Runtime.EnableScheduleForActor(txProxyId);
    Runtime.RegisterService(MakeTxProxyID(), txProxyId, nodeIdx);

    TLocalConfig::TPtr localConfig = new TLocalConfig();
    SetupLocalServiceConfig(appData, *localConfig);

    TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
    tenantPoolConfig->AddStaticSlot("/" + Config.DomainName + "/" + name);

    auto tenantPoolId = Runtime.Register(
        CreateTenantPool(tenantPoolConfig),
        nodeIdx,
        appData.SystemPoolId,
        TMailboxType::Revolving,
        0);
    Runtime.EnableScheduleForActor(tenantPoolId);
    Runtime.RegisterService(
        MakeTenantPoolID(nodeIdx),
        tenantPoolId,
        nodeIdx);

    auto indexService = CreateStorageService(
        StorageConfig,
        StatsRegistry,
        ProfileLog,
        TraceSerializer,
        CreateStatsFetcherStub());
    auto indexServiceId = Runtime.Register(
        indexService.release(),
        nodeIdx,
        appData.UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(indexServiceId);
    Runtime.RegisterService(
        MakeStorageServiceId(),
        indexServiceId,
        nodeIdx);

    auto tabletProxy = CreateIndexTabletProxy(StorageConfig);
    auto tabletProxyId = Runtime.Register(
        tabletProxy.release(),
        nodeIdx,
        appData.UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(tabletProxyId);
    Runtime.RegisterService(
        MakeIndexTabletProxyServiceId(),
        tabletProxyId,
        nodeIdx);

    auto ssProxy = CreateSSProxy(StorageConfig);
    auto ssProxyId = Runtime.Register(
        ssProxy.release(),
        nodeIdx,
        appData.UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(ssProxyId);
    Runtime.RegisterService(
        MakeSSProxyServiceId(),
        ssProxyId,
        nodeIdx);

    auto hiveProxy = CreateHiveProxy({
        StorageConfig->GetPipeClientRetryCount(),
        StorageConfig->GetPipeClientMinRetryTime(),
        TDuration::Seconds(1),  // HiveLockExpireTimeout - does not matter
        TFileStoreComponents::HIVE_PROXY,
        /*TabletBootInfoBackupFilePath=*/"",
        /*UseBinaryFormatForTabletBootInfoBackup=*/false,
        /*FallbackMode=*/false,
    });
    auto hiveProxyId = Runtime.Register(
        hiveProxy.release(),
        nodeIdx,
        appData.UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(hiveProxyId);
    Runtime.RegisterService(
        MakeHiveProxyServiceId(),
        hiveProxyId,
        nodeIdx);

    if (nodeIdx != 0) {
        // we need node-local proxies as well
        auto localTabletProxy = CreateIndexTabletProxy(StorageConfig);
        auto localTabletProxyId = Runtime.Register(
            localTabletProxy.release(),
            0,
            appData.UserPoolId,
            TMailboxType::Simple,
            0);
        Runtime.EnableScheduleForActor(localTabletProxyId);
        Runtime.RegisterService(
            MakeIndexTabletProxyServiceId(),
            localTabletProxyId,
            0);

    auto localSsProxy = CreateSSProxy(StorageConfig);
    auto localSsProxyId = Runtime.Register(
        localSsProxy.release(),
        0,
        appData.UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(localSsProxyId);
    Runtime.RegisterService(
        MakeSSProxyServiceId(),
        localSsProxyId,
        0);

    auto localHiveProxy = CreateHiveProxy({
        StorageConfig->GetPipeClientRetryCount(),
        StorageConfig->GetPipeClientMinRetryTime(),
        TDuration::Seconds(1),  // HiveLockExpireTimeout - does not matter
        TFileStoreComponents::HIVE_PROXY,
        /*TabletBootInfoBackupFilePath=*/"",
        /*UseBinaryFormatForTabletBootInfoBackup=*/false,
        /*FallbackMode=*/false,
    });
    auto localHiveProxyId = Runtime.Register(
        localHiveProxy.release(),
        0,
        appData.UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(localHiveProxyId);
    Runtime.RegisterService(
        MakeHiveProxyServiceId(),
        localHiveProxyId,
        0);
    }

    return nodeIdx;
}

void TTestEnv::SetupLogging()
{
    Runtime.SetLogPriority(NLog::InvalidComponent, Config.LogPriority_Others);

    auto kikimrServices = {
        NKikimrServices::BS_CONTROLLER,
        NKikimrServices::BS_NODE,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::HIVE,
        NKikimrServices::LOCAL,
        NKikimrServices::TX_COORDINATOR,
        NKikimrServices::TX_MEDIATOR,
        NKikimrServices::TX_PROXY,
        NKikimrServices::TX_PROXY_SCHEME_CACHE,
    };

    for (ui32 i: kikimrServices) {
        Runtime.SetLogPriority(i, Config.LogPriority_KiKiMR);
    }

    Runtime.AppendToLogSettings(
        TFileStoreComponents::START,
        TFileStoreComponents::END,
        GetComponentName);

    for (ui32 i = TFileStoreComponents::START; i < TFileStoreComponents::END; ++i) {
        Runtime.SetLogPriority(i, Config.LogPriority_NFS);
    }
}

void TTestEnv::SetupDomain(TAppPrepare& app)
{
    auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
        Config.DomainName, Config.DomainUid, ChangeDomain(Tests::SchemeRoot, Config.DomainUid),
        Config.DomainUid, Config.DomainUid, TVector<ui32>{Config.DomainUid},
        Config.DomainUid, TVector<ui32>{Config.DomainUid},
        7,
        TVector<ui64>{TDomainsInfo::MakeTxCoordinatorID(Config.DomainUid, 1)},
        TVector<ui64>{TDomainsInfo::MakeTxMediatorID(Config.DomainUid, 1)},
        TVector<ui64>{TDomainsInfo::MakeTxAllocatorID(Config.DomainUid, 1)},
        DefaultPoolKinds(2));

    UNIT_ASSERT_EQUAL(domain->Coordinators.front(), ChangeDomain(Tests::Coordinator, Config.DomainUid));
    UNIT_ASSERT_EQUAL(domain->Mediators.front(), ChangeDomain(Tests::Mediator, Config.DomainUid));
    UNIT_ASSERT_EQUAL(domain->TxAllocators.front(), ChangeDomain(Tests::TxAllocator, Config.DomainUid));

    app.AddDomain(domain.Release());
}

void TTestEnv::SetupChannelProfiles(TAppPrepare& app)
{
    // deprecated
    NKikimr::SetupChannelProfiles(app, Config.DomainUid, Config.ChannelCount);
}

void TTestEnv::BootTablets()
{
    BootStandardTablet(
        ChangeDomain(Tests::TxAllocator, Config.DomainUid),
        TTabletTypes::TxAllocator);
    BootStandardTablet(
        ChangeDomain(Tests::Coordinator, Config.DomainUid),
        TTabletTypes::Coordinator);
    BootStandardTablet(
        ChangeDomain(Tests::Mediator, Config.DomainUid),
        TTabletTypes::Mediator);
    BootStandardTablet(
        ChangeDomain(Tests::SchemeRoot, Config.DomainUid),
        TTabletTypes::SchemeShard);
    BootStandardTablet(
        ChangeDomain(Tests::Hive, Config.DomainUid),
        TTabletTypes::Hive);
    BootStandardTablet(
        MakeBSControllerID(Config.DomainUid),
        TTabletTypes::BSController);
}

void TTestEnv::BootStandardTablet(
    ui64 tabletId,
    TTabletTypes::EType type,
    ui32 nodeIdx)
{
    std::function<IActor* (const TActorId&, TTabletStorageInfo*)> factory;
    switch (type) {
        case TTabletTypes::TxAllocator:
            factory = &CreateTxAllocator;
            break;
        case TTabletTypes::Coordinator:
            factory = &CreateFlatTxCoordinator;
            break;
        case TTabletTypes::Mediator:
            factory = &CreateTxMediator;
            break;
        case TTabletTypes::SchemeShard:
            factory = &CreateFlatTxSchemeShard;
            break;
        case TTabletTypes::Hive:
            factory = &CreateDefaultHive;
            break;
        case TTabletTypes::BSController:
            factory = &CreateFlatBsController;
            break;
        default:
            Y_ABORT("Unexpected tablet type");
    }

    auto bootstrapper = CreateTestBootstrapper(
        Runtime,
        CreateTestTabletInfo(tabletId, type),
        factory,
        nodeIdx);
    Runtime.EnableScheduleForActor(bootstrapper);
}

ui64 TTestEnv::BootIndexTablet(ui32 nodeIdx)
{
    auto tabletId = ChangeDomain(Config.DomainUid, ++NextTabletId);

    auto tabletFactory = [=, this] (const TActorId& owner, TTabletStorageInfo* storage) {
        Y_ABORT_UNLESS(storage->TabletType == TTabletTypes::FileStore);
        auto actor = CreateIndexTablet(
            owner,
            storage,
            StorageConfig,
            DiagConfig,
            ProfileLog,
            TraceSerializer,
            Registry,
            true);
        return actor.release();
    };

    auto* tabletStorageInfo = BuildIndexTabletStorageInfo(
        tabletId,
        Config.ChannelCount).release();

    TabletIdToStorageInfo[tabletId] = tabletStorageInfo;

    auto tablet = CreateTestBootstrapper(
        Runtime,
        tabletStorageInfo,
        tabletFactory,
        nodeIdx);
    Runtime.EnableScheduleForActor(tablet);

    return tabletId;
}

std::unique_ptr<TTabletStorageInfo> TTestEnv::BuildIndexTabletStorageInfo(
    ui64 tabletId,
    ui32 channelCount)
{
    auto x = std::make_unique<TTabletStorageInfo>();

    x->TabletID = tabletId;
    x->Version = 0;
    x->TabletType = TTabletTypes::FileStore;
    x->Channels.resize(channelCount);

    for (ui32 channel = 0; channel < channelCount; ++channel) {
        x->Channels[channel].Channel = channel;
        x->Channels[channel].Type = TBlobStorageGroupType(BootGroupErasure);
        x->Channels[channel].History.resize(1);
        x->Channels[channel].History[0].FromGeneration = 0;
        x->Channels[channel].History[0].GroupID = 0;
    }

    return x;
}

void TTestEnv::UpdateTabletStorageInfo(ui64 tabletId, ui32 channelCount)
{
    auto it = TabletIdToStorageInfo.find(tabletId);
    Y_ABORT_UNLESS(it != TabletIdToStorageInfo.end());

    auto newStorageInfo = BuildIndexTabletStorageInfo(tabletId, channelCount);
    newStorageInfo->Version = it->second->Version + 1;
    *(it->second) = *newStorageInfo;
}

ui64 TTestEnv::GetPrivateCacheSize(ui64 tabletId) {
    return GetExecutorCacheSize(Runtime, tabletId);
}

TString TTestEnv::UpdatePrivateCacheSize(ui64 tabletId, ui64 cacheSize)
{
    NTabletFlatScheme::TSchemeChanges scheme;
    TString err;
    TString change =  Sprintf(R"___(
    Delta {
        DeltaType: UpdateExecutorInfo
        ExecutorCacheSize: %lu
    }
    )___", cacheSize);

    LocalSchemeTx(Runtime, tabletId, change, false, scheme, err);

    return err;
}

void TTestEnv::SetupStorage()
{
    SetupBoxAndStoragePool(
        Runtime,
        Runtime.AllocateEdgeActor(),
        Config.DomainUid,
        Config.Groups
    );

    auto selectGroups =
        std::make_unique<TEvBlobStorage::TEvControllerSelectGroups>();
    const auto& spTypes =
        Runtime.GetAppData().DomainsInfo->GetDomain(Config.DomainUid).StoragePoolTypes;
    for (const auto& x : spTypes) {
        selectGroups->Record.AddGroupParameters()
            ->MutableStoragePoolSpecifier()->SetName(x.second.GetName());
    }
    selectGroups->Record.SetReturnAllMatchingGroups(true);
    Runtime.SendToPipe(
        MakeBSControllerID(Config.DomainUid),
        Runtime.AllocateEdgeActor(),
        selectGroups.release()
    );

    TAutoPtr<IEventHandle> handle;
    Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerSelectGroupsResult>(
        handle,
        TDuration::Seconds(5)
    );
    UNIT_ASSERT(handle);

    std::unique_ptr<TEvBlobStorage::TEvControllerSelectGroupsResult> response(
        handle->Release<TEvBlobStorage::TEvControllerSelectGroupsResult>()
        .Release()
    );

    for (const auto& mg : response->Record.GetMatchingGroups()) {
        for (const auto& g : mg.GetGroups()) {
            GroupIds.push_back(g.GetGroupID());
        }
    }

    UNIT_ASSERT(Config.Groups <= GroupIds.size());
}

void TTestEnv::SetupLocalServices()
{
    for (ui32 nodeIdx = 0; nodeIdx < Config.StaticNodes; ++nodeIdx) {
        SetupLocalService(nodeIdx);
    }
}

void TTestEnv::SetupLocalService(ui32 nodeIdx)
{
    auto& appData = Runtime.GetAppData(nodeIdx);

    TLocalConfig::TPtr localConfig = new TLocalConfig();
    SetupLocalServiceConfig(appData, *localConfig);

    TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
    tenantPoolConfig->AddStaticSlot("/" + Config.DomainName);

    auto tenantPoolActorId = Runtime.Register(
        CreateTenantPool(tenantPoolConfig),
        nodeIdx,
        appData.SystemPoolId,
        TMailboxType::Revolving,
        0);
    Runtime.EnableScheduleForActor(tenantPoolActorId);

    Runtime.RegisterService(
        MakeTenantPoolID(Runtime.GetNodeId(nodeIdx)),
        tenantPoolActorId,
        nodeIdx);
}

void TTestEnv::SetupLocalServiceConfig(
    TAppData& appData,
    TLocalConfig& localConfig)
{
    auto tabletFactory = [=, this] (const TActorId& owner, TTabletStorageInfo* storage) {
        Y_ABORT_UNLESS(storage->TabletType == TTabletTypes::FileStore);
        auto actor = CreateIndexTablet(
            owner,
            storage,
            StorageConfig,
            DiagConfig,
            ProfileLog,
            TraceSerializer,
            Registry,
            true);
        return actor.release();
    };

    localConfig.TabletClassInfo[TTabletTypes::FileStore] =
        TLocalConfig::TTabletClassInfo(
            new TTabletSetupInfo(
                tabletFactory,
                TMailboxType::ReadAsFilled,
                appData.UserPoolId,
                TMailboxType::ReadAsFilled,
                appData.SystemPoolId));
}

void TTestEnv::SetupProxies()
{
    Runtime.SetTxAllocatorTabletIds(
        TVector<ui64>(1, ChangeDomain(Tests::TxAllocator, Config.DomainUid)));

    for (ui32 nodeIdx = 0; nodeIdx < Config.StaticNodes; ++nodeIdx) {
        // SetupTicketParser(nodeIdx);
        SetupTxProxy(nodeIdx);
        // SetupCompileService(nodeIdx);
    }
}

void TTestEnv::SetupTicketParser(ui32 nodeIdx)
{
    IActor* ticketParser = CreateTicketParser(Runtime.GetAppData().AuthConfig);
    TActorId ticketParserId = Runtime.Register(ticketParser, nodeIdx);
    Runtime.EnableScheduleForActor(ticketParserId);
    Runtime.RegisterService(MakeTicketParserID(), ticketParserId, nodeIdx);
}

void TTestEnv::SetupTxProxy(ui32 nodeIdx)
{
    IActor* txProxy = CreateTxProxy(Runtime.GetTxAllocatorTabletIds());
    TActorId txProxyId = Runtime.Register(txProxy, nodeIdx);
    Runtime.EnableScheduleForActor(txProxyId);
    Runtime.RegisterService(MakeTxProxyID(), txProxyId, nodeIdx);
}

void TTestEnv::SetupCompileService(ui32 nodeIdx)
{
    IActor* compileService = CreateMiniKQLCompileService(100000);
    TActorId compileServiceId = Runtime.Register(
        compileService,
        nodeIdx,
        Runtime.GetAppData(0).SystemPoolId,
        TMailboxType::Revolving,
        0);
    Runtime.EnableScheduleForActor(compileServiceId);
    Runtime.RegisterService(
        MakeMiniKQLCompileServiceID(),
        compileServiceId,
        nodeIdx);
}

void TTestEnv::InitSchemeShard()
{
    const ui64 tabletId = ChangeDomain(Tests::SchemeRoot, Config.DomainUid);
    const TDomainsInfo::TDomain& domain = Runtime.GetAppData().DomainsInfo->GetDomain(Config.DomainUid);

    auto evTx = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(1, tabletId);
    auto transaction = evTx->Record.AddTransaction();
    transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain);
    transaction->SetWorkingDir("/");

    auto op = transaction->MutableSubDomain();
    op->SetName(domain.Name);

    for (const auto& [kind, pool] : domain.StoragePoolTypes) {
        auto* p = op->AddStoragePools();
        p->SetKind(kind);
        p->SetName(pool.GetName());
    }

    auto sender = Runtime.AllocateEdgeActor();
    Runtime.SendToPipe(tabletId, sender, evTx.Release(), 0, GetPipeConfigWithRetries());

    {
        TAutoPtr<IEventHandle> handle;
        auto event = Runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSchemeshardId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
    }

    auto evSubscribe = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(1);
    Runtime.SendToPipe(tabletId, sender, evSubscribe.Release(), 0, GetPipeConfigWithRetries());

    {
        TAutoPtr<IEventHandle> handle;
        auto event = Runtime.GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), 1);
    }
}

void TTestEnv::WaitForSchemeShardTx(ui64 txId)
{
    ui64 tabletId = ChangeDomain(Tests::SchemeRoot, Config.DomainUid);

    auto sender = Runtime.AllocateEdgeActor();
    Runtime.SendToPipe(
        tabletId,
        sender,
        new TEvSchemeShard::TEvNotifyTxCompletion(txId));

    TAutoPtr<IEventHandle> handle;
    auto event = Runtime.GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
    UNIT_ASSERT(event);

    ui64 eventTxId = event->Record.GetTxId();
    UNIT_ASSERT_EQUAL(eventTxId, txId);
}

////////////////////////////////////////////////////////////////////////////////

void TTestRegistryVisitor::OnStreamBegin()
{
    CurrentEntry = TMetricsEntry();
    MetricsEntries.clear();
}

void TTestRegistryVisitor::OnStreamEnd()
{}

void TTestRegistryVisitor::OnMetricBegin(
    TInstant time,
    NMetrics::EAggregationType aggrType,
    NMetrics::EMetricType metrType)
{
    CurrentEntry.Time = time;
    CurrentEntry.AggrType = aggrType;
    CurrentEntry.MetrType = metrType;
}

void TTestRegistryVisitor::OnMetricEnd()
{
    MetricsEntries.emplace_back(std::move(CurrentEntry));
}

void TTestRegistryVisitor::OnLabelsBegin()
{}

void TTestRegistryVisitor::OnLabelsEnd()
{}

void TTestRegistryVisitor::OnLabel(TStringBuf name, TStringBuf value)
{
    CurrentEntry.Labels.emplace(TString(name), TString(value));
}

void TTestRegistryVisitor::OnValue(i64 value)
{
    CurrentEntry.Value = value;
}

const TVector<TTestRegistryVisitor::TMetricsEntry>& TTestRegistryVisitor::GetEntries() const
{
    return MetricsEntries;
}

void TTestRegistryVisitor::ValidateExpectedCounters(
    const TVector<std::pair<TVector<TLabel>, i64>>& expectedCounters)
{
    for (const auto& [labels, value]: expectedCounters) {
        const auto labelsStr = LabelsToString(labels);

        int matchingCountersCount = 0;
        for (const auto& entry: MetricsEntries) {
            if (entry.Matches(labels)) {
                ++matchingCountersCount;
                UNIT_ASSERT_VALUES_EQUAL_C(entry.Value, value, labelsStr);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(matchingCountersCount, 1, labelsStr);
    }
}

void TTestRegistryVisitor::ValidateExpectedCountersWithPredicate(
    const TVector<
        std::pair<TVector<TLabel>, std::function<bool(i64)>>
    >& expectedCounters)
{
    for (const auto& [labels, predicate]: expectedCounters) {
        const auto labelsStr = LabelsToString(labels);

        int matchingCountersCount = 0;
        for (const auto& entry: MetricsEntries) {
            if (entry.Matches(labels)) {
                ++matchingCountersCount;
                UNIT_ASSERT(predicate(entry.Value));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(matchingCountersCount, 1, labelsStr);
    }
}

void TTestRegistryVisitor::ValidateExpectedHistogram(
    const TVector<std::pair<TVector<TLabel>, i64>>& expectedCounters,
    bool checkEqual)
{
    for (const auto& [labels, value]: expectedCounters) {
        const auto labelsStr = LabelsToString(labels);
        i64 total = 0;

        int matchingCountersCount = 0;
        for (const auto& entry: MetricsEntries) {
            if (entry.Matches(labels)) {
                ++matchingCountersCount;
                total += entry.Value;
            }
        }
        if (checkEqual) {
            UNIT_ASSERT_VALUES_EQUAL_C(total, value, labelsStr);
        } else {
            UNIT_ASSERT_VALUES_UNEQUAL_C(total, value, labelsStr);
        }
        UNIT_ASSERT_VALUES_UNEQUAL_C(matchingCountersCount, 0, labelsStr);
    }
}

TString TTestRegistryVisitor::LabelsToString(const TVector<TLabel>& labels)
{
    TStringBuilder labelsStr;
    for (const auto& label: labels) {
        if (labelsStr) {
            labelsStr << ", ";
        }
        labelsStr << label.GetName() << "=" << label.GetValue();
    }

    return labelsStr;
}

////////////////////////////////////////////////////////////////////////////////

void CheckForkJoin(const NLWTrace::TShuttleTrace& trace, bool forkRequired)
{
    UNIT_ASSERT(trace.GetEvents().size() > 0);

    ui32 forkBudget = 0;
    bool forkSeen = false;
    for (const auto& event: trace.GetEvents()) {
        if (event.GetName() == "Fork") {
            ++forkBudget;
            forkSeen = true;
        } else if (event.GetName() == "Join") {
            UNIT_ASSERT(forkBudget > 0);
            --forkBudget;
        }
    }

    UNIT_ASSERT(!forkRequired || forkSeen);
    UNIT_ASSERT_VALUES_EQUAL(0, forkBudget);
}

////////////////////////////////////////////////////////////////////////////////

TStorageConfigPtr CreateTestStorageConfig(NProto::TStorageConfig storageConfig)
{
    if (!storageConfig.GetSchemeShardDir()) {
        storageConfig.SetSchemeShardDir("local");
    }

    storageConfig.SetHDDSystemChannelPoolKind("pool-kind-1");
    storageConfig.SetHDDLogChannelPoolKind("pool-kind-1");
    storageConfig.SetHDDIndexChannelPoolKind("pool-kind-1");
    storageConfig.SetHDDFreshChannelPoolKind("pool-kind-1");
    storageConfig.SetHDDMixedChannelPoolKind("pool-kind-1");

    storageConfig.SetSSDSystemChannelPoolKind("pool-kind-2");
    storageConfig.SetSSDLogChannelPoolKind("pool-kind-2");
    storageConfig.SetSSDIndexChannelPoolKind("pool-kind-2");
    storageConfig.SetSSDFreshChannelPoolKind("pool-kind-2");
    storageConfig.SetSSDMixedChannelPoolKind("pool-kind-2");

    storageConfig.SetHybridSystemChannelPoolKind("pool-kind-2");
    storageConfig.SetHybridLogChannelPoolKind("pool-kind-2");
    storageConfig.SetHybridIndexChannelPoolKind("pool-kind-2");
    storageConfig.SetHybridFreshChannelPoolKind("pool-kind-2");
    storageConfig.SetHybridMixedChannelPoolKind("pool-kind-1");

    return std::make_shared<TStorageConfig>(storageConfig);
}

}   // namespace NCloud::NFileStore::NStorage
