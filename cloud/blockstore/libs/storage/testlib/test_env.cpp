#include "test_env.h"

#include "test_runtime.h"

#include "disk_registry_proxy_mock.h"
#include "root_kms_key_provider_mock.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/discovery/discovery.h>
#include <cloud/blockstore/libs/endpoints/endpoint_events.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/api/volume_balancer.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/manually_preempted_volumes.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/service/service.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service.h>
#include <cloud/blockstore/libs/storage/ss_proxy/ss_proxy.h>
#include <cloud/blockstore/libs/storage/undelivered/undelivered.h>
#include <cloud/blockstore/libs/storage/volume/volume.h>
#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer.h>
#include <cloud/blockstore/libs/storage/volume_proxy/volume_proxy.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/hive_proxy/hive_proxy.h>

#include <contrib/ydb/core/blobstorage/base/blobstorage_events.h>
#include <contrib/ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <contrib/ydb/core/mind/bscontroller/bsc.h>
#include <contrib/ydb/core/mind/hive/hive.h>
#include <contrib/ydb/core/mind/tenant_pool.h>
#include <contrib/ydb/core/mind/tenant_pool.h>
#include <contrib/ydb/core/security/ticket_parser.h>
#include <contrib/ydb/core/tx/coordinator/coordinator.h>
#include <contrib/ydb/core/tx/mediator/mediator.h>
#include <contrib/ydb/core/tx/schemeshard/schemeshard.h>
#include <contrib/ydb/core/tx/tx_allocator/txallocator.h>
#include <contrib/ydb/core/tx/tx_proxy/proxy.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

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
        ui32 staticNodes,
        ui32 dynamicNodes,
        ui32 nchannels,
        ui32 ngroups,
        TTestEnvState state,
        NKikimr::NFake::TCaches cachesConfig)
    : DomainUid(1)
    , DomainName("local")
    , StaticNodeCount(staticNodes)
    , DynamicNodeCount(dynamicNodes)
    , State(std::move(state))
    , Runtime(staticNodes + dynamicNodes, false)
{
    NextDynamicNode = StaticNodeCount;

    TAppPrepare app;
    SetupLogging();
    SetupDomain(app);
    app.AddHive(DomainUid, ChangeDomain(Tests::Hive, DomainUid));
    SetupChannelProfiles(app, nchannels);
    SetupTabletServices(Runtime, &app, false, {}, std::move(cachesConfig));
    BootTablets();
    SetupStorage(ngroups);
    SetupLocalServices();
    SetupProxies();

    InitSchemeShard();

    TraceSerializer = CreateTraceSerializerStub();
    TraceSerializer->Start();
}

TTestEnv::~TTestEnv()
{
}

ui64 TTestEnv::GetHive()
{
    return ChangeDomain(Tests::Hive, DomainUid);
}

ui64 TTestEnv::GetSchemeShard()
{
    return ChangeDomain(Tests::SchemeRoot, DomainUid);
}

ui64 TTestEnv::AllocateTxId()
{
    ui64 tabletId = ChangeDomain(Tests::TxAllocator, DomainUid);

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
    ui64 tabletId = ChangeDomain(Tests::SchemeRoot, DomainUid);
    ui64 txId = AllocateTxId();

    auto evTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(
        txId,
        tabletId);
    auto* tx = evTx->Record.AddTransaction();
    tx->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain);
    tx->SetWorkingDir("/" + DomainName);
    tx->MutableSubDomain()->CopyFrom(GetSubDomainDefaultSettings(name));
    for (const auto& [kind, pool] : Runtime.GetAppData().DomainsInfo->GetDomain(DomainUid).StoragePoolTypes) {
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

ui32 TTestEnv::CreateBlockStoreNode(
    const TString& name,
    TStorageConfigPtr storageConfig,
    TDiagnosticsConfigPtr diagnosticsConfig)
{
    return CreateBlockStoreNode(
        name,
        std::move(storageConfig),
        std::move(diagnosticsConfig),
        NYdbStats::CreateVolumesStatsUploaderStub(),
        CreateManuallyPreemptedVolumes());
}

ui32 TTestEnv::CreateBlockStoreNode(
    const TString& name,
    TStorageConfigPtr storageConfig,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TManuallyPreemptedVolumesPtr manuallyPreemptedVolumes)
{
    return CreateBlockStoreNode(
        name,
        std::move(storageConfig),
        std::move(diagnosticsConfig),
        NYdbStats::CreateVolumesStatsUploaderStub(),
        std::move(manuallyPreemptedVolumes));
}

ui32 TTestEnv::CreateBlockStoreNode(
    const TString& name,
    TStorageConfigPtr storageConfig,
    TDiagnosticsConfigPtr diagnosticsConfig,
    NYdbStats::IYdbVolumesStatsUploaderPtr ydbStatsUploader,
    TManuallyPreemptedVolumesPtr manuallyPreemptedVolumes)
{
    ui32 nodeIdx = NextDynamicNode++;
    UNIT_ASSERT(nodeIdx < StaticNodeCount + DynamicNodeCount);

    auto* appData = &Runtime.GetAppData(nodeIdx);

    TActorId txProxyId = Runtime.Register(
        CreateTxProxy(Runtime.GetTxAllocatorTabletIds()),
        nodeIdx);
    Runtime.EnableScheduleForActor(txProxyId);
    Runtime.RegisterService(MakeTxProxyID(), txProxyId, nodeIdx);

    auto subDomainPath = "/local/" + name;

    auto factory = [=, this] (const TActorId& owner, TTabletStorageInfo* storage) {
        UNIT_ASSERT_EQUAL(storage->TabletType, TTabletTypes::BlockStoreVolume);
        auto actor = CreateVolumeTablet(
            owner,
            storage,
            storageConfig,
            diagnosticsConfig,
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            TraceSerializer,
            nullptr,   // RdmaClient
            NServer::CreateEndpointEventProxy(),
            EVolumeStartMode::ONLINE,
            {}   // diskId
        );
        return actor.release();
    };

    TLocalConfig::TPtr localConfig = new TLocalConfig();
    localConfig->TabletClassInfo[TTabletTypes::BlockStoreVolume] =
        TLocalConfig::TTabletClassInfo(
            new TTabletSetupInfo(
                factory,
                TMailboxType::ReadAsFilled,
                appData->UserPoolId,
                TMailboxType::ReadAsFilled,
                appData->SystemPoolId));

    TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
    tenantPoolConfig->AddStaticSlot(subDomainPath);

    auto tenantPoolId = Runtime.Register(
        CreateTenantPool(tenantPoolConfig),
        nodeIdx,
        appData->SystemPoolId,
        TMailboxType::Revolving,
        0);
    Runtime.EnableScheduleForActor(tenantPoolId);
    Runtime.RegisterService(
        MakeTenantPoolID(nodeIdx),
        tenantPoolId,
        nodeIdx);

    auto ssProxy = CreateSSProxy(storageConfig);
    auto ssProxyId = Runtime.Register(
        ssProxy.release(),
        nodeIdx,
        appData->UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(ssProxyId);
    Runtime.RegisterService(
        MakeSSProxyServiceId(),
        ssProxyId,
        nodeIdx);

    auto hiveProxy = CreateHiveProxy({
        storageConfig->GetPipeClientRetryCount(),
        storageConfig->GetPipeClientMinRetryTime(),
        storageConfig->GetHiveLockExpireTimeout(),
        TBlockStoreComponents::HIVE_PROXY,
        "",     // TabletBootInfoBackupFilePath
        false,  // FallbackMode
    });
    auto hiveProxyId = Runtime.Register(
        hiveProxy.release(),
        nodeIdx,
        appData->UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(hiveProxyId);
    Runtime.RegisterService(
        MakeHiveProxyServiceId(),
        hiveProxyId,
        nodeIdx);

    if (State.DiskRegistryState->Devices.empty()) {
        // 256GiB should be enough for all uts
        for (ui32 i = 0; i < 2048; ++i) {
            auto& device = State.DiskRegistryState->Devices.emplace_back();
            device.SetNodeId(i / 64);
            device.SetBlocksCount(DefaultDeviceBlockCount);
            device.SetDeviceUUID(Sprintf("uuid%u", i));
            device.SetDeviceName(Sprintf("dev%u", i % 64));
            device.SetTransportId(Sprintf("transport%u", i));
            device.SetBlockSize(DefaultDeviceBlockSize);
            device.MutableRdmaEndpoint()->SetHost(Sprintf("rdma%u", i));
            device.MutableRdmaEndpoint()->SetPort(10020);
            device.SetPhysicalOffset(100500);
        }
    }

    auto drProxy = std::make_unique<TDiskRegistryProxyMock>(
        State.DiskRegistryState);
    auto drProxyId = Runtime.Register(
        drProxy.release(),
        nodeIdx,
        appData->UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(drProxyId);
    Runtime.RegisterService(
        MakeDiskRegistryProxyServiceId(),
        drProxyId,
        nodeIdx);

    auto volumeProxy = CreateVolumeProxy(storageConfig, TraceSerializer);
    auto volumeProxyId = Runtime.Register(
        volumeProxy.release(),
        nodeIdx,
        appData->UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(volumeProxyId);
    Runtime.RegisterService(
        MakeVolumeProxyServiceId(),
        volumeProxyId,
        nodeIdx);

    auto storageStatsService = CreateStorageStatsService(
        storageConfig,
        diagnosticsConfig,
        std::move(ydbStatsUploader),
        CreateStatsAggregatorStub());
    auto storageStatsServiceId = Runtime.Register(
        storageStatsService.release(),
        nodeIdx,
        appData->BatchPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(storageStatsServiceId);
    Runtime.RegisterService(
        MakeStorageStatsServiceId(),
        storageStatsServiceId,
        nodeIdx);

    auto storageService = CreateStorageService(
        storageConfig,
        diagnosticsConfig,
        CreateProfileLogStub(),
        CreateBlockDigestGeneratorStub(),
        NDiscovery::CreateDiscoveryServiceStub(),
        TraceSerializer,
        NServer::CreateEndpointEventProxy(),
        nullptr,   // rdmaClient
        CreateVolumeStatsStub(),
        std::move(manuallyPreemptedVolumes),
        CreateRootKmsKeyProviderMock(),
        false   // temporaryServer
    );

    auto storageServiceId = Runtime.Register(
        storageService.release(),
        nodeIdx,
        appData->UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(storageServiceId);
    Runtime.RegisterService(
        MakeStorageServiceId(),
        storageServiceId,
        nodeIdx);

    auto undeliveredHandler = CreateUndeliveredHandler();
    auto undeliveredHandlerId = Runtime.Register(
        undeliveredHandler.release(),
        nodeIdx,
        appData->UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(undeliveredHandlerId);
    Runtime.RegisterService(
        MakeUndeliveredHandlerServiceId(),
        undeliveredHandlerId,
        nodeIdx);

    auto volumeBalancerService = CreateVolumeBalancerActorStub();
    auto volumeBalancerServiceId = Runtime.Register(
        volumeBalancerService.release(),
        nodeIdx,
        appData->UserPoolId,
        TMailboxType::Simple,
        0);
    Runtime.EnableScheduleForActor(volumeBalancerServiceId);
    Runtime.RegisterService(
        MakeVolumeBalancerServiceId(),
        volumeBalancerServiceId,
        nodeIdx);

    return nodeIdx;
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

void TTestEnv::SetupLogging()
{
    Runtime.AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END;
         ++i)
    {
        Runtime.SetLogPriority(i, NLog::PRI_INFO);
    }

    // Runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    // Runtime.SetLogPriority(NKikimrServices::WILSON, NLog::PRI_DEBUG);
    // Runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
    // Runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_DEBUG);
    // Runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);
    // Runtime.SetLogPriority(NKikimrServices::LOCAL, NLog::PRI_DEBUG);
    // Runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR, NLog::PRI_DEBUG);
    // Runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_DEBUG);
    // Runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
    // Runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NLog::PRI_DEBUG);
    Runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_ERROR);
}

void TTestEnv::SetupDomain(TAppPrepare& app)
{
    auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
        DomainName, DomainUid, ChangeDomain(Tests::SchemeRoot, DomainUid),
        DomainUid, DomainUid, TVector<ui32>{DomainUid},
        DomainUid, TVector<ui32>{DomainUid},
        7,
        TVector<ui64>{TDomainsInfo::MakeTxCoordinatorID(DomainUid, 1)},
        TVector<ui64>{TDomainsInfo::MakeTxMediatorID(DomainUid, 1)},
        TVector<ui64>{TDomainsInfo::MakeTxAllocatorID(DomainUid, 1)},
        DefaultPoolKinds(2));

    UNIT_ASSERT_EQUAL(domain->Coordinators.front(), ChangeDomain(Tests::Coordinator, DomainUid));
    UNIT_ASSERT_EQUAL(domain->Mediators.front(), ChangeDomain(Tests::Mediator, DomainUid));
    UNIT_ASSERT_EQUAL(domain->TxAllocators.front(), ChangeDomain(Tests::TxAllocator, DomainUid));

    app.AddDomain(domain.Release());
}

void TTestEnv::SetupChannelProfiles(TAppPrepare& app, ui32 nchannels)
{
    NKikimr::SetupChannelProfiles(app, DomainUid, nchannels);
}

void TTestEnv::BootTablets()
{
    BootStandardTablet(
        ChangeDomain(Tests::TxAllocator, DomainUid),
        TTabletTypes::TxAllocator);
    BootStandardTablet(
        ChangeDomain(Tests::Coordinator, DomainUid),
        TTabletTypes::Coordinator);
    BootStandardTablet(
        ChangeDomain(Tests::Mediator, DomainUid),
        TTabletTypes::Mediator);
    BootStandardTablet(
        ChangeDomain(Tests::SchemeRoot, DomainUid),
        TTabletTypes::SchemeShard);
    BootStandardTablet(
        ChangeDomain(Tests::Hive, DomainUid),
        TTabletTypes::Hive);
    BootStandardTablet(
        MakeBSControllerID(DomainUid),
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

void TTestEnv::SetupStorage(ui32 ngroups)
{
    SetupBoxAndStoragePool(
        Runtime,
        Runtime.AllocateEdgeActor(),
        DomainUid,
        ngroups
    );

    auto selectGroups =
        std::make_unique<TEvBlobStorage::TEvControllerSelectGroups>();
    const auto& spTypes =
        Runtime.GetAppData().DomainsInfo->GetDomain(DomainUid).StoragePoolTypes;
    for (const auto& x : spTypes) {
        selectGroups->Record.AddGroupParameters()
            ->MutableStoragePoolSpecifier()->SetName(x.second.GetName());
    }
    selectGroups->Record.SetReturnAllMatchingGroups(true);
    Runtime.SendToPipe(
        MakeBSControllerID(DomainUid),
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

    UNIT_ASSERT(ngroups <= GroupIds.size());
}

void TTestEnv::SetupLocalServices()
{
    for (ui32 nodeIdx = 0; nodeIdx < StaticNodeCount; ++nodeIdx) {
        SetupLocalService(nodeIdx);
    }
}

void TTestEnv::SetupLocalService(ui32 nodeIdx)
{
    TLocalConfig::TPtr localConfig = new TLocalConfig();
    auto& appData = Runtime.GetAppData(nodeIdx);
    //SetupLocalConfig(*localConfig, appData);

    TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
    tenantPoolConfig->AddStaticSlot(DomainName);

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

void TTestEnv::SetupProxies()
{
    Runtime.SetTxAllocatorTabletIds(
        TVector<ui64>(1, ChangeDomain(Tests::TxAllocator, DomainUid)));

    for (ui32 nodeIdx = 0; nodeIdx < StaticNodeCount; ++nodeIdx) {
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
    const ui64 tabletId = ChangeDomain(Tests::SchemeRoot, DomainUid);
    const TDomainsInfo::TDomain& domain = Runtime.GetAppData().DomainsInfo->GetDomain(DomainUid);

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
    ui64 tabletId = ChangeDomain(Tests::SchemeRoot, DomainUid);

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

}   // namespace NCloud::NBlockStore::NStorage
