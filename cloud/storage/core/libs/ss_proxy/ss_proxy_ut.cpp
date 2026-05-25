#include "ss_proxy.h"

#include "public.h"

#include <cloud/storage/core/libs/api/ss_proxy.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/blockstore/core/blockstore.h>
#include <contrib/ydb/core/filestore/core/filestore.h>
#include <contrib/ydb/core/tablet_flat/tablet_flat_executed.h>
#include <contrib/ydb/core/testlib/basics/appdata.h>
#include <contrib/ydb/core/testlib/fake_coordinator.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/core/testlib/test_client.h>
#include <contrib/ydb/core/tx/schemeshard/schemeshard.h>
#include <contrib/ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <contrib/ydb/core/tx/tx_allocator/txallocator.h>
#include <contrib/ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

#include <functional>

namespace NCloud::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 NodeIdx = 0;

////////////////////////////////////////////////////////////////////////////////

class TFakeBlockStoreVolume final
    : public TActor<TFakeBlockStoreVolume>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TFakeBlockStoreVolume(const TActorId& tablet, TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {}

    void DefaultSignalTabletActive(const TActorContext&) override
    {}

    void OnActivateExecutor(const TActorContext& ctx) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
    }

    void OnDetach(const TActorContext& ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(
        TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx) override
    {
        Y_UNUSED(ev);
        Die(ctx);
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlockStore::TEvUpdateVolumeConfig, Handle);

            default:
                HandleDefaultEvents(ev, SelfId());
        }
    }

private:
    void Handle(
        TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& request = ev->Get()->Record;

        TAutoPtr<TEvBlockStore::TEvUpdateVolumeConfigResponse> response =
            new TEvBlockStore::TEvUpdateVolumeConfigResponse();

        response->Record.SetTxId(request.GetTxId());
        response->Record.SetOrigin(TabletID());
        response->Record.SetStatus(NKikimrBlockStore::OK);

        ctx.Send(ev->Sender, response.Release());
    }
};

class TFakeFileStore final
    : public TActor<TFakeFileStore>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TFakeFileStore(const TActorId& tablet, TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {}

    void DefaultSignalTabletActive(const TActorContext&) override
    {}

    void OnActivateExecutor(const TActorContext& ctx) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
    }

    void OnDetach(const TActorContext& ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(
        TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx) override
    {
        Y_UNUSED(ev);
        Die(ctx);
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvFileStore::TEvUpdateConfig, Handle);

            default:
                HandleDefaultEvents(ev, SelfId());
        }
    }

private:
    void Handle(
        TEvFileStore::TEvUpdateConfig::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& request = ev->Get()->Record;

        TAutoPtr<TEvFileStore::TEvUpdateConfigResponse> response =
            new TEvFileStore::TEvUpdateConfigResponse();

        response->Record.SetTxId(request.GetTxId());
        response->Record.SetOrigin(TabletID());
        response->Record.SetStatus(NKikimrFileStore::OK);

        ctx.Send(ev->Sender, response.Release());
    }
};

class TTxNotificationSubscriber final
    : public TActor<TTxNotificationSubscriber>
{
public:
    explicit TTxNotificationSubscriber(ui64 schemeShardTabletId)
        : TActor(&TThis::StateWork)
        , SchemeShardTabletId(schemeShardTabletId)
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletion, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);
        }
    }

    void Handle(
        TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const TActorContext& ctx)
    {
        if (ev->Get()->Status != NKikimrProto::OK) {
            HandlePipeDisconnected(ev->Get()->ClientId, ctx);
        }
    }

    void Handle(
        TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const TActorContext& ctx)
    {
        HandlePipeDisconnected(ev->Get()->ClientId, ctx);
    }

    void Handle(
        TEvSchemeShard::TEvNotifyTxCompletion::TPtr& ev,
        const TActorContext& ctx)
    {
        const ui64 txId = ev->Get()->Record.GetTxId();

        Waiters[txId].insert(ev->Sender);

        if (TxToPipe.find(txId) != TxToPipe.end()) {
            DropPipe(txId, ctx);
        }

        SendToSchemeShard(txId, ctx);
    }

    void Handle(
        TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
        const TActorContext& ctx)
    {
        const ui64 txId = ev->Get()->Record.GetTxId();

        auto it = Waiters.find(txId);
        if (it == Waiters.end()) {
            return;
        }

        for (const auto& waiter: it->second) {
            ctx.Send(
                waiter,
                new TEvSchemeShard::TEvNotifyTxCompletionResult(txId));
        }

        Waiters.erase(it);
        DropPipe(txId, ctx);
    }

    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        for (const auto& item: PipeToTx) {
            NTabletPipe::CloseClient(ctx, item.first);
        }

        PipeToTx.clear();
        TxToPipe.clear();
        Waiters.clear();

        Die(ctx);
    }

    void HandlePipeDisconnected(
        const TActorId& pipe,
        const TActorContext& ctx)
    {
        const auto it = PipeToTx.find(pipe);
        if (it == PipeToTx.end()) {
            return;
        }

        const ui64 txId = it->second;

        PipeToTx.erase(it);
        TxToPipe.erase(txId);

        SendToSchemeShard(txId, ctx);
    }

    void DropPipe(ui64 txId, const TActorContext& ctx)
    {
        const auto it = TxToPipe.find(txId);
        if (it == TxToPipe.end()) {
            return;
        }

        const TActorId pipe = it->second;

        TxToPipe.erase(it);
        PipeToTx.erase(pipe);

        NTabletPipe::CloseClient(ctx, pipe);
    }

    void SendToSchemeShard(ui64 txId, const TActorContext& ctx)
    {
        // Keep the same ordering property as the original ut_helpers code:
        // every NotifyTxCompletion goes through a fresh pipe, so TabletResolver
        // participates in the delivery path just like it does for ModifyScheme.
        const TActorId pipe = ctx.Register(NTabletPipe::CreateClient(
            ctx.SelfID,
            SchemeShardTabletId,
            GetPipeConfigWithRetries()));

        NTabletPipe::SendData(
            ctx,
            pipe,
            new TEvSchemeShard::TEvNotifyTxCompletion(txId));

        TxToPipe[txId] = pipe;
        PipeToTx[pipe] = txId;
    }

private:
    const ui64 SchemeShardTabletId;

    THashMap<ui64, THashSet<TActorId>> Waiters;
    THashMap<ui64, TActorId> TxToPipe;
    THashMap<TActorId, ui64> PipeToTx;
};

class TTestEnv final
{
public:
    using TSchemeShardFactory =
        std::function<IActor*(const TActorId&, TTabletStorageInfo*)>;

    explicit TTestEnv(
            TTestActorRuntime& runtime,
            ui32 channels = 4,
            bool enablePipeRetries = true,
            TSchemeShardFactory schemeShardFactory = &CreateFlatTxSchemeShard,
            bool enableSystemViews = false)
        : SchemeShardFactory(std::move(schemeShardFactory))
        , HiveState(new TFakeHiveState)
        , CoordinatorState(new TFakeCoordinator::TState)
    {
        Y_UNUSED(enableSystemViews);

        const ui64 hive = TTestTxConfig::Hive;
        const ui64 schemeRoot = TTestTxConfig::SchemeShard;
        const ui64 coordinator = TTestTxConfig::Coordinator;
        const ui64 txAllocator = TTestTxConfig::TxAllocator;

        TAppPrepare app;
        app.FeatureFlags.SetEnablePublicApiExternalBlobs(true);
        app.FeatureFlags.SetEnableTableDatetime64(true);

        AddDomain(runtime, app, TTestTxConfig::DomainUid, hive, schemeRoot);
        SetupLogging(runtime);
        SetupChannelProfiles(app, channels);

        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            SetupSchemeCache(
                runtime,
                node,
                app.Domains->GetDomain(TTestTxConfig::DomainUid).Name);
        }

        SetupTabletServices(runtime, &app);

        if (enablePipeRetries) {
            PipeRetriesGuard = EnableSchemeshardPipeRetries(runtime);
        }

        const TActorId sender = runtime.AllocateEdgeActor();

        BootSchemeShard(runtime, schemeRoot);
        BootTxAllocator(runtime, txAllocator);
        BootFakeCoordinator(runtime, coordinator, CoordinatorState);
        BootFakeHive(runtime, hive, HiveState, &GetTabletCreationFunc);
        InitRootStoragePools(
            runtime,
            schemeRoot,
            sender,
            TTestTxConfig::DomainUid);

        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            IActor* txProxy = CreateTxProxy(runtime.GetTxAllocatorTabletIds());
            const TActorId txProxyId = runtime.Register(txProxy, node);
            runtime.RegisterService(MakeTxProxyID(), txProxyId, node);
        }

        SetSplitMergePartCountLimit(&runtime, -1);
    }

    void TestWaitNotification(
        TTestActorRuntime& runtime,
        ui64 txId,
        ui64 schemeShardId = TTestTxConfig::SchemeShard)
    {
        TSet<ui64> txIds;
        txIds.insert(txId);

        TestWaitNotification(runtime, std::move(txIds), schemeShardId);
    }

    void TestWaitNotification(
        TTestActorRuntime& runtime,
        TSet<ui64> txIds,
        ui64 schemeShardId = TTestTxConfig::SchemeShard)
    {
        auto it = TxNotificationSubscribers.find(schemeShardId);
        if (it == TxNotificationSubscribers.end()) {
            TxNotificationSubscribers[schemeShardId] =
                runtime.Register(
                    new TTxNotificationSubscriber(schemeShardId));

            it = TxNotificationSubscribers.find(schemeShardId);
        }

        TestWaitNotification(runtime, std::move(txIds), it->second);
    }

private:
    static void SetupLogging(TTestActorRuntime& runtime)
    {
        runtime.SetLogPriority(
            NKikimrServices::PIPE_CLIENT,
            NLog::PRI_ERROR);
        runtime.SetLogPriority(
            NKikimrServices::PIPE_SERVER,
            NLog::PRI_ERROR);
        runtime.SetLogPriority(
            NKikimrServices::TABLET_MAIN,
            NLog::PRI_ERROR);
        runtime.SetLogPriority(
            NKikimrServices::TABLET_RESOLVER,
            NLog::PRI_ERROR);
        runtime.SetLogPriority(
            NKikimrServices::TX_PROXY,
            NLog::PRI_DEBUG);
        runtime.SetLogPriority(
            NKikimrServices::HIVE,
            NLog::PRI_ERROR);
        runtime.SetLogPriority(
            NKikimrServices::FLAT_TX_SCHEMESHARD,
            NLog::PRI_DEBUG);
        runtime.SetLogPriority(
            NKikimrServices::SCHEMESHARD_DESCRIBE,
            NLog::PRI_DEBUG);
    }

    static void AddDomain(
        TTestActorRuntime& runtime,
        TAppPrepare& app,
        ui32 domainUid,
        ui64 hive,
        ui64 schemeRoot)
    {
        app.ClearDomainsAndHive();

        constexpr ui32 planResolution = 50;
        auto domain =
            TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
                "MyRoot",
                domainUid,
                schemeRoot,
                planResolution,
                TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
                TVector<ui64>{},
                TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)},
                DefaultPoolKinds(2));

        TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
        ids.insert(
            ids.end(),
            domain->TxAllocators.begin(),
            domain->TxAllocators.end());
        runtime.SetTxAllocatorTabletIds(ids);

        app.AddDomain(domain.Release());
        app.AddHive(hive);
    }

    static TAutoPtr<ITabletScheduledEventsGuard>
    EnableSchemeshardPipeRetries(TTestActorRuntime& runtime)
    {
        const TActorId sender = runtime.AllocateEdgeActor();

        TVector<ui64> tabletIds;
        tabletIds.push_back(static_cast<ui64>(TTestTxConfig::SchemeShard));

        return CreateTabletScheduledEventsGuard(tabletIds, runtime, sender);
    }

    static std::function<IActor*(const TActorId&, TTabletStorageInfo*)>
    GetTabletCreationFunc(ui32 type)
    {
        switch (type) {
            case TTabletTypes::BlockStoreVolume:
                return [] (
                    const TActorId& tablet,
                    TTabletStorageInfo* info)
                {
                    return new TFakeBlockStoreVolume(tablet, info);
                };

            case TTabletTypes::FileStore:
                return [] (
                    const TActorId& tablet,
                    TTabletStorageInfo* info)
                {
                    return new TFakeFileStore(tablet, info);
                };

            default:
                return nullptr;
        }
    }

    void InitRootStoragePools(
        TTestActorRuntime& runtime,
        ui64 schemeRoot,
        const TActorId& sender,
        ui64 domainUid)
    {
        const TDomainsInfo::TDomain& domain =
            runtime.GetAppData().DomainsInfo->GetDomain(domainUid);

        auto* request =
            new TEvSchemeShard::TEvModifySchemeTransaction(
                1,
                TTestTxConfig::SchemeShard);

        auto* transaction = request->Record.AddTransaction();
        transaction->SetOperationType(
            NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain);
        transaction->SetWorkingDir("/");

        auto* op = transaction->MutableSubDomain();
        op->SetName(domain.Name);

        for (const auto& [kind, pool]: domain.StoragePoolTypes) {
            auto* storagePool = op->AddStoragePools();
            storagePool->SetKind(kind);
            storagePool->SetName(pool.GetName());
        }

        runtime.SendToPipe(
            schemeRoot,
            sender,
            request,
            0,
            GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto* event =
                runtime.GrabEdgeEvent<
                    TEvSchemeShard::TEvModifySchemeTransactionResult>(
                        handle);

            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL(
                event->Record.GetSchemeshardId(),
                schemeRoot);
            UNIT_ASSERT_VALUES_EQUAL(
                event->Record.GetStatus(),
                NKikimrScheme::EStatus::StatusAccepted);
        }

        runtime.SendToPipe(
            schemeRoot,
            sender,
            new TEvSchemeShard::TEvNotifyTxCompletion(1),
            0,
            GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto* event =
                runtime.GrabEdgeEvent<
                    TEvSchemeShard::TEvNotifyTxCompletionResult>(
                        handle);

            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), 1);
        }
    }

    void BootSchemeShard(TTestActorRuntime& runtime, ui64 schemeRoot)
    {
        CreateTestBootstrapper(
            runtime,
            CreateTestTabletInfo(schemeRoot, TTabletTypes::SchemeShard),
            SchemeShardFactory);
    }

    static void BootTxAllocator(TTestActorRuntime& runtime, ui64 tabletId)
    {
        CreateTestBootstrapper(
            runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::TxAllocator),
            &CreateTxAllocator);
    }

    static void TestWaitNotification(
        TTestActorRuntime& runtime,
        TSet<ui64> txIds,
        const TActorId& subscriberActorId)
    {
        const TActorId sender = runtime.AllocateEdgeActor();

        for (ui64 txId: txIds) {
            runtime.Send(new IEventHandle(
                subscriberActorId,
                sender,
                new TEvSchemeShard::TEvNotifyTxCompletion(txId)));
        }

        TAutoPtr<IEventHandle> handle;
        while (!txIds.empty()) {
            auto* event =
                runtime.GrabEdgeEvent<
                    TEvSchemeShard::TEvNotifyTxCompletionResult>(
                        handle);

            UNIT_ASSERT(event);

            const ui64 eventTxId = event->Record.GetTxId();
            UNIT_ASSERT(txIds.find(eventTxId) != txIds.end());

            txIds.erase(eventTxId);
        }
    }

private:
    TSchemeShardFactory SchemeShardFactory;
    TFakeHiveState::TPtr HiveState;
    TFakeCoordinator::TState::TPtr CoordinatorState;
    TAutoPtr<ITabletScheduledEventsGuard> PipeRetriesGuard;
    TMap<ui64, TActorId> TxNotificationSubscribers;
};

void TestMkDir(
    TTestActorRuntime& runtime,
    ui64 txId,
    const TString& workingDir,
    const TString& name)
{
    const TActorId sender = runtime.AllocateEdgeActor(NodeIdx);

    auto* request =
        new TEvSchemeShard::TEvModifySchemeTransaction(
            txId,
            TTestTxConfig::SchemeShard);

    auto* transaction = request->Record.AddTransaction();
    transaction->SetOperationType(
        NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
    transaction->SetWorkingDir(workingDir);
    transaction->MutableMkDir()->SetName(name);

    ForwardToTablet(
        runtime,
        TTestTxConfig::SchemeShard,
        sender,
        request);

    TAutoPtr<IEventHandle> handle;
    auto* response =
        runtime.GrabEdgeEventRethrow<
            TEvSchemeShard::TEvModifySchemeTransactionResult>(
                handle);

    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTxId(), txId);
    UNIT_ASSERT_VALUES_EQUAL(
        response->Record.GetSchemeshardId(),
        TTestTxConfig::SchemeShard);
    UNIT_ASSERT_VALUES_EQUAL(
        response->Record.GetStatus(),
        NKikimrScheme::EStatus::StatusAccepted);
}

////////////////////////////////////////////////////////////////////////////////

TSSProxyConfig CreateConfig(bool useSchemeCache)

{
    TSSProxyConfig config;
    config.SchemeShardDir = "/MyRoot";
    config.UseSchemeCache = useSchemeCache;
    return config;
}

TActorId SetupSSProxy(
    TTestBasicRuntime& runtime,
    bool useSchemeCache)
{
    auto ssProxyId = runtime.Register(
        CreateSSProxy(CreateConfig(useSchemeCache)).release(),
        NodeIdx);

    runtime.EnableScheduleForActor(ssProxyId);
    runtime.DispatchEvents({}, TDuration::Seconds(1));

    return ssProxyId;
}

TEvSSProxy::TEvDescribeSchemeResponse::TPtr DescribePath(
    TTestBasicRuntime& runtime,
    const TActorId& ssProxyId,
    const TString& path)
{
    const auto sender = runtime.AllocateEdgeActor(NodeIdx);

    runtime.Send(
        new IEventHandle(
            ssProxyId,
            sender,
            new TEvSSProxy::TEvDescribeSchemeRequest(path)),
        NodeIdx);

    TAutoPtr<IEventHandle> handle;
    auto* response =
        runtime.GrabEdgeEventRethrow<
            TEvSSProxy::TEvDescribeSchemeResponse>(handle);

    UNIT_ASSERT_C(
        !HasError(response->GetError()),
        response->GetError());

    return IEventHandle::Downcast<
        TEvSSProxy::TEvDescribeSchemeResponse>(
            std::move(handle));
}

void CreateFileStore(
    TTestBasicRuntime& runtime,
    const TActorId& ssProxyId,
    const TString& fileSystemId)
{
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir("/MyRoot/nbs");
    modifyScheme.SetOperationType(
        NKikimrSchemeOp::ESchemeOpCreateFileStore);

    auto& fs = *modifyScheme.MutableCreateFileStore();
    fs.SetName(fileSystemId);

    auto& config = *fs.MutableConfig();
    config.SetFileSystemId(fileSystemId);
    config.SetBlockSize(4096);
    config.SetBlocksCount(1024);
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

    const auto sender = runtime.AllocateEdgeActor(NodeIdx);

    runtime.Send(
        new IEventHandle(
            ssProxyId,
            sender,
            new TEvSSProxy::TEvModifySchemeRequest(
                std::move(modifyScheme))),
        NodeIdx);

    TAutoPtr<IEventHandle> handle;
    auto* response =
        runtime.GrabEdgeEventRethrow<
            TEvSSProxy::TEvModifySchemeResponse>(handle);

    UNIT_ASSERT_C(
        !HasError(response->GetError()),
        response->GetError());
}

void CreateBlockStoreVolume(
    TTestBasicRuntime& runtime,
    const TActorId& ssProxyId,
    const TString& diskId)
{
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir("/MyRoot/nbs");
    modifyScheme.SetOperationType(
        NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume);

    auto& volume = *modifyScheme.MutableCreateBlockStoreVolume();
    volume.SetName(diskId);

    auto& config = *volume.MutableVolumeConfig();
    config.SetDiskId(diskId);
    config.SetBlockSize(4096);
    config.AddPartitions()->SetBlockCount(1024);
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

    const auto sender = runtime.AllocateEdgeActor(NodeIdx);

    runtime.Send(
        new IEventHandle(
            ssProxyId,
            sender,
            new TEvSSProxy::TEvModifySchemeRequest(
                std::move(modifyScheme))),
        NodeIdx);

    TAutoPtr<IEventHandle> handle;
    auto* response =
        runtime.GrabEdgeEventRethrow<
            TEvSSProxy::TEvModifySchemeResponse>(handle);

    UNIT_ASSERT_C(
        !HasError(response->GetError()),
        response->GetError());
}

void TestShouldDescribeFileStore(bool useSchemeCache)
{
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);

    ui64 txId = 100;

    TestMkDir(runtime, ++txId, "/MyRoot", "nbs");
    env.TestWaitNotification(runtime, txId);

    auto ssProxyId = SetupSSProxy(runtime, useSchemeCache);

    CreateFileStore(runtime, ssProxyId, "fs");

    const auto response =
        DescribePath(runtime, ssProxyId, "/MyRoot/nbs/fs");

    const auto& pathDescription =
        response->Get()->PathDescription;

    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrSchemeOp::EPathTypeFileStore,
        pathDescription.GetSelf().GetPathType());

    UNIT_ASSERT_VALUES_EQUAL(
        "fs",
        pathDescription
            .GetFileStoreDescription()
            .GetConfig()
            .GetFileSystemId());
}

void TestShouldDescribeBlockStoreVolume(bool useSchemeCache)
{
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);

    ui64 txId = 100;

    TestMkDir(runtime, ++txId, "/MyRoot", "nbs");
    env.TestWaitNotification(runtime, txId);

    auto ssProxyId = SetupSSProxy(runtime, useSchemeCache);

    CreateBlockStoreVolume(runtime, ssProxyId, "volume");

    const auto response =
        DescribePath(runtime, ssProxyId, "/MyRoot/nbs/volume");

    const auto& pathDescription =
        response->Get()->PathDescription;

    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrSchemeOp::EPathTypeBlockStoreVolume,
        pathDescription.GetSelf().GetPathType());

    UNIT_ASSERT_VALUES_EQUAL(
        "volume",
        pathDescription
            .GetBlockStoreVolumeDescription()
            .GetVolumeConfig()
            .GetDiskId());
}

void TestShouldListFileStoresAndBlockStoreVolumes(
    bool useSchemeCache)
{
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);

    ui64 txId = 100;

    TestMkDir(runtime, ++txId, "/MyRoot", "nbs");
    env.TestWaitNotification(runtime, txId);

    auto ssProxyId = SetupSSProxy(runtime, useSchemeCache);

    CreateFileStore(runtime, ssProxyId, "fs");
    CreateBlockStoreVolume(runtime, ssProxyId, "volume");

    const auto response =
        DescribePath(runtime, ssProxyId, "/MyRoot/nbs");

    const auto& pathDescription =
        response->Get()->PathDescription;

    bool foundFileStore = false;
    bool foundBlockStoreVolume = false;

    for (const auto& child: pathDescription.GetChildren()) {
        if (child.GetName() == "fs") {
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimrSchemeOp::EPathTypeFileStore,
                child.GetPathType());

            foundFileStore = true;
        } else if (child.GetName() == "volume") {
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimrSchemeOp::EPathTypeBlockStoreVolume,
                child.GetPathType());

            foundBlockStoreVolume = true;
        }
    }

    UNIT_ASSERT(foundFileStore);
    UNIT_ASSERT(foundBlockStoreVolume);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSSProxyTest)
{
    Y_UNIT_TEST(ShouldDescribeFileStoreWithSchemeCache)
    {
        TestShouldDescribeFileStore(true);
    }

    Y_UNIT_TEST(ShouldDescribeFileStoreWithoutSchemeCache)
    {
        TestShouldDescribeFileStore(false);
    }

    Y_UNIT_TEST(ShouldDescribeBlockStoreVolumeWithSchemeCache)
    {
        TestShouldDescribeBlockStoreVolume(true);
    }

    Y_UNIT_TEST(ShouldDescribeBlockStoreVolumeWithoutSchemeCache)
    {
        TestShouldDescribeBlockStoreVolume(false);
    }

    Y_UNIT_TEST(ShouldListFileStoresAndBlockStoreVolumesWithSchemeCache)
    {
        TestShouldListFileStoresAndBlockStoreVolumes(true);
    }

    Y_UNIT_TEST(ShouldListFileStoresAndBlockStoreVolumesWithoutSchemeCache)
    {
        TestShouldListFileStoresAndBlockStoreVolumes(false);
    }
}

}   // namespace NCloud::NStorage
