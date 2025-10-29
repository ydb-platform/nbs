#include "hive_proxy.h"
#include "hive_proxy_events_private.h"

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr ui64 FakeHiveTablet = 0x0000000000000001;
static constexpr ui64 TenantHiveTablet = 0x0000000000000002;
static constexpr ui64 FakeSchemeRoot = 0x00000000008401F0;
static constexpr ui64 FakeTablet2 = 0x0000000000840102;
static constexpr ui64 FakeTablet3 = 0x0000000000840103;
static constexpr ui64 FakeMissingTablet = 0x0000000000840104;

////////////////////////////////////////////////////////////////////////////////

struct THiveMockState
    : public TAtomicRefCount<THiveMockState>
{
    using TPtr = TIntrusivePtr<THiveMockState>;

    THashMap<ui64, TActorId> LockedTablets;
    THashMap<ui64, ui64> ReconnectRequests;
    THashMap<ui64, TTabletStorageInfoPtr> StorageInfos;
    THashMap<ui64, ui32> KnownGenerations;
    THashSet<ui32> DrainableNodeIds;
    THashSet<ui32> DownNodeIds;
};

////////////////////////////////////////////////////////////////////////////////

enum class EExternalBootOptions
{
    PROCESS  = 0,
    FAIL     = 1,
    TRYLATER = 2,
    IGNORE   = 3,
};

////////////////////////////////////////////////////////////////////////////////

class THiveMock final
    : public TActor<THiveMock>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
private:
    THiveMockState::TPtr State;
    EExternalBootOptions ExternalBootOptions = EExternalBootOptions::PROCESS;

public:
    THiveMock(
            const TActorId& owner,
            TTabletStorageInfo* info,
            THiveMockState::TPtr state)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, owner, nullptr)
        , State(std::move(state))
    {
        UNIT_ASSERT(State);
    }

    THiveMock(
            const TActorId& owner,
            TTabletStorageInfo* info,
            THiveMockState::TPtr state,
            EExternalBootOptions externalBootOptions)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, owner, nullptr)
        , State(std::move(state))
        , ExternalBootOptions(externalBootOptions)
    {
        UNIT_ASSERT(State);
    }

private:
    void DefaultSignalTabletActive(const TActorContext&) override
    {
        // must be empty
    }

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

    void Enqueue(STFUNC_SIG) override
    {
        Y_ABORT("Unexpected event %x", ev->GetTypeRewrite());
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoison);
            HFunc(TEvHive::TEvLockTabletExecution, HandleLock);
            HFunc(TEvHive::TEvUnlockTabletExecution, HandleUnlock);
            HFunc(TEvHive::TEvGetTabletStorageInfo, HandleInfo);
            HFunc(TEvHive::TEvInitiateTabletExternalBoot, HandleBoot);
            HFunc(TEvHive::TEvReassignTablet, HandleReassignTablet);
            HFunc(TEvHive::TEvDrainNode, HandleDrainNode);
            HFunc(TEvHive::TEvTabletMetrics, HandleTabletMetrics);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    Y_ABORT("Unexpected event %u", ev->GetTypeRewrite());
                }
        }
    }

    void HandlePoison(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
    }

    TActorId GetLockOwner(ui64 tabletId)
    {
        auto it = State->LockedTablets.find(tabletId);
        if (it != State->LockedTablets.end()) {
            return it->second;
        }
        return TActorId();
    }

    void SetLockOwner(
        const TActorContext& ctx,
        ui64 tabletId,
        TActorId newOwner)
    {
        auto prev = State->LockedTablets[tabletId];
        State->LockedTablets[tabletId] = newOwner;
        if (prev && prev != newOwner) {
            ctx.Send(prev, new TEvHive::TEvLockTabletExecutionLost(tabletId));
        }
    }

    TTabletStorageInfoPtr GetStorageInfo(ui64 tabletId)
    {
        auto it = State->StorageInfos.find(tabletId);
        if (it != State->StorageInfos.end()) {
            return it->second;
        }
        return nullptr;
    }

    void HandleLock(
        const TEvHive::TEvLockTabletExecution::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        UNIT_ASSERT_C(
            !msg->Record.HasOwnerActor(),
            "owner actors not supported");

        ui64 tabletId = msg->Record.GetTabletID();
        if (tabletId == FakeMissingTablet) {
            ctx.Send(
                ev->Sender,
                new TEvHive::TEvLockTabletExecutionResult(
                    tabletId,
                    NKikimrProto::ERROR,
                    "Tablet not found"));
            return;
        }

        if (msg->Record.GetReconnect()) {
            ++State->ReconnectRequests[tabletId];

            if (GetLockOwner(tabletId) != ev->Sender) {
                ctx.Send(
                    ev->Sender,
                    new TEvHive::TEvLockTabletExecutionResult(
                        tabletId,
                        NKikimrProto::ERROR,
                        "Not locked"));
                return;
            }
        } else {
            SetLockOwner(ctx, tabletId, ev->Sender);
        }

        ctx.Send(
            ev->Sender,
            new TEvHive::TEvLockTabletExecutionResult(
                tabletId,
                NKikimrProto::OK,
                ""));
    }

    void HandleUnlock(
        const TEvHive::TEvUnlockTabletExecution::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        UNIT_ASSERT_C(
            !msg->Record.HasOwnerActor(),
            "owner actors not supported");

        ui64 tabletId = msg->Record.GetTabletID();

        if (GetLockOwner(tabletId) != ev->Sender) {
            ctx.Send(
                ev->Sender,
                new TEvHive::TEvUnlockTabletExecutionResult(
                    tabletId,
                    NKikimrProto::ERROR,
                    "Not locked"));
            return;
        }

        State->LockedTablets.erase(tabletId);
        ctx.Send(
            ev->Sender,
            new TEvHive::TEvUnlockTabletExecutionResult(
                tabletId,
                NKikimrProto::OK,
                ""));
    }

    void HandleInfo(
        const TEvHive::TEvGetTabletStorageInfo::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        ui64 tabletId = msg->Record.GetTabletID();
        auto info = GetStorageInfo(tabletId);
        if (!info) {
            ctx.Send(
                ev->Sender,
                new TEvHive::TEvGetTabletStorageInfoResult(
                    tabletId,
                    NKikimrProto::ERROR));
            return;
        }

        ctx.Send(
            ev->Sender,
            new TEvHive::TEvGetTabletStorageInfoRegistered(tabletId));

        ctx.Send(
            ev->Sender,
            new TEvHive::TEvGetTabletStorageInfoResult(tabletId, *info));
    }

    void HandleBoot(
        const TEvHive::TEvInitiateTabletExternalBoot::TPtr& ev,
        const TActorContext& ctx)
    {
        if (ExternalBootOptions == EExternalBootOptions::IGNORE) {
            return;
        }

        if (ExternalBootOptions != EExternalBootOptions::PROCESS) {
            auto errorCode = NKikimrProto::ERROR;
            if (ExternalBootOptions == EExternalBootOptions::TRYLATER) {
                errorCode = NKikimrProto::TRYLATER;
            }
            ctx.Send(
                ev->Sender,
                new TEvHive::TEvBootTabletReply(errorCode));
            return;
        }

        const auto& msg = ev->Get();

        ui64 tabletId = msg->Record.GetTabletID();
        auto info = GetStorageInfo(tabletId);
        if (!info) {
            ctx.Send(
                ev->Sender,
                new TEvHive::TEvBootTabletReply(NKikimrProto::ERROR));
            return;
        }

        ctx.Send(ev->Sender,
            new TEvLocal::TEvBootTablet(
                *info,
                0,
                ++State->KnownGenerations[tabletId]));
    }

    void HandleReassignTablet(
        const TEvHive::TEvReassignTablet::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& msg = ev->Get();

        ui64 tabletId = msg->Record.GetTabletID();
        auto info = GetStorageInfo(tabletId);

        if (!info) {
            ctx.Send(
                ev->Sender,
                new TEvHive::TEvBootTabletReply(NKikimrProto::ERROR));
            return;
        }

        auto gen = ++State->KnownGenerations[tabletId];

        UNIT_ASSERT(info->Channels.size());

        for (auto channel: msg->Record.GetChannels()) {
            UNIT_ASSERT(channel < info->Channels.size());
            auto& channelInfo = info->Channels[channel];
            channelInfo.History.emplace_back(
                gen,
                channelInfo.History.back().GroupID + 1
            );
        }

        ctx.Send(ev->Sender, new TEvHive::TEvTabletCreationResult());
    }

    void HandleDrainNode(
        const TEvHive::TEvDrainNode::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& msg = ev->Get();

        NKikimrProto::EReplyStatus replyStatus = NKikimrProto::OK;
        ui64 nodeId = msg->Record.GetNodeID();
        if (State->DrainableNodeIds.contains(nodeId)) {
            if (msg->Record.GetKeepDown()) {
                State->DownNodeIds.insert(nodeId);
            }
        } else {
            replyStatus = NKikimrProto::ERROR;
        }

        ctx.Send(ev->Sender, new TEvHive::TEvDrainNodeResult(replyStatus));
    }

    void HandleTabletMetrics(
        const TEvHive::TEvTabletMetrics::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        auto response = std::make_unique<TEvLocal::TEvTabletMetricsAck>();
        for (ui32 i = 0; i < msg->Record.TabletMetricsSize(); ++i) {
            response->Record.AddTabletId(
                msg->Record.GetTabletMetrics(i).GetTabletID());
            response->Record.AddFollowerId(
                msg->Record.GetTabletMetrics(i).GetFollowerID());
        }

        ctx.Send(ev->Sender, response.release());
    }
};

////////////////////////////////////////////////////////////////////////////////

void BootHiveMock(
    TTestActorRuntime& runtime,
    ui64 tabletId,
    THiveMockState::TPtr state,
    EExternalBootOptions externalBootOptions)
{
    TActorId actorId = CreateTestBootstrapper(
        runtime,
        CreateTestTabletInfo(tabletId, TTabletTypes::Hive),
        [=] (const TActorId& owner, TTabletStorageInfo* info) -> IActor* {
            return new THiveMock(owner, info, state, externalBootOptions);
        });
    runtime.EnableScheduleForActor(actorId);

    {
        TDispatchOptions options;
        options.FinalEvents.push_back(
            TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
        runtime.DispatchEvents(options);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    TTestActorRuntime& Runtime;
    bool Debug = false;

    TActorId HiveProxyActorId;
    THiveMockState::TPtr HiveState = MakeIntrusive<THiveMockState>();
    THiveMockState::TPtr TenantHiveState = MakeIntrusive<THiveMockState>();

    TTestEnv(
            TTestActorRuntime& runtime,
            TString tabletBootInfoBackupFilePath = "",
            bool fallbackMode = false,
            bool debug = false,
            bool useTenantHive = false)
        : Runtime(runtime)
        , Debug(debug)
    {
        TAppPrepare app;

        AddDomain(app, 0, 0, FakeHiveTablet, FakeSchemeRoot);

        SetupLogging();
        SetupChannelProfiles(app);
        SetupTabletServices(Runtime, &app, true);

        BootHiveMock(Runtime, FakeHiveTablet, HiveState, EExternalBootOptions::PROCESS);
        BootHiveMock(Runtime, TenantHiveTablet, TenantHiveState, EExternalBootOptions::PROCESS);

        ui64 tenantHive = useTenantHive ? TenantHiveTablet : 0;
        SetupHiveProxy(tabletBootInfoBackupFilePath, fallbackMode, tenantHive);
    }

    TTestEnv(
            TTestActorRuntime& runtime,
            EExternalBootOptions externalBootOptions)
        : Runtime(runtime)
        , HiveState(MakeIntrusive<THiveMockState>())
    {
        TAppPrepare app;

        AddDomain(app, 0, 0, FakeHiveTablet, FakeSchemeRoot);

        SetupLogging();
        SetupChannelProfiles(app);
        SetupTabletServices(Runtime, &app, true);

        BootHiveMock(Runtime, FakeHiveTablet, HiveState, externalBootOptions);

        SetupHiveProxy("", false, 0);
    }

    ~TTestEnv()
    {
    }

    void AddDomain(TAppPrepare& app, ui32 domainUid, ui32 ssId, ui64 hive, ui64 schemeRoot)
    {
        ui32 planResolution = 50;
        app.ClearDomainsAndHive();
        app.AddDomain(TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            "MyRoot", domainUid, schemeRoot,
            ssId, ssId, TVector<ui32>{ssId},
            domainUid, TVector<ui32>{domainUid},
            planResolution,
            TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(domainUid, 1)},
            TVector<ui64>{},
            TVector<ui64>{},
            DefaultPoolKinds()).Release());
        app.AddHive(domainUid, hive);
    }

    void SetupLogging()
    {
        if (Debug) {
            Runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
        }
        Runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_ERROR);
    }

    void SetupHiveProxy(
        TString tabletBootInfoBackupFilePath,
        bool fallbackMode,
        ui64 tenantHive)
    {
        THiveProxyConfig config{
            .PipeClientRetryCount = 4,
            .PipeClientMinRetryTime = TDuration::Seconds(1),
            .HiveLockExpireTimeout = TDuration::Seconds(30),
            .LogComponent = 0,
            .TabletBootInfoBackupFilePath = tabletBootInfoBackupFilePath,
            .FallbackMode = fallbackMode,
            .TenantHiveTabletId = tenantHive,
        };
        HiveProxyActorId = Runtime.Register(
            CreateHiveProxy(
                std::move(config),
                Runtime.GetAppData(0).Counters).release());
        Runtime.EnableScheduleForActor(HiveProxyActorId);
        Runtime.RegisterService(MakeHiveProxyServiceId(), HiveProxyActorId);
    }

    void EnableTabletResolverScheduling(ui32 nodeIdx = 0)
    {
        auto actorId = Runtime.GetLocalServiceId(
            MakeTabletResolverID(),
            nodeIdx);
        UNIT_ASSERT(actorId);
        Runtime.EnableScheduleForActor(actorId);
    }

    void RebootHive()
    {
        auto sender = Runtime.AllocateEdgeActor();
        RebootTablet(Runtime, FakeHiveTablet, sender);
    }

    void SendLockRequest(
        const TActorId& sender,
        ui64 tabletId,
        ui32 errorCode = 0)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvLockTabletRequest(tabletId)));
        TAutoPtr<IEventHandle> handle;
        auto* event =
            Runtime.GrabEdgeEvent<TEvHiveProxy::TEvLockTabletResponse>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->GetError().GetCode(), errorCode);
    }

    void SendUnlockRequest(
        const TActorId& sender,
        ui64 tabletId,
        ui32 errorCode = 0)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvUnlockTabletRequest(tabletId)));
        TAutoPtr<IEventHandle> handle;
        auto* event =
            Runtime.GrabEdgeEvent<TEvHiveProxy::TEvUnlockTabletResponse>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->GetError().GetCode(), errorCode);
    }

    void WaitLockLost(
        const TActorId& sender,
        ui64 tabletId,
        ui32 errorCode = 0)
    {
        TAutoPtr<IEventHandle> handle;
        TEvHiveProxy::TEvTabletLockLost* event = nullptr;
        do {
            event =
                Runtime.GrabEdgeEvent<TEvHiveProxy::TEvTabletLockLost>(handle);
        } while (handle && handle->GetRecipientRewrite() != sender);
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->TabletId, tabletId);
        UNIT_ASSERT_VALUES_EQUAL(event->GetError().GetCode(), errorCode);
    }

    TTabletStorageInfoPtr SendGetStorageInfoRequest(
        const TActorId& sender,
        ui64 tabletId,
        ui32 errorCode = 0)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvGetStorageInfoRequest(tabletId)));
        TAutoPtr<IEventHandle> handle;
        auto* event =
            Runtime.GrabEdgeEvent<TEvHiveProxy::TEvGetStorageInfoResponse>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->GetError().GetCode(), errorCode);
        return event->StorageInfo;
    }

    TEvHiveProxy::TBootExternalResponse SendBootExternalRequest(
        const TActorId& sender,
        ui64 tabletId,
        ui32 errorCode)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvBootExternalRequest(tabletId)));
        auto ev = Runtime.GrabEdgeEvent<TEvHiveProxy::TEvBootExternalResponse>(sender);
        UNIT_ASSERT(ev);
        const auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->GetStatus(), errorCode);
        return *msg;
    }

    TEvHiveProxy::TBootExternalResponse SendBootExternalRequest(
        const TActorId& sender,
        ui64 tabletId,
        ui32 errorCode,
        TDuration requestTimeout)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvBootExternalRequest(tabletId, requestTimeout)));
        auto ev = Runtime.GrabEdgeEvent<TEvHiveProxy::TEvBootExternalResponse>(sender);
        UNIT_ASSERT(ev);
        const auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->GetStatus(), errorCode);
        return *msg;
    }

    TEvHiveProxy::TReassignTabletResponse SendReassignTabletRequest(
        const TActorId& sender,
        ui64 tabletId,
        TVector<ui32> channels,
        ui32 errorCode)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvReassignTabletRequest(
                tabletId,
                std::move(channels)
            )
        ));
        using TResponse = TEvHiveProxy::TEvReassignTabletResponse;
        auto ev = Runtime.GrabEdgeEvent<TResponse>(sender);
        UNIT_ASSERT(ev);
        const auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(
            msg->GetStatus(),
            errorCode,
            msg->GetErrorReason()
        );

        return *msg;
    }

    TEvHiveProxy::TDrainNodeResponse SendDrainNodeRequest(
        const TActorId& sender,
        bool keepDown,
        ui32 errorCode)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvDrainNodeRequest(keepDown)
        ));
        using TResponse = TEvHiveProxy::TEvDrainNodeResponse;
        auto ev = Runtime.GrabEdgeEvent<TResponse>(sender);
        UNIT_ASSERT(ev);
        const auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(
            msg->GetStatus(),
            errorCode,
            msg->GetErrorReason()
        );

        return *msg;
    }

    TEvHiveProxy::TBackupTabletBootInfosResponse SendBackupTabletBootInfos(
        const TActorId& sender,
        ui32 errorCode)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvBackupTabletBootInfosRequest()));
        auto ev =
            Runtime.GrabEdgeEvent<TEvHiveProxy::TEvBackupTabletBootInfosResponse>(
                sender);
        UNIT_ASSERT(ev);
        const auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->GetStatus(), errorCode);
        return *msg;
    }

    TEvHiveProxy::TListTabletBootInfoBackupsResponse
    SendListTabletBootInfoBackups(const TActorId& sender, ui32 errorCode)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvListTabletBootInfoBackupsRequest()));
        auto ev = Runtime.GrabEdgeEvent<
            TEvHiveProxy::TEvListTabletBootInfoBackupsResponse>(sender);
        UNIT_ASSERT(ev);
        const auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->GetStatus(), errorCode);
        return *msg;
    }

    void SendCreateTabletRequestAsync(
        const TActorId& sender,
        ui64 hiveId,
        NKikimrHive::TEvCreateTablet request)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvCreateTabletRequest(
                hiveId,
                std::move(request))));
    }

    void SendLookupTabletRequestAsync(
        const TActorId& sender,
        ui64 hiveId,
        const ui64 owner,
        const ui64 ownerIdx)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvLookupTabletRequest(
                hiveId,
                owner,
                ownerIdx)));
    }

    TEvHiveProxy::TCreateTabletResponse SendCreateTabletRequest(
        const TActorId& sender,
        ui64 hiveId,
        NKikimrHive::TEvCreateTablet request,
        ui32 errorCode)
    {
        SendCreateTabletRequestAsync(sender, hiveId, std::move(request));

        auto ev = Runtime.GrabEdgeEvent<TEvHiveProxy::TEvCreateTabletResponse>(
            sender);
        UNIT_ASSERT(ev);
        const auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->GetStatus(), errorCode);
        return *msg;
    }

    TEvHiveProxy::TLookupTabletResponse SendLookupTabletRequest(
        const TActorId& sender,
        ui64 hiveId,
        const ui64 owner,
        const ui64 ownerIdx,
        ui32 errorCode)
    {
        SendLookupTabletRequestAsync(sender, hiveId, owner, ownerIdx);

        auto ev =
            Runtime.GrabEdgeEvent<TEvHiveProxy::TEvLookupTabletResponse>(
                sender);
        UNIT_ASSERT(ev);
        const auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->GetStatus(), errorCode);
        return *msg;
    }

    void SendTabletMetricRequestAsync(
        const TActorId& sender,
        ui64 tabletId)
    {
        NKikimrTabletBase::TMetrics resourceValues;
        resourceValues.SetCPU(1'000'000);
        auto request = std::make_unique<TEvLocal::TEvTabletMetrics>(
            tabletId,
            0,
            resourceValues);

        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            request.release()));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THiveProxyTest)
{
    Y_UNIT_TEST(LockUnlock)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);

        env.SendUnlockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            TActorId());
    }

    Y_UNIT_TEST(LockSameTabletTwice)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);

        env.SendLockRequest(sender, FakeTablet2, E_REJECTED);
    }

    Y_UNIT_TEST(LockMissingTablet) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        env.SendLockRequest(sender, FakeMissingTablet, MAKE_KIKIMR_ERROR(NKikimrProto::ERROR));
    }

    Y_UNIT_TEST(LockDifferentTablets)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);

        env.SendLockRequest(sender, FakeTablet3);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet3],
            env.HiveProxyActorId);
    }

    Y_UNIT_TEST(LockStolenNotification)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);

        {
            auto senderB = runtime.AllocateEdgeActor();
            runtime.SendToPipe(
                FakeHiveTablet,
                senderB,
                new TEvHive::TEvLockTabletExecution(FakeTablet2));

            TAutoPtr<IEventHandle> handle;
            auto response = runtime.GrabEdgeEvent<TEvHive::TEvLockTabletExecutionResult>(handle);

            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(handle->GetRecipientRewrite(), senderB);
            UNIT_ASSERT_VALUES_EQUAL(
                response->Record.GetStatus(),
                NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(
                env.HiveState->LockedTablets[FakeTablet2],
                senderB);
        }

        // Virtual lock owner should receive a lost notification
        env.WaitLockLost(sender, FakeTablet2, E_REJECTED);

        // New lock request should succeed, stealing the lock back
        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);
    }

    Y_UNIT_TEST(LockReconnected)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->ReconnectRequests[FakeTablet2],
            0);

        int hiveLockRequests = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvHive::EvLockTabletExecution) {
                    ++hiveLockRequests;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });

        env.EnableTabletResolverScheduling();
        env.RebootHive();

        for (int retries = 0; retries < 5 && !hiveLockRequests; ++retries) {
            // Pipe to hive may take a long time to connect
            // Wait until hive receives the lock request
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

        runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);

        // Rebooting hive should reconnect the lock
        UNIT_ASSERT_VALUES_EQUAL(hiveLockRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->ReconnectRequests[FakeTablet2],
            1);

        // Unlock should succeed, since lock isn't lost
        env.SendUnlockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.HiveState->LockedTablets[FakeTablet2],
            TActorId());
    }

    Y_UNIT_TEST(GetStorageInfoMissing)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        auto result = env.SendGetStorageInfoRequest(
            sender,
            FakeTablet2,
            MAKE_KIKIMR_ERROR(NKikimrProto::ERROR));

        UNIT_ASSERT(!result);
    }

    Y_UNIT_TEST(GetStorageInfoOK)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TTabletStorageInfoPtr expected =
            CreateTestTabletInfo(FakeTablet2, TTabletTypes::BlockStoreVolume);
        env.HiveState->StorageInfos[FakeTablet2] = expected;

        auto sender = runtime.AllocateEdgeActor();

        auto result = env.SendGetStorageInfoRequest(sender, FakeTablet2);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->TabletID, expected->TabletID);
        UNIT_ASSERT_VALUES_EQUAL(result->TabletType, expected->TabletType);
    }

    Y_UNIT_TEST(ReassignTablet)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TTabletStorageInfoPtr expected =
            CreateTestTabletInfo(FakeTablet2, TTabletTypes::BlockStoreVolume);
        env.HiveState->StorageInfos[FakeTablet2] =
            new TTabletStorageInfo(*expected);

        auto sender = runtime.AllocateEdgeActor();

        env.SendReassignTabletRequest(
            sender,
            FakeTablet2,
            {1, 3},
            S_OK
        );

        auto result = env.HiveState->StorageInfos[FakeTablet2];

        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            result->Channels.size(),
            expected->Channels.size()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            result->Channels[0].History.size(),
            expected->Channels[0].History.size());
        UNIT_ASSERT_VALUES_EQUAL(
            result->Channels[1].History.size(),
            expected->Channels[1].History.size() + 1
        );
        UNIT_ASSERT_VALUES_EQUAL(
            result->Channels[1].History.back().GroupID,
            expected->Channels[1].History.back().GroupID + 1
        );
        UNIT_ASSERT_VALUES_EQUAL(
            result->Channels[2].History.size(),
            expected->Channels[2].History.size());
        UNIT_ASSERT_VALUES_EQUAL(
            result->Channels[3].History.size(),
            expected->Channels[3].History.size() + 1
        );
        UNIT_ASSERT_VALUES_EQUAL(
            result->Channels[3].History.back().GroupID,
            expected->Channels[3].History.back().GroupID + 1
        );
    }

    Y_UNIT_TEST(ReassignTabletDuringDisconnect)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TTabletStorageInfoPtr expected =
            CreateTestTabletInfo(FakeTablet2, TTabletTypes::BlockStoreVolume);
        env.HiveState->StorageInfos[FakeTablet2] =
            new TTabletStorageInfo(*expected);

        auto sender = runtime.AllocateEdgeActor();

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvHive::EvReassignTablet) {
                    env.RebootHive();
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            });

        env.SendReassignTabletRequest(
            sender,
            FakeTablet2,
            {1, 3},
            E_REJECTED
        );
    }

    Y_UNIT_TEST(BootExternalError)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        auto result = env.SendBootExternalRequest(
            sender,
            FakeTablet2,
            MAKE_KIKIMR_ERROR(NKikimrProto::ERROR));
        UNIT_ASSERT(!result.StorageInfo);
    }

    Y_UNIT_TEST(BootExternalOK)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TTabletStorageInfoPtr expected =
            CreateTestTabletInfo(FakeTablet2, TTabletTypes::BlockStorePartition);
        env.HiveState->StorageInfos[FakeTablet2] = expected;

        auto sender = runtime.AllocateEdgeActor();

        auto result = env.SendBootExternalRequest(sender, FakeTablet2, S_OK);
        UNIT_ASSERT(result.StorageInfo);
        UNIT_ASSERT_VALUES_EQUAL(result.StorageInfo->TabletID, expected->TabletID);
        UNIT_ASSERT_VALUES_EQUAL(result.StorageInfo->TabletType, expected->TabletType);
        UNIT_ASSERT_VALUES_EQUAL(result.SuggestedGeneration, 1u);

        auto result2 = env.SendBootExternalRequest(sender, FakeTablet2, S_OK);
        UNIT_ASSERT(result2.StorageInfo);
        UNIT_ASSERT_VALUES_EQUAL(result2.SuggestedGeneration, 2u);
    }

    Y_UNIT_TEST(DrainNode)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        env.SendDrainNodeRequest(sender, false, E_FAIL);

        env.HiveState->DrainableNodeIds.insert(sender.NodeId());
        env.SendDrainNodeRequest(sender, false, S_OK);

        UNIT_ASSERT(!env.HiveState->DownNodeIds.contains(sender.NodeId()));

        env.SendDrainNodeRequest(sender, true, S_OK);
        UNIT_ASSERT(env.HiveState->DownNodeIds.contains(sender.NodeId()));
    }

    Y_UNIT_TEST(DontBackupWithEmptyBootInfoFilePath)
    {
        TString backupFilePath = "";

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, backupFilePath);

        auto sender = runtime.AllocateEdgeActor();

        env.SendBackupTabletBootInfos(sender, S_FALSE);
    }

    Y_UNIT_TEST(BootExternalInFallbackMode)
    {
        TString backupFilePath =
            "BootExternalInFallbackMode.tablet_boot_info_backup.txt";
        bool fallbackMode = false;

        {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, backupFilePath, fallbackMode);

            TTabletStorageInfoPtr expected = CreateTestTabletInfo(
                FakeTablet2,
                TTabletTypes::BlockStorePartition);
            env.HiveState->StorageInfos[FakeTablet2] = expected;

            auto sender = runtime.AllocateEdgeActor();

            auto result = env.SendBootExternalRequest(sender, FakeTablet2, S_OK);
            UNIT_ASSERT(result.StorageInfo);
            UNIT_ASSERT_VALUES_EQUAL(
                FakeTablet2,
                result.StorageInfo->TabletID);
            UNIT_ASSERT_VALUES_EQUAL(1u, result.SuggestedGeneration);

            // Smoke check for background sync (15 seconds should be enough).
            runtime.AdvanceCurrentTime(TDuration::Seconds(15));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(15));

            env.SendBackupTabletBootInfos(sender, S_OK);
        }

        fallbackMode = true;
        {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, backupFilePath, fallbackMode);

            auto sender = runtime.AllocateEdgeActor();

            auto result1 = env.SendBootExternalRequest(sender, FakeTablet2, S_OK);
            UNIT_ASSERT(result1.StorageInfo);
            UNIT_ASSERT_VALUES_EQUAL(
                FakeTablet2,
                result1.StorageInfo->TabletID);
            UNIT_ASSERT_VALUES_EQUAL(1u, result1.SuggestedGeneration);

            // unknown tablet should not be booted
            auto result2 = env.SendBootExternalRequest(
                sender, 0xdeadbeaf, E_REJECTED);
            UNIT_ASSERT(!result2.StorageInfo);

            auto result3 = env.SendBootExternalRequest(sender, FakeTablet2, S_OK);
            UNIT_ASSERT(result3.StorageInfo);
            UNIT_ASSERT_VALUES_EQUAL(
                FakeTablet2,
                result3.StorageInfo->TabletID);
            // suggested generation should be incremented after last boot
            UNIT_ASSERT_VALUES_EQUAL(2u, result3.SuggestedGeneration);
        }
    }

    Y_UNIT_TEST(ShouldHandleTryLaterStatusDuringBoot)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, EExternalBootOptions::TRYLATER);

        auto sender = runtime.AllocateEdgeActor();

        auto result = env.SendBootExternalRequest(
            sender,
            FakeTablet2,
            MAKE_KIKIMR_ERROR(NKikimrProto::TRYLATER));
        UNIT_ASSERT(!result.StorageInfo);
    }

    Y_UNIT_TEST(ShouldFailBootIfTimesout)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, EExternalBootOptions::IGNORE);

        auto sender = runtime.AllocateEdgeActor();

        auto result = env.SendBootExternalRequest(
            sender,
            FakeTablet2,
            MAKE_KIKIMR_ERROR(NKikimrProto::TRYLATER),
            TDuration::Seconds(1));
        UNIT_ASSERT(!result.StorageInfo);
    }

    Y_UNIT_TEST(ShouldHandleCreateTabletRequest)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, EExternalBootOptions::IGNORE);

        auto sender = runtime.AllocateEdgeActor();

        NKikimrHive::TEvCreateTablet request;

        request.SetOwner(0);
        request.SetOwnerIdx(1);
        request.SetTabletType(TTabletTypes::BlockStoreDiskRegistry);

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvHive::EvCreateTablet) {
                    const auto* msg = ev->Get<TEvHive::TEvCreateTablet>();

                    UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetOwner(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetOwnerIdx(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(
                        msg->Record.GetTabletType(),
                        TTabletTypes::BlockStoreDiskRegistry);

                    runtime.Send(
                        new IEventHandle(
                            ev->Sender,
                            ev->Sender,
                            new TEvHive::TEvCreateTabletReply(
                                NKikimrProto::OK,
                                msg->Record.GetOwner(),
                                msg->Record.GetOwnerIdx(),
                                FakeTablet2),
                                0,
                                ev->Cookie));

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            });


        auto result = env.SendCreateTabletRequest(
            sender,
            FakeHiveTablet,
            request,
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(result.TabletId, FakeTablet2);
    }

    Y_UNIT_TEST(ShouldHandleLookupTabletRequest)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, EExternalBootOptions::IGNORE);

        auto sender = runtime.AllocateEdgeActor();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvHive::EvLookupTablet) {
                    const auto* msg = ev->Get<TEvHive::TEvLookupTablet>();

                    UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetOwner(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetOwnerIdx(), 1);

                    runtime.Send(
                        new IEventHandle(
                            ev->Sender,
                            ev->Sender,
                            new TEvHive::TEvCreateTabletReply(
                                NKikimrProto::OK,
                                msg->Record.GetOwner(),
                                msg->Record.GetOwnerIdx(),
                                FakeTablet2),
                                0,
                                ev->Cookie));

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            });


        auto result = env.SendLookupTabletRequest(
            sender,
            FakeHiveTablet,
            0,
            1,
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(result.TabletId, FakeTablet2);
    }

    Y_UNIT_TEST(ShouldAckAllLookupRequestTillNextCreateRequest)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, EExternalBootOptions::IGNORE);

        auto sender = runtime.AllocateEdgeActor();

        NKikimrHive::TEvCreateTablet request;

        request.SetOwner(0);
        request.SetOwnerIdx(1);
        request.SetTabletType(TTabletTypes::BlockStoreDiskRegistry);

        bool sentCreateReply = false;
        auto sendReply = [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
            const auto* msg = ev->Get<TEvHive::TEvCreateTablet>();

            runtime.Send(
                new IEventHandle(
                    ev->Sender,
                    ev->Sender,
                    new TEvHive::TEvCreateTabletReply(
                        NKikimrProto::OK,
                        msg->Record.GetOwner(),
                        msg->Record.GetOwnerIdx(),
                        FakeTablet2),
                        0,
                        ev->Cookie));
                sentCreateReply = true;
        };

        ui32 lookupCnt = 0;
        TAutoPtr<IEventHandle> savedEvent;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvHive::EvCreateTablet: {
                        const auto* msg = ev->Get<TEvHive::TEvCreateTablet>();

                        UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetOwner(), 0);
                        UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetOwnerIdx(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(
                            msg->Record.GetTabletType(),
                            TTabletTypes::BlockStoreDiskRegistry);

                        if (lookupCnt != 10) {
                            savedEvent = ev.Release();
                        } else if (!sentCreateReply) {
                            sendReply(runtime, ev);
                        }

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    case TEvHiveProxy::EvLookupTabletRequest: {
                        if (++lookupCnt == 10 && !sentCreateReply && savedEvent) {
                            sendReply(runtime, savedEvent);
                        }

                        return TTestActorRuntime::EEventAction::PROCESS;
                    }
                    case TEvHive::EvLookupTablet: {
                        const auto* msg = ev->Get<TEvHive::TEvLookupTablet>();

                        runtime.Send(
                            new IEventHandle(
                                ev->Sender,
                                ev->Sender,
                                new TEvHive::TEvCreateTabletReply(
                                    NKikimrProto::OK,
                                    msg->Record.GetOwner(),
                                    msg->Record.GetOwnerIdx(),
                                    FakeTablet2),
                                    0,
                                    ev->Cookie));

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        env.SendCreateTabletRequestAsync(
            sender,
            FakeHiveTablet,
            request);

        for (uint32_t i = 0; i < 10; ++i) {
            env.SendLookupTabletRequestAsync(
                sender,
                FakeHiveTablet,
                0,
                1);
        }

        env.SendCreateTabletRequestAsync(
            sender,
            FakeHiveTablet,
            request);

        {
            auto ev =
                runtime.GrabEdgeEvent<TEvHiveProxy::TEvCreateTabletResponse>(
                    sender);
            UNIT_ASSERT(ev);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->GetStatus(), S_OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletId, FakeTablet2);
        }

        for (uint32_t i = 0; i < 10; ++i) {
            auto ev =
                runtime.GrabEdgeEvent<TEvHiveProxy::TEvLookupTabletResponse>(
                    sender);
            UNIT_ASSERT(ev);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->GetStatus(), S_OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletId, FakeTablet2);
        }

        {
            auto ev =
                runtime.GrabEdgeEvent<TEvHiveProxy::TEvCreateTabletResponse>(
                    sender,
                    TDuration::Seconds(1));
            UNIT_ASSERT(!ev);
        }
    }

    Y_UNIT_TEST(ShouldRejectLookupEventsOnPipeReset)
    {
        constexpr ui32 LookupCount = 10;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, EExternalBootOptions::IGNORE);
        TActorId sender = runtime.AllocateEdgeActor();

        ui64 lookupCount = 0;
        ui64 hiveLookupCount = 0;
        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                    case TEvHive::EvLookupTablet: {
                        ++hiveLookupCount;
                        return true;
                    }
                    case TEvHiveProxy::EvLookupTabletRequest: {
                        ++lookupCount;
                        return false;
                    }
                }

                return false;
            });

        for (ui32 i = 0; i < LookupCount; ++i) {
            env.SendLookupTabletRequestAsync(sender, FakeHiveTablet, 0, 1);
        }
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return hiveLookupCount && lookupCount == LookupCount;
            }});

        UNIT_ASSERT_VALUES_EQUAL(1, hiveLookupCount);

        env.EnableTabletResolverScheduling();
        env.RebootHive();

        for (ui32 i = 0; i < LookupCount; ++i) {
            auto ev =
                runtime.GrabEdgeEvent<TEvHiveProxy::TEvLookupTabletResponse>(
                    sender);
            UNIT_ASSERT(ev);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, msg->GetStatus());
        }

        auto sendReply =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            const auto* msg = ev->Get<TEvHive::TEvLookupTablet>();
            runtime.Send(new IEventHandle(
                ev->Sender,
                ev->Sender,
                new TEvHive::TEvCreateTabletReply(
                    NKikimrProto::OK,
                    msg->Record.GetOwner(),
                    msg->Record.GetOwnerIdx(),
                    FakeTablet2),
                0,
                ev->Cookie));
        };

        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                    case TEvHive::EvLookupTablet: {
                        sendReply(runtime, ev);
                        return true;
                    }
                }

                return false;
            });

        for (ui32 i = 0; i < LookupCount; ++i) {
            const auto& response =
                env.SendLookupTabletRequest(sender, FakeHiveTablet, 0, 1, S_OK);
            UNIT_ASSERT_VALUES_EQUAL(FakeTablet2, response.TabletId);
        }
    }

    Y_UNIT_TEST(ShouldUseTenantHiveIfConfigured)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, "", false, false, true);

        auto sender = runtime.AllocateEdgeActor();

        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.TenantHiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);

        env.SendUnlockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.TenantHiveState->LockedTablets[FakeTablet2],
            TActorId());
    }

    Y_UNIT_TEST(ShouldNotResendMetricsIfTheyUpdatedBeforeSending)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, "", false, false, true);

        ui32 hiveMessages = 0;
        ui32 wakeups = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHive::EvTabletMetrics: {
                        ++hiveMessages;
                        break;
                    }
                    case TEvHiveProxyPrivate::EvSendTabletMetrics: {
                        ++wakeups;
                        break;
                    }
                    default: break;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });

        auto sender = runtime.AllocateEdgeActor();

        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_EQUAL(
            env.TenantHiveState->LockedTablets[FakeTablet2],
            env.HiveProxyActorId);

        env.SendTabletMetricRequestAsync(sender, FakeTablet2);
        env.SendTabletMetricRequestAsync(sender, FakeTablet2);

        runtime.DispatchEvents({}, TDuration::Seconds(6));
        UNIT_ASSERT_VALUES_EQUAL(1, hiveMessages);
        UNIT_ASSERT_VALUES_EQUAL(1, wakeups);
    }

    Y_UNIT_TEST(ShouldReportHiveReconnectTime)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto sender = runtime.AllocateEdgeActor();

        auto counter = env.Runtime.GetAppData(0).Counters
            ->GetCounter("HiveReconnectTime", true);

        env.SendLockRequest(sender, FakeTablet2);
        UNIT_ASSERT_VALUES_UNEQUAL(0, counter->Val());

        auto oldVal = counter->Val();

        int hiveLockRequests = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvHive::EvLockTabletExecution) {
                    ++hiveLockRequests;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });

        env.EnableTabletResolverScheduling();
        env.RebootHive();

        while (!hiveLockRequests) {
            // Pipe to hive may take a long time to connect
            // Wait until hive receives the lock request
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

        runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);

        // Rebooting hive should reconnect the lock
        UNIT_ASSERT_GT(counter->Val(), oldVal);
    }

    Y_UNIT_TEST(ShouldLoadTabletBootInfoAtStartup)
    {
        TString backupFilePath =
            "BootExternal.tablet_boot_info_backup.txt";

        NKikimr::TTabletStorageInfoPtr storageInfo;

        TVector<TVector<ui64>> expectedGroupIds;
        {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, backupFilePath, false);

            TTabletStorageInfoPtr expected = CreateTestTabletInfo(
                FakeTablet2,
                TTabletTypes::BlockStorePartition);
            env.HiveState->StorageInfos[FakeTablet2] = expected;

            auto sender = runtime.AllocateEdgeActor();

            auto result =
                env.SendBootExternalRequest(sender, FakeTablet2, S_OK);
            UNIT_ASSERT(result.StorageInfo);
            UNIT_ASSERT_VALUES_EQUAL(FakeTablet2, result.StorageInfo->TabletID);
            UNIT_ASSERT_VALUES_EQUAL(1u, result.SuggestedGeneration);

            // Smoke check for background sync (15 seconds should be enough).
            runtime.AdvanceCurrentTime(TDuration::Seconds(15));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(15));

            for (auto& channel: result.StorageInfo->Channels) {
                expectedGroupIds.emplace_back();
                for (auto& historyEntry: channel.History) {
                    expectedGroupIds.back().emplace_back(
                        historyEntry.GroupID);
                }
            }
            storageInfo = result.StorageInfo;
            env.SendBackupTabletBootInfos(sender, S_OK);
        }

        {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, backupFilePath, false);

            auto sender = runtime.AllocateEdgeActor();

            auto result = env.SendListTabletBootInfoBackups(sender, S_OK);
            UNIT_ASSERT_VALUES_EQUAL(1, result.TabletBootInfos.size());
            UNIT_ASSERT_VALUES_EQUAL(
                FakeTablet2,
                result.TabletBootInfos[0].StorageInfo->TabletID);
            TVector<TVector<ui64>> actualGroupIds;
            for (auto& channel: result.TabletBootInfos[0].StorageInfo->Channels)
            {
                actualGroupIds.emplace_back();
                for (auto& historyEntry: channel.History) {
                    actualGroupIds.back().emplace_back(historyEntry.GroupID);
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(expectedGroupIds, actualGroupIds);
        }
    }
}

}   // namespace NCloud::NStorage
