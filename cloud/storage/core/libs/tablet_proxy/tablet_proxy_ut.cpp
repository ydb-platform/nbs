#include "tablet_proxy.h"

#include <cloud/storage/core/libs/api/tablet_proxy.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/tabletid.h>
#include <contrib/ydb/core/tablet_flat/tablet_flat_executed.h>
#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/event_pb.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>
#include <memory>
#include <utility>

namespace NCloud::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui64 HiveId = MakeDefaultHiveID(0);
const ui64 TabletA = MakeTabletID(0, HiveId, 1);
const ui64 TabletB = MakeTabletID(0, HiveId, 2);
const ui64 MissingTablet = MakeTabletID(0, HiveId, 99);

const TDuration IdleTimeout = TDuration::Seconds(5);
const TDuration GrabTimeout = TDuration::Hours(1);

////////////////////////////////////////////////////////////////////////////////

struct TEvRelease
    : public TEventLocal<TEvRelease, EventSpaceBegin(TEvents::ES_PRIVATE)>
{
};

struct TTabletState
{
    TActorId ActorId;
    ui32 Connects = 0;
    ui32 Disconnects = 0;
    TInstant LastReceived;
    TInstant LastDisconnected;
    TVector<std::pair<TActorId, ui64>> Held;
};

// Echoes blobs back, holds "hold" ones until released.
class TFakeTablet final
    : public TActor<TFakeTablet>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
private:
    const std::shared_ptr<TTabletState> State;

public:
    TFakeTablet(
            const TActorId& owner,
            TTabletStorageInfo* info,
            std::shared_ptr<TTabletState> state)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, owner, nullptr)
        , State(std::move(state))
    {}

private:
    void DefaultSignalTabletActive(const TActorContext&) override
    {}

    void OnActivateExecutor(const TActorContext& ctx) override
    {
        State->ActorId = SelfId();
        State->Held.clear();
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
            case TEvTabletPipe::EvServerConnected:
                ++State->Connects;
                break;
            case TEvTabletPipe::EvServerDisconnected:
                ++State->Disconnects;
                State->LastDisconnected = TActivationContext::Now();
                break;

            HFunc(TEvents::TEvBlob, HandleBlob);
            HFunc(TEvRelease, HandleRelease);

            default:
                HandleDefaultEvents(ev, SelfId());
        }
    }

    void HandleBlob(const TEvents::TEvBlob::TPtr& ev, const TActorContext& ctx)
    {
        const TString body = ev->Get()->Blob;
        State->LastReceived = ctx.Now();
        if (body == "hold") {
            State->Held.emplace_back(ev->Sender, ev->Cookie);
            return;
        }

        if (body != "serialized") {
            ctx.Send(ev->Sender, new TEvents::TEvBlob(body), 0, ev->Cookie);
            return;
        }

        TEvents::TEvBlob reply(body);
        TAllocChunkSerializer serializer;
        reply.SerializeToArcadiaStream(&serializer);
        ctx.Send(new IEventHandle(
            TEvents::TEvBlob::EventType,
            0,   // flags
            ev->Sender,
            SelfId(),
            serializer.Release(reply.CreateSerializationInfo()),
            ev->Cookie));
    }

    void HandleRelease(const TEvRelease::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        for (const auto& [sender, cookie]: State->Held) {
            ctx.Send(sender, new TEvents::TEvBlob("held"), 0, cookie);
        }
        State->Held.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TTestBasicRuntime Runtime{1};
    TActorId Edge;
    TActorId Proxy;
    THashMap<ui64, std::shared_ptr<TTabletState>> Tablets;

    explicit TEnv(TTabletProxyConfig config = {})
    {
        Runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
            {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });
        SetupTabletServices(Runtime);

        Edge = Runtime.AllocateEdgeActor();
        config.LogComponent = NKikimrServices::PIPE_CLIENT;
        Proxy = Runtime.Register(
            CreateTabletProxy(std::move(config)).release());
        Runtime.RegisterService(MakeTabletProxyServiceId(), Proxy);

        for (ui64 tabletId: {TabletA, TabletB}) {
            auto state = std::make_shared<TTabletState>();
            Tablets[tabletId] = state;
            CreateTestBootstrapper(
                Runtime,
                CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
                [=] (const TActorId& owner, TTabletStorageInfo* info) {
                    return new TFakeTablet(owner, info, state);
                });
            WaitFor([&] { return bool(state->ActorId); });
        }
    }

    void WaitFor(const std::function<bool()>& condition)
    {
        TDispatchOptions options;
        options.CustomFinalCondition = condition;
        Runtime.DispatchEvents(options);
        UNIT_ASSERT(condition());
    }

    void Send(IEventBase* ev, TActorId sender = {})
    {
        Runtime.Send(new IEventHandle(Proxy, sender ? sender : Edge, ev));
    }

    void Forward(ui64 tabletId, IEventHandlePtr request)
    {
        const TActorId sender = request->Sender;
        Send(
            new TEvTabletProxy::TEvForward(tabletId, std::move(request)),
            sender);
    }

    void Forward(ui64 tabletId, const TString& body, ui64 cookie)
    {
        Forward(tabletId, MakeRequest(Edge, body, cookie));
    }

    static IEventHandlePtr
    MakeRequest(const TActorId& sender, const TString& body, ui64 cookie)
    {
        return std::make_unique<IEventHandle>(
            TActorId(),
            sender,
            new TEvents::TEvBlob(body),
            0,   // flags
            cookie);
    }

    // A request as it arrives from another node.
    static IEventHandlePtr MakeSerializedRequest(
        const TActorId& sender,
        const TString& body,
        ui64 cookie)
    {
        TEvents::TEvBlob blob(body);
        TAllocChunkSerializer serializer;
        blob.SerializeToArcadiaStream(&serializer);
        return std::make_unique<IEventHandle>(
            TEvents::TEvBlob::EventType,
            0,   // flags
            TActorId(),
            sender,
            serializer.Release(blob.CreateSerializationInfo()),
            cookie);
    }

    TString GrabReply(ui64 cookie, TActorId edge = {})
    {
        auto ev = Runtime.GrabEdgeEvent<TEvents::TEvBlob>(
            edge ? edge : Edge,
            GrabTimeout);
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(cookie, ev->Cookie);
        return ev->Get()->Blob;
    }

    // The tablet id and the cookie of the next disconnect notice.
    std::pair<ui64, ui64> GrabDisconnected(TActorId edge = {})
    {
        auto ev = Runtime.GrabEdgeEvent<TEvTabletProxy::TEvDisconnected>(
            edge ? edge : Edge,
            GrabTimeout);
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, ev->Get()->GetStatus());
        return {ev->Get()->TabletId, ev->Cookie};
    }

    // No event of @p TEvent reaches the edge within @p timeout.
    template <typename TEvent>
    void AssertNone(TDuration timeout, TActorId edge = {})
    {
        auto ev = Runtime.GrabEdgeEvent<TEvent>(edge ? edge : Edge, timeout);
        UNIT_ASSERT_C(!ev, "unexpected " << TEvent::EventType);
    }

    void WaitForHeld(ui64 tabletId, size_t count)
    {
        WaitFor([&] { return Tablets[tabletId]->Held.size() == count; });
    }

    void Release(ui64 tabletId)
    {
        Runtime.Send(
            new IEventHandle(Tablets[tabletId]->ActorId, Edge, new TEvRelease));
    }

    void Pass(TDuration duration)
    {
        auto timer = Runtime.AllocateEdgeActor();
        Runtime.EnableScheduleForActor(timer);
        Runtime.Schedule(
            new IEventHandle(timer, timer, new TEvents::TEvWakeup),
            duration);
        Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(timer);
    }

    void RebootTablet(ui64 tabletId)
    {
        auto& state = *Tablets[tabletId];
        const TActorId old = state.ActorId;
        auto sender = Runtime.AllocateEdgeActor();
        auto guard =
            CreateTabletScheduledEventsGuard({tabletId}, Runtime, sender);
        NKikimr::RebootTablet(Runtime, tabletId, sender);
        WaitFor([&] { return state.ActorId != old; });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTabletProxyTest)
{
    Y_UNIT_TEST(ShouldRouteRepliesToTheirSenders)
    {
        TEnv env;
        const TActorId other = env.Runtime.AllocateEdgeActor();

        // Two senders with the same cookie, answered together.
        env.Forward(TabletA, TEnv::MakeRequest(env.Edge, "hold", 1));
        env.Forward(TabletA, TEnv::MakeRequest(other, "hold", 1));
        env.WaitForHeld(TabletA, 2);
        env.Release(TabletA);
        UNIT_ASSERT_VALUES_EQUAL("held", env.GrabReply(1));
        UNIT_ASSERT_VALUES_EQUAL("held", env.GrabReply(1, other));

        // A request and a reply as they arrive from another node.
        env.Forward(TabletB, "serialized", 2);
        UNIT_ASSERT_VALUES_EQUAL("serialized", env.GrabReply(2));
        env.Forward(TabletA, TEnv::MakeSerializedRequest(env.Edge, "a", 3));
        UNIT_ASSERT_VALUES_EQUAL("a", env.GrabReply(3));

        // The reply goes to the request's sender, not to the forwarder.
        env.Send(new TEvTabletProxy::TEvForward(
            TabletA,
            TEnv::MakeRequest(other, "b", 4)));
        UNIT_ASSERT_VALUES_EQUAL("b", env.GrabReply(4, other));

        // One pipe per tablet.
        UNIT_ASSERT_VALUES_EQUAL(1, env.Tablets[TabletA]->Connects);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Tablets[TabletB]->Connects);
    }

    Y_UNIT_TEST(ShouldNotifyRequestsWhenTabletRestarts)
    {
        TEnv env;
        const TActorId other = env.Runtime.AllocateEdgeActor();

        // A notice per request in flight, with its cookie; a completed
        // request and another tablet's requests are not affected.
        env.Forward(TabletA, TEnv::MakeRequest(other, "a", 3));
        UNIT_ASSERT_VALUES_EQUAL("a", env.GrabReply(3, other));
        env.Forward(TabletA, "hold", 1);
        env.Forward(TabletA, TEnv::MakeRequest(other, "hold", 2));
        env.Forward(TabletB, "hold", 5);
        env.WaitForHeld(TabletA, 2);
        env.WaitForHeld(TabletB, 1);

        env.RebootTablet(TabletA);
        UNIT_ASSERT_VALUES_EQUAL(
            std::make_pair(TabletA, 1UL),
            env.GrabDisconnected());
        UNIT_ASSERT_VALUES_EQUAL(
            std::make_pair(TabletA, 2UL),
            env.GrabDisconnected(other));
        env.AssertNone<TEvTabletProxy::TEvDisconnected>(
            TDuration::Seconds(1));
        env.AssertNone<TEvTabletProxy::TEvDisconnected>(
            TDuration::Seconds(1),
            other);
        env.Release(TabletB);
        UNIT_ASSERT_VALUES_EQUAL("held", env.GrabReply(5));

        // A reply to a request of the old pipe is dropped. The proxy numbers
        // requests from 1 in forward order, so the first held one is 2.
        env.Runtime.Send(new IEventHandle(
            env.Proxy,
            env.Tablets[TabletA]->ActorId,
            new TEvents::TEvBlob("late"),
            0,   // flags
            2));
        env.AssertNone<TEvents::TEvBlob>(TDuration::Seconds(1));

        // The next request opens a new pipe.
        env.Forward(TabletA, "a", 3);
        UNIT_ASSERT_VALUES_EQUAL("a", env.GrabReply(3));
        UNIT_ASSERT_VALUES_EQUAL(2, env.Tablets[TabletA]->Connects);
    }

    Y_UNIT_TEST(ShouldNotifySenderWhenTabletIsUnreachable)
    {
        TEnv env;

        env.Forward(MissingTablet, "a", 1);
        UNIT_ASSERT_VALUES_EQUAL(
            std::make_pair(MissingTablet, 1UL),
            env.GrabDisconnected());

        env.Forward(MissingTablet, "b", 2);
        UNIT_ASSERT_VALUES_EQUAL(
            std::make_pair(MissingTablet, 2UL),
            env.GrabDisconnected());
    }

    Y_UNIT_TEST(ShouldClosePipeAfterIdleTimeout)
    {
        TEnv env({.PipeInactivityTimeout = IdleTimeout});
        const auto& tablet = *env.Tablets[TabletA];

        // A request halfway through the timeout restarts it.
        env.Forward(TabletA, "a", 1);
        UNIT_ASSERT_VALUES_EQUAL("a", env.GrabReply(1));
        env.Pass(IdleTimeout / 2);
        env.Forward(TabletA, "b", 2);
        UNIT_ASSERT_VALUES_EQUAL("b", env.GrabReply(2));

        env.WaitFor([&] { return tablet.Disconnects == 1; });
        UNIT_ASSERT(
            tablet.LastDisconnected >= tablet.LastReceived + IdleTimeout);

        env.Forward(TabletA, "c", 3);
        UNIT_ASSERT_VALUES_EQUAL("c", env.GrabReply(3));
        UNIT_ASSERT_VALUES_EQUAL(2, tablet.Connects);
    }

    Y_UNIT_TEST(ShouldIgnoreIdleCheckOfClosedPipe)
    {
        TEnv env({.PipeInactivityTimeout = IdleTimeout});
        const auto& tablet = *env.Tablets[TabletA];

        ui32 wakeups = 0;
        env.Runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
            if (ev->Recipient == env.Proxy &&
                ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType)
            {
                ++wakeups;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // The idle check of the first pipe is pending when the pipe breaks
        // and the next request opens another.
        env.Forward(TabletA, "a", 1);
        UNIT_ASSERT_VALUES_EQUAL("a", env.GrabReply(1));
        env.RebootTablet(TabletA);
        env.Forward(TabletA, "b", 2);
        UNIT_ASSERT_VALUES_EQUAL("b", env.GrabReply(2));
        UNIT_ASSERT_VALUES_EQUAL(2, tablet.Connects);

        // The stale check does not touch the new pipe, whose own check closes
        // it: two checks in all.
        const ui32 disconnects = tablet.Disconnects;
        env.WaitFor([&] { return tablet.Disconnects == disconnects + 1; });
        UNIT_ASSERT(
            tablet.LastDisconnected >= tablet.LastReceived + IdleTimeout);
        UNIT_ASSERT_VALUES_EQUAL(2, wakeups);
    }

    Y_UNIT_TEST(ShouldKeepPipeWithRequestInflight)
    {
        TEnv env({.PipeInactivityTimeout = IdleTimeout});
        const auto& tablet = *env.Tablets[TabletA];

        env.Forward(TabletA, "a", 1);
        UNIT_ASSERT_VALUES_EQUAL("a", env.GrabReply(1));
        env.Forward(TabletA, "hold", 2);
        env.WaitForHeld(TabletA, 1);

        env.Pass(IdleTimeout * 2);
        UNIT_ASSERT_VALUES_EQUAL(0, tablet.Disconnects);

        env.Release(TabletA);
        UNIT_ASSERT_VALUES_EQUAL("held", env.GrabReply(2));
        UNIT_ASSERT_VALUES_EQUAL(1, tablet.Connects);

        // Idle again once the held request completed.
        env.WaitFor([&] { return tablet.Disconnects == 1; });
        UNIT_ASSERT(
            tablet.LastDisconnected >= tablet.LastReceived + IdleTimeout);
    }

    Y_UNIT_TEST(ShouldNotifySendersOnShutdown)
    {
        TEnv env;

        env.Forward(TabletA, "hold", 1);
        env.Forward(TabletB, "hold", 2);
        env.WaitForHeld(TabletA, 1);
        env.WaitForHeld(TabletB, 1);

        env.Runtime.Send(
            new IEventHandle(env.Proxy, env.Edge, new TEvents::TEvPoisonPill));
        TVector<std::pair<ui64, ui64>> notices{
            env.GrabDisconnected(),
            env.GrabDisconnected()};
        Sort(notices);
        const TVector<std::pair<ui64, ui64>> expected{
            {TabletA, 1},
            {TabletB, 2}};
        UNIT_ASSERT_VALUES_EQUAL(expected, notices);
        env.WaitFor([&] {
            return env.Tablets[TabletA]->Disconnects == 1 &&
                   env.Tablets[TabletB]->Disconnects == 1;
        });
    }
}

}   // namespace NCloud::NStorage
