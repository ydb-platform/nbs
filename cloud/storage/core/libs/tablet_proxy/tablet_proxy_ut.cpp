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

    // Sends the event to the tablet proxy.
    void Send(const TActorId& sender, IEventBase* ev)
    {
        Runtime.Send(new IEventHandle(Proxy, sender, ev));
    }

    void Forward(
        const TActorId& forwarder,
        ui64 tabletId,
        IEventHandlePtr request)
    {
        Forward(forwarder, tabletId, std::move(request), false);
    }

    void Forward(
        const TActorId& forwarder,
        ui64 tabletId,
        IEventHandlePtr request,
        bool subscribe)
    {
        Send(
            forwarder,
            new TEvTabletProxy::TEvForward(
                tabletId,
                std::move(request),
                subscribe));
    }

    static IEventHandlePtr
    TestMessage(const TActorId& sender, const TString& body, ui64 cookie)
    {
        return std::make_unique<IEventHandle>(
            TActorId(),
            sender,
            new TEvents::TEvBlob(body),
            0,   // flags
            cookie);
    }

    // A request as it arrives from another node.
    static IEventHandlePtr SerializedTestMessage(
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

    template <typename TEvent>
    auto GrabEvent(const TActorId& recipient)
    {
        return GrabEvent<TEvent>(recipient, GrabTimeout);
    }

    // Null if no such event reaches the recipient within the timeout.
    template <typename TEvent>
    auto GrabEvent(const TActorId& recipient, TDuration timeout)
    {
        return Runtime.GrabEdgeEvent<TEvent>(recipient, timeout);
    }

    void WaitForHeld(ui64 tabletId, size_t count)
    {
        WaitFor([&] { return Tablets[tabletId]->Held.size() == count; });
    }

    // Makes the tablet answer the requests it holds.
    void Release(ui64 tabletId)
    {
        Runtime.Send(new IEventHandle(
            Tablets[tabletId]->ActorId,
            TActorId(),
            new TEvRelease));
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
        const TActorId first = env.Runtime.AllocateEdgeActor();
        const TActorId second = env.Runtime.AllocateEdgeActor();

        // Two senders with the same cookie, answered together.
        env.Forward(first, TabletA, TEnv::TestMessage(first, "hold", 1));
        env.Forward(second, TabletA, TEnv::TestMessage(second, "hold", 1));
        env.WaitForHeld(TabletA, 2);
        env.Release(TabletA);
        auto reply = env.GrabEvent<TEvents::TEvBlob>(first);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("held", reply->Get()->Blob);
        UNIT_ASSERT_VALUES_EQUAL(1, reply->Cookie);

        reply = env.GrabEvent<TEvents::TEvBlob>(second);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("held", reply->Get()->Blob);
        UNIT_ASSERT_VALUES_EQUAL(1, reply->Cookie);

        // A request and a reply as they arrive from another node.
        env.Forward(first, TabletB, TEnv::TestMessage(first, "serialized", 2));
        reply = env.GrabEvent<TEvents::TEvBlob>(first);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("serialized", reply->Get()->Blob);
        UNIT_ASSERT_VALUES_EQUAL(2, reply->Cookie);

        env.Forward(first, TabletA, TEnv::SerializedTestMessage(first, "a", 3));
        reply = env.GrabEvent<TEvents::TEvBlob>(first);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("a", reply->Get()->Blob);
        UNIT_ASSERT_VALUES_EQUAL(3, reply->Cookie);

        // The reply goes to the request's sender, not to the forwarder.
        env.Forward(first, TabletA, TEnv::TestMessage(second, "b", 4));
        reply = env.GrabEvent<TEvents::TEvBlob>(second);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("b", reply->Get()->Blob);
        UNIT_ASSERT_VALUES_EQUAL(4, reply->Cookie);

        UNIT_ASSERT(!env.GrabEvent<TEvents::TEvBlob>(
            first,
            TDuration::Seconds(1)));

        // One pipe per tablet.
        UNIT_ASSERT_VALUES_EQUAL(1, env.Tablets[TabletA]->Connects);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Tablets[TabletB]->Connects);
    }

    Y_UNIT_TEST(ShouldNotifyRequestsWhenTabletRestarts)
    {
        TEnv env;
        const TActorId first = env.Runtime.AllocateEdgeActor();
        const TActorId second = env.Runtime.AllocateEdgeActor();

        // A notice per request in flight, with its cookie; a completed
        // request and another tablet's requests are not affected.
        env.Forward(second, TabletA, TEnv::TestMessage(second, "a", 3));
        auto reply = env.GrabEvent<TEvents::TEvBlob>(second);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("a", reply->Get()->Blob);
        UNIT_ASSERT_VALUES_EQUAL(3, reply->Cookie);

        env.Forward(first, TabletA, TEnv::TestMessage(first, "hold", 1));
        env.Forward(second, TabletA, TEnv::TestMessage(second, "hold", 2));
        env.Forward(first, TabletB, TEnv::TestMessage(first, "hold", 5));
        env.WaitForHeld(TabletA, 2);
        env.WaitForHeld(TabletB, 1);

        env.RebootTablet(TabletA);
        auto disconnected =
            env.GrabEvent<TEvTabletProxy::TEvDisconnected>(first);
        UNIT_ASSERT(disconnected);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            disconnected->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(TabletA, disconnected->Get()->TabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, disconnected->Cookie);

        disconnected = env.GrabEvent<TEvTabletProxy::TEvDisconnected>(second);
        UNIT_ASSERT(disconnected);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            disconnected->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(TabletA, disconnected->Get()->TabletId);
        UNIT_ASSERT_VALUES_EQUAL(2, disconnected->Cookie);

        UNIT_ASSERT(!env.GrabEvent<TEvTabletProxy::TEvDisconnected>(
            first,
            TDuration::Seconds(1)));
        UNIT_ASSERT(!env.GrabEvent<TEvTabletProxy::TEvDisconnected>(
            second,
            TDuration::Seconds(1)));
        env.Release(TabletB);
        reply = env.GrabEvent<TEvents::TEvBlob>(first);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("held", reply->Get()->Blob);
        UNIT_ASSERT_VALUES_EQUAL(5, reply->Cookie);

        // A reply to a request of the old pipe is dropped. The proxy numbers
        // requests from 1 in forward order, so the first held one is 2.
        env.Runtime.Send(new IEventHandle(
            env.Proxy,
            env.Tablets[TabletA]->ActorId,
            new TEvents::TEvBlob("late"),
            0,   // flags
            2));
        UNIT_ASSERT(!env.GrabEvent<TEvents::TEvBlob>(
            first,
            TDuration::Seconds(1)));

        // The next request opens a new pipe.
        env.Forward(first, TabletA, TEnv::TestMessage(first, "a", 3));
        reply = env.GrabEvent<TEvents::TEvBlob>(first);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("a", reply->Get()->Blob);
        UNIT_ASSERT_VALUES_EQUAL(3, reply->Cookie);

        UNIT_ASSERT_VALUES_EQUAL(2, env.Tablets[TabletA]->Connects);
    }

    Y_UNIT_TEST(ShouldNotifySenderWhenTabletIsUnreachable)
    {
        TEnv env;
        const TActorId client = env.Runtime.AllocateEdgeActor();

        env.Forward(client, MissingTablet, TEnv::TestMessage(client, "a", 1));
        auto disconnected =
            env.GrabEvent<TEvTabletProxy::TEvDisconnected>(client);
        UNIT_ASSERT(disconnected);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            disconnected->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(MissingTablet, disconnected->Get()->TabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, disconnected->Cookie);

        env.Forward(client, MissingTablet, TEnv::TestMessage(client, "b", 2));
        disconnected = env.GrabEvent<TEvTabletProxy::TEvDisconnected>(client);
        UNIT_ASSERT(disconnected);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            disconnected->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(MissingTablet, disconnected->Get()->TabletId);
        UNIT_ASSERT_VALUES_EQUAL(2, disconnected->Cookie);
    }

    Y_UNIT_TEST(ShouldClosePipeAfterIdleTimeout)
    {
        TEnv env({.PipeInactivityTimeout = IdleTimeout});
        const TActorId client = env.Runtime.AllocateEdgeActor();
        const auto& tablet = *env.Tablets[TabletA];

        // A request halfway through the timeout restarts it.
        env.Forward(client, TabletA, TEnv::TestMessage(client, "a", 1));
        auto reply = env.GrabEvent<TEvents::TEvBlob>(client);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("a", reply->Get()->Blob);

        env.Pass(IdleTimeout / 2);
        env.Forward(client, TabletA, TEnv::TestMessage(client, "b", 2));
        reply = env.GrabEvent<TEvents::TEvBlob>(client);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("b", reply->Get()->Blob);

        env.WaitFor([&] { return tablet.Disconnects == 1; });
        UNIT_ASSERT(
            tablet.LastDisconnected >= tablet.LastReceived + IdleTimeout);

        env.Forward(client, TabletA, TEnv::TestMessage(client, "c", 3));
        reply = env.GrabEvent<TEvents::TEvBlob>(client);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("c", reply->Get()->Blob);

        UNIT_ASSERT_VALUES_EQUAL(2, tablet.Connects);
    }

    Y_UNIT_TEST(ShouldIgnoreIdleCheckOfClosedPipe)
    {
        TEnv env({.PipeInactivityTimeout = IdleTimeout});
        const TActorId client = env.Runtime.AllocateEdgeActor();
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
        env.Forward(client, TabletA, TEnv::TestMessage(client, "a", 1));
        auto reply = env.GrabEvent<TEvents::TEvBlob>(client);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("a", reply->Get()->Blob);

        env.RebootTablet(TabletA);
        env.Forward(client, TabletA, TEnv::TestMessage(client, "b", 2));
        reply = env.GrabEvent<TEvents::TEvBlob>(client);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("b", reply->Get()->Blob);

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
        const TActorId client = env.Runtime.AllocateEdgeActor();
        const auto& tablet = *env.Tablets[TabletA];

        env.Forward(client, TabletA, TEnv::TestMessage(client, "a", 1));
        auto reply = env.GrabEvent<TEvents::TEvBlob>(client);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("a", reply->Get()->Blob);

        env.Forward(client, TabletA, TEnv::TestMessage(client, "hold", 2));
        env.WaitForHeld(TabletA, 1);

        env.Pass(IdleTimeout * 2);
        UNIT_ASSERT_VALUES_EQUAL(0, tablet.Disconnects);

        env.Release(TabletA);
        reply = env.GrabEvent<TEvents::TEvBlob>(client);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("held", reply->Get()->Blob);

        UNIT_ASSERT_VALUES_EQUAL(1, tablet.Connects);

        // Idle again once the held request completed.
        env.WaitFor([&] { return tablet.Disconnects == 1; });
        UNIT_ASSERT(
            tablet.LastDisconnected >= tablet.LastReceived + IdleTimeout);
    }

    Y_UNIT_TEST(ShouldKeepPipeOnKeepAlive)
    {
        TEnv env({.PipeInactivityTimeout = IdleTimeout});
        const TActorId client = env.Runtime.AllocateEdgeActor();
        const auto& tablet = *env.Tablets[TabletA];

        auto keepAlive = [&]
        {
            env.Send(client, new TEvTabletProxy::TEvKeepAliveRequest(TabletA));
            auto response =
                env.GrabEvent<TEvTabletProxy::TEvKeepAliveResponse>(client);
            UNIT_ASSERT(response);
            return response->Get()->GetStatus();
        };

        // A keep-alive does not open a pipe, a request does.
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, keepAlive());
        env.Forward(client, TabletA, TEnv::TestMessage(client, "a", 1));
        auto reply = env.GrabEvent<TEvents::TEvBlob>(client);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL("a", reply->Get()->Blob);

        // Each keep-alive restarts the idle timeout.
        for (ui32 i = 0; i != 3; ++i) {
            env.Pass(IdleTimeout / 2);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, keepAlive());
        }
        const TInstant lastKeepAlive = env.Runtime.GetCurrentTime();
        UNIT_ASSERT_VALUES_EQUAL(0, tablet.Disconnects);

        env.WaitFor([&] { return tablet.Disconnects == 1; });
        UNIT_ASSERT(tablet.LastDisconnected >= lastKeepAlive + IdleTimeout);

        // The pipe closed idle: nothing to keep.
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, keepAlive());
        UNIT_ASSERT_VALUES_EQUAL(1, tablet.Connects);
    }

    Y_UNIT_TEST(ShouldNotifySendersOnShutdown)
    {
        TEnv env;
        const TActorId first = env.Runtime.AllocateEdgeActor();
        const TActorId second = env.Runtime.AllocateEdgeActor();

        env.Forward(first, TabletA, TEnv::TestMessage(first, "hold", 1));
        env.Forward(second, TabletB, TEnv::TestMessage(second, "hold", 2));
        env.WaitForHeld(TabletA, 1);
        env.WaitForHeld(TabletB, 1);

        env.Send(first, new TEvents::TEvPoisonPill);
        auto disconnected =
            env.GrabEvent<TEvTabletProxy::TEvDisconnected>(first);
        UNIT_ASSERT(disconnected);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            disconnected->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(TabletA, disconnected->Get()->TabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, disconnected->Cookie);

        disconnected = env.GrabEvent<TEvTabletProxy::TEvDisconnected>(second);
        UNIT_ASSERT(disconnected);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            disconnected->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(TabletB, disconnected->Get()->TabletId);
        UNIT_ASSERT_VALUES_EQUAL(2, disconnected->Cookie);

        env.WaitFor([&] {
            return env.Tablets[TabletA]->Disconnects == 1 &&
                   env.Tablets[TabletB]->Disconnects == 1;
        });
    }

    Y_UNIT_TEST(ShouldNotifySubscriberWhenPipeBreaks)
    {
        TEnv env;
        const TActorId subscriber = env.Runtime.AllocateEdgeActor();
        const TActorId other = env.Runtime.AllocateEdgeActor();

        // Subscribed twice: one notice; none for a sender that did not.
        env.Forward(
            subscriber,
            TabletA,
            TEnv::TestMessage(subscriber, "a", 1),
            true);
        UNIT_ASSERT(env.GrabEvent<TEvents::TEvBlob>(subscriber));
        env.Forward(
            subscriber,
            TabletA,
            TEnv::TestMessage(subscriber, "b", 2),
            true);
        UNIT_ASSERT(env.GrabEvent<TEvents::TEvBlob>(subscriber));
        env.Forward(other, TabletA, TEnv::TestMessage(other, "c", 3));
        UNIT_ASSERT(env.GrabEvent<TEvents::TEvBlob>(other));
        env.RebootTablet(TabletA);
        auto closed =
            env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(subscriber);
        UNIT_ASSERT(closed);
        UNIT_ASSERT_VALUES_EQUAL(TabletA, closed->Get()->TabletId);

        UNIT_ASSERT(!env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(
            subscriber,
            TDuration::Seconds(1)));
        UNIT_ASSERT(!env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(
            other,
            TDuration::Seconds(1)));

        // The notice goes to the forwarder, not to the request's sender.
        env.Forward(
            subscriber,
            TabletA,
            TEnv::TestMessage(other, "hold", 4),
            true);
        env.WaitForHeld(TabletA, 1);
        env.RebootTablet(TabletA);
        UNIT_ASSERT(env.GrabEvent<TEvTabletProxy::TEvDisconnected>(other));
        closed = env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(subscriber);
        UNIT_ASSERT(closed);
        UNIT_ASSERT_VALUES_EQUAL(TabletA, closed->Get()->TabletId);

        UNIT_ASSERT(!env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(
            other,
            TDuration::Seconds(1)));

        // A pipe that could not connect.
        env.Forward(
            subscriber,
            MissingTablet,
            TEnv::TestMessage(subscriber, "d", 5),
            true);
        closed = env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(subscriber);
        UNIT_ASSERT(closed);
        UNIT_ASSERT_VALUES_EQUAL(MissingTablet, closed->Get()->TabletId);
    }

    Y_UNIT_TEST(ShouldNotifySubscriberWhenPipeClosesIdleOrProxyStops)
    {
        TEnv env({.PipeInactivityTimeout = IdleTimeout});
        const TActorId subscriber = env.Runtime.AllocateEdgeActor();
        const auto& tablet = *env.Tablets[TabletA];

        // Each pipe needs its own subscription.
        for (ui32 i = 1; i != 3; ++i) {
            env.Forward(
                subscriber,
                TabletA,
                TEnv::TestMessage(subscriber, "a", i),
                true);
            UNIT_ASSERT(env.GrabEvent<TEvents::TEvBlob>(subscriber));
            UNIT_ASSERT_VALUES_EQUAL(i, tablet.Connects);

            auto closed =
                env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(subscriber);
            UNIT_ASSERT(closed);
            UNIT_ASSERT_VALUES_EQUAL(TabletA, closed->Get()->TabletId);
            env.WaitFor([&] { return tablet.Disconnects == i; });
            UNIT_ASSERT(
                tablet.LastDisconnected >= tablet.LastReceived + IdleTimeout);
        }
        UNIT_ASSERT(!env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(
            subscriber,
            IdleTimeout * 2));

        // The proxy stops with an open pipe.
        env.Forward(
            subscriber,
            TabletA,
            TEnv::TestMessage(subscriber, "b", 3),
            true);
        UNIT_ASSERT(env.GrabEvent<TEvents::TEvBlob>(subscriber));
        env.Send(subscriber, new TEvents::TEvPoisonPill);
        auto closed =
            env.GrabEvent<TEvTabletProxy::TEvPipeClosed>(subscriber);
        UNIT_ASSERT(closed);
        UNIT_ASSERT_VALUES_EQUAL(TabletA, closed->Get()->TabletId);
    }
}

}   // namespace NCloud::NStorage
