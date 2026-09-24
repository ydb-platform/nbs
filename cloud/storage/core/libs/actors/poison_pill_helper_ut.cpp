#include "poison_pill_helper.h"

#include <contrib/ydb/core/testlib/actors/test_runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMyTestEnv final
{
public:
    TActorId Sender;

private:
    TTestActorRuntime Runtime;

public:
    TMyTestEnv()
    {
        NKikimr::SetupTabletServices(Runtime);
        Sender = Runtime.AllocateEdgeActor();
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = Runtime.Register(actor.release());
        Runtime.EnableScheduleForActor(actorId);

        return actorId;
    }

    TActorId AllocateEdgeActor()
    {
        return Runtime.AllocateEdgeActor();
    }

    void Send(const TActorId& recipient, IEventBasePtr event, ui64 cookie)
    {
        Send(Sender, recipient, std::move(event), cookie);
    }

    void Send(
        const TActorId& sender,
        const TActorId& recipient,
        IEventBasePtr event,
        ui64 cookie)
    {
        Runtime.Send(
            new IEventHandle(recipient, sender, event.release(), 0, cookie));
    }

    void DispatchEvents(TDuration timeout)
    {
        Runtime.DispatchEvents(TDispatchOptions(), timeout);
    }

    TAutoPtr<IEventHandle> GrabPoisonTakenEvent(TDuration timeout)
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TEvents::TEvPoisonTaken>(handle, timeout);
        return handle;
    }

    TEvents::TEvPoisonTaken::TPtr GrabPoisonTakenEvent(
        const TActorId& recipient,
        TDuration timeout)
    {
        return Runtime.GrabEdgeEventRethrow<TEvents::TEvPoisonTaken>(
            recipient,
            timeout);
    }

    TTestActorRuntimeBase& AccessRuntime()
    {
        return Runtime;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChildActor: public TActor<TChildActor>
{
private:
    const bool WaitForWakeup;
    TActorId Poisoner;
    ui64 PoisonCookie = 0;

public:
    explicit TChildActor(bool waitForWakeup = false)
        : TActor(&TThis::Main)
        , WaitForWakeup(waitForWakeup)
    {}

private:
    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(TEvents::TEvWakeup, HandleWakeup);
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        if (WaitForWakeup) {
            if (!Poisoner) {
                Poisoner = ev->Sender;
                PoisonCookie = ev->Cookie;
            }
            return;
        }

        ctx.Send(
            ev->Sender,
            std::make_unique<TEvents::TEvPoisonTaken>(),
            0,
            ev->Cookie);
        Die(ctx);
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        UNIT_ASSERT(Poisoner);

        ctx.Send(
            Poisoner,
            std::make_unique<TEvents::TEvPoisonTaken>(),
            0,
            PoisonCookie);
        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TParentActor
    : public TActor<TParentActor>
    , IMortalActor
{
private:
    using TBase = TActor<TParentActor>;
    TPoisonPillHelper PoisonPillHelper;
    ui32 ChildCount;
    bool WaitForChildWakeup;
    TVector<TActorId> Children;

public:
    TParentActor(ui32 childCount, bool waitForChildWakeup = false)
        : TActor(&TThis::Main)
        , PoisonPillHelper(this)
        , ChildCount(childCount)
        , WaitForChildWakeup(waitForChildWakeup)
    {}

    void Poison(const NActors::TActorContext& ctx) override
    {
        TBase::Die(ctx);
    }

    TVector<TActorId> GetChildren()
    {
        return Children;
    }

private:
    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvBootstrap, HandleBootstrap);

            HFunc(TEvents::TEvPoisonPill, PoisonPillHelper.HandlePoisonPill);
            HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);
        }
    }

    void HandleBootstrap(
        const TEvents::TEvBootstrap::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        {   // We give ownership and take it away immediately.
            auto childId = ctx.Register(new TChildActor());
            PoisonPillHelper.TakeOwnership(ctx, childId);
            ctx.Send(childId, std::make_unique<TEvents::TEvPoisonPill>());
            PoisonPillHelper.ReleaseOwnership(ctx, childId);
        }

        // Give ownership for long time.
        for (ui32 i = 0; i < ChildCount; ++i) {
            Children.push_back(
                ctx.Register(new TChildActor(WaitForChildWakeup)));
            PoisonPillHelper.TakeOwnership(ctx, Children.back());
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPoisonPillHelperTest)
{
    Y_UNIT_TEST(Basic)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TParentActor>(10));

        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>(), 0);

        testEnv.Send(actorId, std::make_unique<TEvents::TEvPoisonPill>(), 1000);

        auto poisonTakenEvent =
            testEnv.GrabPoisonTakenEvent(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, poisonTakenEvent);
        UNIT_ASSERT_VALUES_EQUAL(1000, poisonTakenEvent->Cookie);
    }

    Y_UNIT_TEST(NoChildren)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TParentActor>(0));

        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>(), 0);

        testEnv.Send(actorId, std::make_unique<TEvents::TEvPoisonPill>(), 1000);

        auto poisonTakenEvent =
            testEnv.GrabPoisonTakenEvent(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, poisonTakenEvent);
        UNIT_ASSERT_VALUES_EQUAL(1000, poisonTakenEvent->Cookie);
    }

    Y_UNIT_TEST(ShouldReplyToAllPoisoners)
    {
        TMyTestEnv testEnv;

        auto actorId =
            testEnv.Register(std::make_unique<TParentActor>(1, true));

        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>(), 0);
        testEnv.DispatchEvents(10ms);

        auto* actorHandle = dynamic_cast<TParentActor*>(
            testEnv.AccessRuntime().FindActor(actorId));
        const auto children = actorHandle->GetChildren();

        UNIT_ASSERT_VALUES_EQUAL(1, children.size());

        TVector<std::pair<TActorId, ui64>> poisoners;
        for (ui64 cookie: {1000, 2000, 3000}) {
            poisoners.emplace_back(testEnv.AllocateEdgeActor(), cookie);
            testEnv.Send(
                poisoners.back().first,
                actorId,
                std::make_unique<TEvents::TEvPoisonPill>(),
                cookie);
        }

        testEnv.DispatchEvents(10ms);
        testEnv.Send(
            children.front(),
            std::make_unique<TEvents::TEvWakeup>(),
            0);

        for (const auto& [poisoner, cookie]: poisoners) {
            auto poisonTakenEvent = testEnv.GrabPoisonTakenEvent(
                poisoner,
                TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_UNEQUAL(nullptr, poisonTakenEvent);
            UNIT_ASSERT_VALUES_EQUAL(cookie, poisonTakenEvent->Cookie);
        }
    }

    Y_UNIT_TEST(ReleaseOwnershipWhenChildIsDead)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TParentActor>(10));

        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>(), 0);

        testEnv.DispatchEvents(10ms);

        auto* actorHandle = dynamic_cast<TParentActor*>(
            testEnv.AccessRuntime().FindActor(actorId));

        auto children = actorHandle->GetChildren();

        UNIT_ASSERT_VALUES_EQUAL(10, children.size());

        for (auto child: children) {
            testEnv.Send(child, std::make_unique<TEvents::TEvPoisonPill>(), 0);

            auto poisonTakenEvent =
                testEnv.GrabPoisonTakenEvent(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_UNEQUAL(nullptr, poisonTakenEvent);
        }

        testEnv.Send(actorId, std::make_unique<TEvents::TEvPoisonPill>(), 1000);

        auto poisonTakenEvent =
            testEnv.GrabPoisonTakenEvent(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, poisonTakenEvent);
        UNIT_ASSERT_VALUES_EQUAL(1000, poisonTakenEvent->Cookie);
    }
}
}   // namespace NCloud::NBlockStore::NStorage
