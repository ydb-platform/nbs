#include "actor_pool.h"

#include <cloud/storage/core/libs/actors/helpers.h>

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

class TTestActorSystem final: public IActorSystem
{
private:
    TTestActorRuntimeBase& Runtime;
    TActorId Sender;

    TProgramShouldContinue ProgramShouldContinue;

public:
    TTestActorSystem(TTestActorRuntimeBase& runtime, TActorId sender)
        : Runtime(runtime)
        , Sender(sender)
    {}

    ~TTestActorSystem() override = default;

    //
    // IStartable
    //

    void Start() override
    {}

    void Stop() override
    {}

    //
    // ILoggingService
    //

    TLog CreateLog(const TString& component) override
    {
        Y_UNUSED(component);
        return {};
    }

    //
    // IMonitoringService
    //

    NMonitoring::IMonPagePtr RegisterIndexPage(
        const TString& path,
        const TString& title) override
    {
        Y_UNUSED(path);
        Y_UNUSED(title);
        return {};
    }

    void RegisterMonPage(NMonitoring::IMonPagePtr page) override
    {
        Y_UNUSED(page);
    }

    NMonitoring::IMonPagePtr GetMonPage(const TString& path) override
    {
        Y_UNUSED(path);
        return {};
    }

    NMonitoring::TDynamicCountersPtr GetCounters() override
    {
        return {};
    }

    //
    // IActorSystem
    //

    NActors::TActorId Register(
        NActors::IActorPtr actor,
        TStringBuf executorName = {}) override
    {
        Y_UNUSED(executorName);

        auto actorId = Runtime.Register(actor.release());
        Runtime.EnableScheduleForActor(actorId);

        return actorId;
    }

    bool Send(
        const NActors::TActorId& recipient,
        NActors::IEventBasePtr event) override
    {
        Runtime.Send(new IEventHandle(recipient, Sender, event.release()));
        return true;
    }

    bool Send(TAutoPtr<NActors::IEventHandle> ev) override
    {
        Runtime.Send(ev);
        return true;
    }

    void Schedule(
        TDuration delta,
        std::unique_ptr<NActors::IEventHandle> ev,
        NActors::ISchedulerCookie* cookie) override
    {
        Y_UNUSED(cookie);
        Runtime.Schedule(ev.release(), delta);
    }

    NActors::NLog::TSettings* LoggerSettings() const override
    {
        return Runtime.GetLogSettings(0).Get();
    }

    TProgramShouldContinue& GetProgramShouldContinue() override
    {
        return ProgramShouldContinue;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestEnv final
{
public:
    TActorId Sender;

private:
    TTestActorRuntime Runtime;
    TIntrusivePtr<TTestActorSystem> ActorSystem;

public:
    TTestEnv()
    {
        NKikimr::SetupTabletServices(Runtime);
        Sender = Runtime.AllocateEdgeActor();
        ActorSystem = MakeIntrusive<TTestActorSystem>(Runtime, Sender);
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = Runtime.Register(actor.release());
        Runtime.EnableScheduleForActor(actorId);

        return actorId;
    }

    void Send(const TActorId& recipient, IEventBasePtr event, ui64 cookie)
    {
        Runtime.Send(
            new IEventHandle(recipient, Sender, event.release(), 0, cookie));
    }

    void DispatchEvents(TDuration timeout)
    {
        Runtime.DispatchEvents(TDispatchOptions(), timeout);
    }

    TAutoPtr<IEventHandle> GrabPingEvent(TDuration timeout)
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TEvents::TEvPing>(handle, timeout);
        return handle;
    }

    TTestActorRuntime& AccessRuntime()
    {
        return Runtime;
    }

    TIntrusivePtr<TTestActorSystem> GetActorSystem()
    {
        return ActorSystem;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestActor: public IPooledActor<TTestActor>
{
private:
    std::function<void()> OnDestroyCallback;
    std::function<void()> OnResetCallback;

public:
    TTestActor()
        : IPooledActor(&TThis::Main)
    {}

    ~TTestActor() override
    {
        if (OnDestroyCallback) {
            OnDestroyCallback();
        }
    }

    void SetOnDestroyCallback(std::function<void()> callback)
    {
        OnDestroyCallback = std::move(callback);
    }

    void SetOnResetCallback(std::function<void()> callback)
    {
        OnResetCallback = std::move(callback);
    }

    void SendPing(TActorId recipient)
    {
        GetActorSystem()->Send(
            new IEventHandle(recipient, GetSelfId(), new TEvents::TEvPing()));
    }

private:
    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(TEvents::TEvPong, HandlePong);
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        NCloud::Reply(ctx, *ev, std::make_unique<TEvents::TEvPoisonTaken>());
        Die(ctx);
    }

    void HandlePong(const TEvents::TEvPong::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        WorkFinished(ctx);
    }

    void Reset() override
    {
        if (OnResetCallback) {
            OnResetCallback();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TActorPoolTest)
{
    Y_UNIT_TEST(Basic)
    {
        TTestEnv testEnv;
        auto actorPool =
            MakeIntrusive<TActorPool<TTestActor>>(testEnv.GetActorSystem(), 2u);

        std::array<ui32, 3> deadCounters{};
        std::array<ui32, 3> resetCounters{};

        auto* actor1 = actorPool->GetPooledActor();
        actor1->SetOnDestroyCallback([&deadCounters]() { deadCounters[0]++; });
        actor1->SetOnResetCallback([&resetCounters]() { resetCounters[0]++; });
        auto* actor2 = actorPool->GetPooledActor();
        actor2->SetOnDestroyCallback([&deadCounters]() { deadCounters[1]++; });
        actor2->SetOnResetCallback([&resetCounters]() { resetCounters[1]++; });

        // This actor should be destroyed after its work is finished.
        auto* actor3 = actorPool->GetPooledActor();
        actor3->SetOnDestroyCallback([&deadCounters]() { deadCounters[2]++; });
        actor3->SetOnResetCallback([&resetCounters]() { resetCounters[2]++; });

        actor1->SendPing(testEnv.Sender);
        actor2->SendPing(testEnv.Sender);
        actor3->SendPing(testEnv.Sender);

        for (int i = 0; i < 3; ++i) {
            auto pingEvent = testEnv.GrabPingEvent(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_UNEQUAL(nullptr, pingEvent);
            testEnv.Send(
                pingEvent->Sender,
                std::make_unique<TEvents::TEvPong>(),
                0);
        }

        testEnv.DispatchEvents(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(0, deadCounters[0]);
        UNIT_ASSERT_VALUES_EQUAL(0, deadCounters[1]);
        UNIT_ASSERT_VALUES_EQUAL(1, deadCounters[2]);

        UNIT_ASSERT_VALUES_EQUAL(1, resetCounters[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, resetCounters[1]);
        UNIT_ASSERT_VALUES_EQUAL(0, resetCounters[2]);

        // 1 by test itself + 2 by the actors in the pool.
        UNIT_ASSERT_VALUES_EQUAL(3, actorPool->RefCount());

        actor1->SetOnDestroyCallback({});
        actor2->SetOnDestroyCallback({});
    }

    Y_UNIT_TEST(ZeroMaxActors)
    {
        TTestEnv testEnv;
        auto actorPool =
            MakeIntrusive<TActorPool<TTestActor>>(testEnv.GetActorSystem(), 0u);

        ui32 deadCounter = 0;
        // This actor should be destroyed after its work is finished.
        auto* actor = actorPool->GetPooledActor();
        actor->SetOnDestroyCallback([&deadCounter]() { deadCounter++; });

        actor->SendPing(testEnv.Sender);
        auto pingEvent = testEnv.GrabPingEvent(TDuration::Seconds(1));
        testEnv.Send(
            pingEvent->Sender,
            std::make_unique<TEvents::TEvPong>(),
            0);

        testEnv.DispatchEvents(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, deadCounter);
        UNIT_ASSERT_VALUES_EQUAL(1, actorPool->RefCount());
    }

    Y_UNIT_TEST(OnBeforeDestroy)
    {
        TTestEnv testEnv;
        auto actorPool =
            MakeIntrusive<TActorPool<TTestActor>>(testEnv.GetActorSystem(), 2u);

        std::array<ui32, 3> deadCounters{};
        std::array<ui32, 3> resetCounters{};

        auto* actor1 = actorPool->GetPooledActor();
        actor1->SetOnDestroyCallback([&deadCounters]() { deadCounters[0]++; });
        actor1->SetOnResetCallback([&resetCounters]() { resetCounters[0]++; });
        auto* actor2 = actorPool->GetPooledActor();
        actor2->SetOnDestroyCallback([&deadCounters]() { deadCounters[1]++; });
        actor2->SetOnResetCallback([&resetCounters]() { resetCounters[1]++; });
        auto* actor3 = actorPool->GetPooledActor();
        actor3->SetOnDestroyCallback([&deadCounters]() { deadCounters[2]++; });
        actor3->SetOnResetCallback([&resetCounters]() { resetCounters[2]++; });

        actorPool->OnBeforeDestroy();

        testEnv.DispatchEvents(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, deadCounters[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, deadCounters[1]);
        UNIT_ASSERT_VALUES_EQUAL(0, deadCounters[2]);

        UNIT_ASSERT_VALUES_EQUAL(0, resetCounters[0]);
        UNIT_ASSERT_VALUES_EQUAL(0, resetCounters[1]);
        UNIT_ASSERT_VALUES_EQUAL(0, resetCounters[2]);
        UNIT_ASSERT_VALUES_EQUAL(1, actorPool->RefCount());

        actor3->SetOnDestroyCallback({});
    }
}
}   // namespace NCloud::NBlockStore::NStorage
