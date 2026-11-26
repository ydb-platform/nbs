#include "address_resolver.h"

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/dnsresolver/dnsresolver.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <chrono>
#include <functional>
#include <future>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;

class TDnsWithoutReply final: public TActor<TDnsWithoutReply>
{
public:
    TDnsWithoutReply()
        : TActor(&TThis::Ignore)
    {}

private:
    STFUNC(Ignore)
    {
        Y_UNUSED(ev);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestActorSystem final: public IActorSystem
{
    TTestActorRuntimeBase Runtime{1, true};
    TProgramShouldContinue ProgramShouldContinue;

public:
    std::function<void()> BeforeLookupSend;

    TTestActorSystem()
    {
        Runtime.Initialize();
        Runtime.RegisterService(
            NDnsResolver::MakeDnsResolverActorId(),
            Runtime.Register(new TDnsWithoutReply()));
    }

    // IStartable implementation
    void Start() override
    {}

    void Stop() override
    {}

    // IActorSystem implementation
    TActorId Register(IActorPtr actor, TStringBuf executorName) override
    {
        Y_UNUSED(executorName);
        return Runtime.Register(actor.release());
    }

    bool Send(const TActorId& recipient, IEventBasePtr event) override
    {
        if (BeforeLookupSend && event->Type() != TEvents::TEvPoison::EventType)
        {
            BeforeLookupSend();
        }
        return Runtime.GetActorSystem(0)->Send(recipient, event.release());
    }

    bool Send(IEventHandlePtr event) override
    {
        return Runtime.GetActorSystem(0)->Send(event.release());
    }

    void Schedule(
        TDuration delta,
        IEventHandlePtr event,
        ISchedulerCookie* cookie) override
    {
        Runtime.GetActorSystem(0)->Schedule(delta, event.release(), cookie);
    }

    TProgramShouldContinue& GetProgramShouldContinue() override
    {
        return ProgramShouldContinue;
    }

    // ILoggingService implementation
    TLog CreateLog(const TString& component) override
    {
        Y_UNUSED(component);
        return {};
    }

    // IMonitoringService implementation
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIcV2ActorAddressResolverTest)
{
    Y_UNIT_TEST(StopReturnsWhileAdmittedLookupIsSending)
    {
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->BeforeLookupSend = [entered, release]() mutable
        {
            if (entered.TrySetValue()) {
                release.GetFuture().GetValueSync();
            }
        };
        auto resolver =
            CreateAddressResolver(actorSystem, CreateTaskQueueStub());
        resolver->Start();

        std::future<TResolveFuture> resolving;
        std::future<void> stopping;
        Y_DEFER
        {
            release.TrySetValue();
        };
        resolving = std::async(
            std::launch::async,
            [&]
            {
                return resolver->Resolve("peer.test", 1234, TMonotonic::Max());
            });
        const auto timeout = TDuration::Seconds(5);
        UNIT_ASSERT(entered.GetFuture().Wait(timeout));

        stopping = std::async(std::launch::async, [&] { resolver->Stop(); });
        UNIT_ASSERT(
            stopping.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready);
        stopping.get();

        auto stopped = resolver->Resolve("peer.test", 1234, TMonotonic::Max());
        UNIT_ASSERT(stopped.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            stopped.GetValue().GetError().GetCode());

        release.SetValue();
        UNIT_ASSERT(
            resolving.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready);
        const auto pending = resolving.get();
        UNIT_ASSERT(pending.Wait(timeout));
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            pending.GetValue().GetError().GetCode());
    }
}

}   // namespace NCloud::NIcV2
