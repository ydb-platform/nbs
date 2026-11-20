#include "address_resolver.h"

#include <cloud/storage/core/libs/common/task_queue.h>

#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/dnsresolver/dnsresolver.h>
#include <contrib/ydb/library/actors/interconnect/events_local.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <array>
#include <thread>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;
using NDnsResolver::TEvDns;

class TManualExecutor final: public ITaskQueue
{
    TMutex Mutex;
    TVector<ITaskPtr> Tasks;

public:
    void Start() override
    {}

    void Stop() override
    {}

    void Enqueue(ITaskPtr task) override
    {
        with_lock (Mutex) {
            Tasks.push_back(std::move(task));
        }
    }

    size_t TaskCount()
    {
        with_lock (Mutex) {
            return Tasks.size();
        }
    }

    void Run()
    {
        TVector<ITaskPtr> tasks;
        with_lock (Mutex) {
            tasks.swap(Tasks);
        }
        for (const auto& task: tasks) {
            task->Execute();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDnsRequest
{
    TActorId Sender;
    TString Host;
    int Family = 0;
};

class TMockDnsResolver final: public TActor<TMockDnsResolver>
{
    TVector<TDnsRequest>& Requests;

public:
    explicit TMockDnsResolver(TVector<TDnsRequest>& requests)
        : TActor(&TThis::StateWork)
        , Requests(requests)
    {}

private:
    STRICT_STFUNC(StateWork, {
        hFunc(TEvDns::TEvGetHostByName, HandleDnsRequest);
    });

    void HandleDnsRequest(TEvDns::TEvGetHostByName::TPtr& ev)
    {
        Requests.push_back(
            TDnsRequest{
                .Sender = ev->Sender,
                .Host = ev->Get()->Name,
                .Family = ev->Get()->Family,
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    TTestActorRuntimeBase Runtime;
    TVector<TDnsRequest> Requests;
    std::shared_ptr<TManualExecutor> Executor =
        std::make_shared<TManualExecutor>();
    IAddressResolverPtr Resolver;

    explicit TTestEnv(bool registerDns = true)
    {
        Runtime.SetScheduledEventFilter(
            [](auto&& runtime, auto&& event, auto&& delay, auto&& deadline)
            {
                Y_UNUSED(runtime);
                Y_UNUSED(event);
                Y_UNUSED(delay);
                Y_UNUSED(deadline);
                return false;
            });
        Runtime.Initialize();
        if (registerDns) {
            Runtime.RegisterService(
                NDnsResolver::MakeDnsResolverActorId(),
                Runtime.Register(new TMockDnsResolver(Requests)));
        }
        Resolver = CreateAddressResolver(Runtime.GetActorSystem(0), Executor);
    }

    template <typename F>
    void DispatchUntil(F condition)
    {
        if (condition()) {
            return;
        }
        TDispatchOptions options;
        options.CustomFinalCondition = condition;
        Runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_C(
            condition(),
            "Actor dispatch did not reach the expected state");
    }

    void Complete(const TResolveFuture& future)
    {
        while (!future.IsReady()) {
            Executor->Run();
            if (future.IsReady()) {
                return;
            }
            DispatchUntil([&] { return Executor->TaskCount() != 0; });
        }
    }

    void Reply(
        size_t index,
        const TVector<TString>& ipv4 = {"127.0.0.1"},
        const TVector<TString>& ipv6 = {},
        const TString& error = {})
    {
        auto response = std::make_unique<TEvDns::TEvGetHostByNameResult>();
        if (!error.empty()) {
            response->Status = 1;
            response->ErrorText = error;
        }
        for (const auto& ip: ipv4) {
            response->AddrsV4.push_back(
                NInterconnect::TAddress(ip, 0).Addr.Ipv4.sin_addr);
        }
        for (const auto& ip: ipv6) {
            response->AddrsV6.push_back(
                NInterconnect::TAddress(ip, 0).Addr.Ipv6.sin6_addr);
        }
        Runtime.Send(new IEventHandle(
            Requests.at(index).Sender,
            {},
            response.release()));
    }
};

void AssertSuccess(const TResolveFuture& future)
{
    UNIT_ASSERT(!future.HasException());
    UNIT_ASSERT_C(
        !HasError(future.GetValue().GetError()),
        FormatError(future.GetValue().GetError()));
}

}   // namespace

Y_UNIT_TEST_SUITE(TAddressResolverTest)
{
    Y_UNIT_TEST(OwnsArgumentsAndDeliversThroughExecutor)
    {
        TTestEnv env;
        TString host = "original.test";
        auto future = env.Resolver->Resolve(host, 1234, TMonotonic::Max());
        host = "different.test";
        env.DispatchUntil([&] { return env.Requests.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL("original.test", env.Requests[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(AF_UNSPEC, env.Requests[0].Family);
        env.Reply(0);
        env.DispatchUntil([&] { return env.Executor->TaskCount() == 1; });
        UNIT_ASSERT(!future.IsReady());
        env.Resolver.reset();
        env.Complete(future);
        AssertSuccess(future);
        const auto& addresses = future.GetValue().GetResult();
        UNIT_ASSERT_VALUES_EQUAL(1, addresses.size());
        UNIT_ASSERT_VALUES_EQUAL("127.0.0.1", addresses.front().GetAddress());
        UNIT_ASSERT_VALUES_EQUAL(1234, addresses.front().GetPort());
    }

    Y_UNIT_TEST(ReturnsEveryIpv4AndIpv6Address)
    {
        TTestEnv env;
        auto future =
            env.Resolver->Resolve("many.test", 1234, TMonotonic::Max());
        env.DispatchUntil([&] { return env.Requests.size() == 1; });
        env.Reply(0, {"127.0.0.1", "127.0.0.2"}, {"::1", "2001:db8::1"});
        env.Complete(future);
        AssertSuccess(future);
        const auto& addresses = future.GetValue().GetResult();
        UNIT_ASSERT_VALUES_EQUAL(4, addresses.size());
        UNIT_ASSERT_VALUES_EQUAL("::1", addresses[0].GetAddress());
        UNIT_ASSERT_VALUES_EQUAL("2001:db8::1", addresses[1].GetAddress());
        UNIT_ASSERT_VALUES_EQUAL("127.0.0.1", addresses[2].GetAddress());
        UNIT_ASSERT_VALUES_EQUAL("127.0.0.2", addresses[3].GetAddress());
        for (const auto& address: addresses) {
            UNIT_ASSERT_VALUES_EQUAL(1234, address.GetPort());
        }
    }

    Y_UNIT_TEST(ReturnsErrorsAndEmptyResults)
    {
        TTestEnv env;
        auto failed =
            env.Resolver->Resolve("failed.test", 1234, TMonotonic::Max());
        auto empty =
            env.Resolver->Resolve("empty.test", 1234, TMonotonic::Max());
        env.DispatchUntil([&] { return env.Requests.size() == 2; });
        for (size_t i = 0; i < env.Requests.size(); ++i) {
            if (env.Requests[i].Host == "failed.test") {
                env.Reply(i, {}, {}, "test DNS failure");
            } else {
                env.Reply(i, {});
            }
        }
        env.Complete(failed);
        env.Complete(empty);
        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAVAILABLE,
            failed.GetValue().GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            failed.GetValue().GetError().GetMessage(),
            "test DNS failure");
        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAVAILABLE,
            empty.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ReportsMissingDnsService)
    {
        TTestEnv env(false);
        auto future =
            env.Resolver->Resolve("missing.test", 1234, TMonotonic::Max());
        env.Complete(future);
        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAVAILABLE,
            future.GetValue().GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            future.GetValue().GetError().GetMessage(),
            "unavailable");
    }

    Y_UNIT_TEST(ReportsInterconnectDeadline)
    {
        TTestEnv env;
        const auto deadline =
            env.Runtime.GetCurrentMonotonicTime() + TDuration::MilliSeconds(50);
        auto future = env.Resolver->Resolve("timeout.test", 1234, deadline);
        auto other =
            env.Resolver->Resolve("other.test", 1234, TMonotonic::Max());
        env.DispatchUntil([&] { return env.Requests.size() == 2; });
        env.Complete(future);
        UNIT_ASSERT_VALUES_EQUAL(
            E_TIMEOUT,
            future.GetValue().GetError().GetCode());
        UNIT_ASSERT(!other.IsReady());
        for (size_t i = 0; i < env.Requests.size(); ++i) {
            if (env.Requests[i].Host == "other.test") {
                env.Reply(i);
            }
        }
        env.Complete(other);
        AssertSuccess(other);
    }

    Y_UNIT_TEST(SlowLookupDoesNotBlockOtherLookups)
    {
        TTestEnv env;
        auto slow = env.Resolver->Resolve("slow.test", 1234, TMonotonic::Max());
        auto fast = env.Resolver->Resolve("fast.test", 1234, TMonotonic::Max());
        env.DispatchUntil([&] { return env.Requests.size() == 2; });
        for (size_t i = 0; i < env.Requests.size(); ++i) {
            if (env.Requests[i].Host == "fast.test") {
                env.Reply(i);
            }
        }
        env.Complete(fast);
        AssertSuccess(fast);
        UNIT_ASSERT(!slow.IsReady());
        env.Resolver.reset();
        env.Complete(slow);
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            slow.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(CancelsRequestQueuedBeforeStop)
    {
        TTestEnv env;
        auto future =
            env.Resolver->Resolve("cancelled.test", 1234, TMonotonic::Max());
        env.Resolver.reset();
        env.Complete(future);
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            future.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(CancelsPendingFutureAndIgnoresLateDnsReply)
    {
        TTestEnv env;
        auto future =
            env.Resolver->Resolve("cancelled.test", 1234, TMonotonic::Max());
        env.DispatchUntil([&] { return env.Requests.size() == 1; });
        env.Resolver.reset();
        env.Complete(future);
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            future.GetValue().GetError().GetCode());
        env.Reply(0);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvLocalNodeInfo::EventType, 1);
        env.Runtime.DispatchEvents(options);
        env.Executor->Run();
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            future.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ResolvesManyOutstandingRequests)
    {
        constexpr size_t requestCount = 2000;
        TTestEnv env;
        size_t registered = 0;
        env.Runtime.SetRegistrationObserverFunc(
            [&](TTestActorRuntimeBase& runtime,
                const TActorId& parent,
                const TActorId& actor)
            {
                Y_UNUSED(runtime);
                Y_UNUSED(parent);
                Y_UNUSED(actor);
                ++registered;
            });
        TVector<TResolveFuture> futures;
        for (size_t i = 0; i < requestCount; ++i) {
            futures.push_back(env.Resolver->Resolve(
                TStringBuilder() << "pending-" << i << ".test",
                1234,
                TMonotonic::Max()));
        }
        env.DispatchUntil([&] { return env.Requests.size() == requestCount; });
        UNIT_ASSERT_VALUES_EQUAL(requestCount, registered);
        for (const auto& future: futures) {
            UNIT_ASSERT(!future.IsReady());
        }
        for (size_t i = 0; i < env.Requests.size(); ++i) {
            env.Reply(i);
        }
        for (const auto& future: futures) {
            env.Complete(future);
            AssertSuccess(future);
        }
    }

    Y_UNIT_TEST(MatchesOutOfOrderRepliesForTheSameHost)
    {
        TTestEnv env;
        auto first =
            env.Resolver->Resolve("same.test", 1234, TMonotonic::Max());
        auto second =
            env.Resolver->Resolve("same.test", 5678, TMonotonic::Max());
        env.DispatchUntil([&] { return env.Requests.size() == 2; });
        env.Reply(1, {"127.0.0.2"});
        env.Complete(second);
        AssertSuccess(second);
        UNIT_ASSERT(!first.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            5678,
            second.GetValue().GetResult().front().GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            "127.0.0.2",
            second.GetValue().GetResult().front().GetAddress());
        env.Reply(0);
        env.Complete(first);
        AssertSuccess(first);
        UNIT_ASSERT_VALUES_EQUAL(
            1234,
            first.GetValue().GetResult().front().GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            "127.0.0.1",
            first.GetValue().GetResult().front().GetAddress());
    }

    Y_UNIT_TEST(CanStartAnotherLookupFromContinuation)
    {
        TTestEnv env;
        auto first =
            env.Resolver->Resolve("first.test", 1234, TMonotonic::Max());
        env.DispatchUntil([&] { return env.Requests.size() == 1; });
        env.Reply(0);
        env.DispatchUntil([&] { return env.Executor->TaskCount() == 1; });
        TResolveFuture second;
        first.Subscribe(
            [&](const TResolveFuture& result)
            {
                AssertSuccess(result);
                second = env.Resolver->Resolve(
                    "second.test",
                    1234,
                    TMonotonic::Max());
            });
        env.Complete(first);
        env.DispatchUntil([&] { return env.Requests.size() == 2; });
        env.Reply(1);
        env.Complete(second);
        AssertSuccess(second);
    }

    Y_UNIT_TEST(SupportsConcurrentCallers)
    {
        TTestEnv env;
        TVector<TResolveFuture> futures(1000);
        std::array<std::thread, 4> threads;
        for (size_t n = 0; n < threads.size(); ++n) {
            threads[n] = std::thread(
                [&, n]
                {
                    for (size_t i = n; i < futures.size();
                         i += threads.size()) {
                        futures[i] = env.Resolver->Resolve(
                            "parallel.test",
                            1234,
                            TMonotonic::Max());
                    }
                });
        }
        for (auto& thread: threads) {
            thread.join();
        }
        env.DispatchUntil([&] { return env.Requests.size() == 1000; });
        env.Resolver.reset();
        for (const auto& future: futures) {
            env.Complete(future);
            UNIT_ASSERT_VALUES_EQUAL(
                E_CANCELLED,
                future.GetValue().GetError().GetCode());
        }
    }

    Y_UNIT_TEST(CanDestroyResolverFromContinuation)
    {
        TTestEnv env;
        auto future =
            env.Resolver->Resolve("done.test", 1234, TMonotonic::Max());
        future.Subscribe(
            [&](const TResolveFuture& result)
            {
                AssertSuccess(result);
                env.Resolver.reset();
            });
        env.DispatchUntil([&] { return env.Requests.size() == 1; });
        env.Reply(0);
        env.Complete(future);
        UNIT_ASSERT(!env.Resolver);
    }

    Y_UNIT_TEST(DoesNotRestartDeadlineOnDelivery)
    {
        TTestEnv env;
        const auto deadline =
            env.Runtime.GetCurrentMonotonicTime() + TDuration::MilliSeconds(50);
        auto future = env.Resolver->Resolve("expired.test", 1234, deadline);
        env.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
        env.Complete(future);
        UNIT_ASSERT_VALUES_EQUAL(
            E_TIMEOUT,
            future.GetValue().GetError().GetCode());
        UNIT_ASSERT(env.Requests.empty());
    }
}

}   // namespace NCloud::NIcV2
