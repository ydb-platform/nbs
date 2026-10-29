#include "connector.h"

#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <future>

namespace NCloud::NIcV2 {

using namespace NTests;
using namespace std::chrono_literals;

namespace {

class TBlockedCompletion
{
    const IConnectorPtr Connector;
    NThreading::TPromise<void> Subscribed = NThreading::NewPromise<void>();
    NThreading::TPromise<void> Entered = NThreading::NewPromise<void>();
    NThreading::TPromise<void> Released = NThreading::NewPromise<void>();
    std::future<void> Subscription;

public:
    TBlockedCompletion(
        IConnectorPtr connector,
        TSocketFuture future,
        bool requestStop)
        : Connector(std::move(connector))
    {
        Subscription = std::async(
            std::launch::async,
            [this, future, requestStop]
            {
                future.Subscribe(
                    [this, requestStop](const TSocketFuture&)
                    {
                        if (requestStop) {
                            Connector->RequestStop();
                        }
                        Entered.SetValue();
                        Released.GetFuture().GetValueSync();
                    });
                Subscribed.SetValue();
            });
    }

    ~TBlockedCompletion()
    {
        Release();
        Subscription.wait();
        Connector->Stop();
    }

    void WaitUntilSubscribed()
    {
        UNIT_ASSERT(Subscribed.GetFuture().Wait(TDuration::Seconds(5)));
    }

    void WaitUntilEntered()
    {
        UNIT_ASSERT(Entered.GetFuture().Wait(TDuration::Seconds(5)));
    }

    void Release()
    {
        Released.TrySetValue();
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2ConnectorTest)
{
    Y_UNIT_TEST(ConnectsConcurrentlyOverIpv4AndIpv6)
    {
        for (const char* host: {"127.0.0.1", "::1"}) {
            TTestListener listener(host);
            listener.Listener->Start();
            auto connector = CreateConnector();
            connector->Start();
            std::array<TSocketFuture, 8> futures;
            for (auto& future: futures) {
                future = connector->Connect(
                    listener.Listener->GetAddress(),
                    TDuration::Seconds(5));
            }
            for (auto& future: futures) {
                CheckSocket(WaitSocket(future));
                CheckSocket(listener.Accept());
            }
            connector->Stop();
        }
    }

    Y_UNIT_TEST(ReportsConnectionRefused)
    {
        TTestListener listener;
        const auto address = listener.Listener->GetAddress();
        listener.Listener->Stop();
        auto connector = CreateConnector();
        connector->Start();
        auto result =
            WaitResult(connector->Connect(address, TDuration::Seconds(5)));
        connector->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetError().GetCode(),
            MAKE_SYSTEM_ERROR(ECONNREFUSED));
        UNIT_ASSERT(!result.GetResult());
    }

    Y_UNIT_TEST(RejectsRequestsOutsideLifecycleAndExpiredDeadline)
    {
        auto connector = CreateConnector();
        const NInterconnect::TAddress address("127.0.0.1", 1);
        auto before =
            WaitResult(connector->Connect(address, TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(before.GetError().GetCode(), E_REJECTED);
        connector->Start();
        auto expired =
            WaitResult(connector->Connect(address, TDuration::Zero()));
        UNIT_ASSERT_VALUES_EQUAL(expired.GetError().GetCode(), E_TIMEOUT);
        connector->RequestStop();
        auto after =
            WaitResult(connector->Connect(address, TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(after.GetError().GetCode(), E_REJECTED);
        connector->Stop();
        connector->Stop();
    }

    Y_UNIT_TEST(CancelsPendingConnectWithoutWaitingForDeadline)
    {
        TTestListener listener(
            "127.0.0.1",
            1);   // intentionally does not accept
        auto occupied = OccupyBacklog(listener.Listener->GetAddress());
        auto connector = CreateConnector();
        connector->Start();
        auto pending = connector->Connect(
            listener.Listener->GetAddress(),
            TDuration::Seconds(30));
        const bool waiting = !pending.Wait(TDuration::MilliSeconds(50));
        auto stopping =
            std::async(std::launch::async, [&] { connector->Stop(); });
        UNIT_ASSERT(stopping.wait_for(5s) == std::future_status::ready);
        stopping.get();
        UNIT_ASSERT(waiting);
        auto result = WaitResult(pending);
        UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), E_REJECTED);
    }

    Y_UNIT_TEST(TimesOutPendingConnect)
    {
        TTestListener listener("127.0.0.1", 1);
        auto occupied = OccupyBacklog(listener.Listener->GetAddress());
        auto connector = CreateConnector();
        connector->Start();
        auto pending = connector->Connect(
            listener.Listener->GetAddress(),
            TDuration::MilliSeconds(100));
        auto result = WaitResult(pending);
        connector->Stop();
        UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), E_TIMEOUT);
        UNIT_ASSERT(!result.GetResult());
    }

    Y_UNIT_TEST(RequestStopFromCompletionAndConcurrentStop)
    {
        TTestListener listener("127.0.0.1", 1);
        auto occupied = OccupyBacklog(listener.Listener->GetAddress());
        auto connector = CreateConnector();
        connector->Start();
        auto pending = connector->Connect(
            listener.Listener->GetAddress(),
            TDuration::Max());
        std::future<void> first;
        std::future<void> second;
        TBlockedCompletion completion(connector, pending, true);
        completion.WaitUntilSubscribed();
        listener.Listener->Start();
        completion.WaitUntilEntered();
        first = std::async(std::launch::async, [&] { connector->Stop(); });
        second = std::async(std::launch::async, [&] { connector->Stop(); });
        const bool waits =
            first.wait_for(50ms) == std::future_status::timeout &&
            second.wait_for(50ms) == std::future_status::timeout;
        completion.Release();
        first.get();
        second.get();
        UNIT_ASSERT(waits);
        CheckSocket(WaitSocket(pending));
    }

    Y_UNIT_TEST(DeadlineIncludesQueueTime)
    {
        TTestListener listener("127.0.0.1", 1);
        auto occupied = OccupyBacklog(listener.Listener->GetAddress());
        auto connector = CreateConnector();
        connector->Start();
        auto first = connector->Connect(
            listener.Listener->GetAddress(),
            TDuration::Max());
        TBlockedCompletion completion(connector, first, false);
        completion.WaitUntilSubscribed();
        listener.Listener->Start();
        completion.WaitUntilEntered();
        auto queued = connector->Connect(
            listener.Listener->GetAddress(),
            TDuration::MilliSeconds(10));
        const bool waiting = !queued.Wait(TDuration::MilliSeconds(50));
        completion.Release();
        auto result = WaitResult(queued);
        connector->Stop();
        CheckSocket(WaitSocket(first));
        UNIT_ASSERT(waiting);
        UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), E_TIMEOUT);
    }
}

}   // namespace NCloud::NIcV2
