#include "transport.h"

#include <contrib/ydb/library/actors/core/event_pb.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_direct_session.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>
#include <contrib/ydb/library/actors/protos/unittests.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/network/pair.h>
#include <util/network/socket.h>

#include <poll.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;
using namespace std::chrono_literals;

const TActorId ActorId(1, 0, 1, 1);

class TSignal
{
    std::mutex Mutex;
    std::condition_variable Changed;
    bool Signaled = false;

public:
    void Set()
    {
        std::lock_guard guard(Mutex);
        Signaled = true;
        Changed.notify_all();
    }

    void Wait()
    {
        std::unique_lock lock(Mutex);
        UNIT_ASSERT(Changed.wait_for(lock, 10s, [&] { return Signaled; }));
    }
};

struct THandler final: IConnectionHandler
{
    std::atomic<size_t> Events = 0;
    std::atomic<size_t> Disconnects = 0;
    std::function<void(IEventHandle&)> OnEvent;
    std::function<void()> OnDisconnect;

    void HandleEvent(std::unique_ptr<IEventHandle> ev) override
    {
        ++Events;
        if (OnEvent) {
            OnEvent(*ev);
        }
    }

    void HandleDisconnect(const TString& /*reason*/) override
    {
        ++Disconnects;
        if (OnDisconnect) {
            OnDisconnect();
        }
    }
};

struct TTestEngine final: IUringEngine
{
    struct TRegistration
    {
        TActorId SessionId;
        bool Active;
    };

    IUringEngineClientPtr Client;
    std::vector<TRegistration> Registrations;
    std::function<void(TDisconnectReason)> Disconnect;
    std::function<void()> OnRegister;
    std::function<void()> OnSend;
    std::function<void()> OnStop;
    std::atomic<size_t> Sends = 0;
    std::atomic<size_t> Unregisters = 0;
    std::atomic<size_t> Stops = 0;
    std::atomic<bool> Unregistered = false;
    ui64 RegisterResult = 1;
    std::thread Worker;
    bool HoldEvents = false;
    bool TrackQueue = false;
    ui64 OutputQueueBytes = 17;
    std::vector<std::unique_ptr<IEventHandle>> PendingEvents;

    void SetClient(IUringEngineClientPtr client) override
    {
        Client = std::move(client);
        Client->Start(TUringEnginePtr(this));
    }

    ui64 Register(
        TIntrusivePtr<NInterconnect::TStreamSocket>,
        const TActorId& sessionId,
        TScopeId,
        std::function<void(TDisconnectReason)> disconnect,
        bool,
        std::shared_ptr<std::atomic<int64_t>>,
        std::shared_ptr<std::atomic<uint64_t>>,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket) override
    {
        UNIT_ASSERT(!xdcSocket);
        Registrations.push_back({sessionId, RegisterResult != 0});
        Disconnect = std::move(disconnect);
        if (OnRegister) {
            OnRegister();
        }
        return RegisterResult ? Registrations.size() : 0;
    }

    void Send(
        ui64 handle,
        std::unique_ptr<IEventHandle> ev,
        TIntrusivePtr<IReceiveCallback>) override
    {
        UNIT_ASSERT(Registrations.at(handle - 1).Active);
        ++Sends;
        if (TrackQueue) {
            OutputQueueBytes += ev->GetSize();
        }
        if (HoldEvents) {
            PendingEvents.push_back(std::move(ev));
        }
        if (OnSend) {
            OnSend();
        }
    }

    void Unregister(ui64 handle) override
    {
        UNIT_ASSERT(std::exchange(Registrations.at(handle - 1).Active, false));
        Unregistered = true;
        ++Unregisters;
    }

    void RegisterReceiveCallback(
        ui64,
        TActorId,
        TIntrusivePtr<IReceiveCallback>) override
    {
        UNIT_FAIL(
            "NBS client must route events without a separate callback "
            "registration");
    }

    void Stop() override
    {
        if (Worker.joinable()) {
            Worker.join();
        }
        ++Stops;
        if (OnStop) {
            OnStop();
        }
        PendingEvents.clear();
        Client->Stop();
    }

    void IssueMonRequest(ui64, NMon::TEvHttpInfoRes::TPtr) override
    {}

    ui64 GetTotalOutputQueueSize(ui64 handle) override
    {
        UNIT_ASSERT(Registrations.at(handle - 1).Active);
        return OutputQueueBytes;
    }

    void Deliver(size_t registration = 0)
    {
        Client->Send(
            std::make_unique<IEventHandle>(
                Registrations.at(registration).SessionId,
                0,
                0,
                ActorId,
                ActorId,
                TIntrusivePtr<TEventSerializedData>{},
                0,
                TScopeId(),
                NWilson::TTraceId()));
    }
};

using TSockets = std::array<TIntrusivePtr<NInterconnect::TStreamSocket>, 2>;

TSockets MakeSockets()
{
    SOCKET fds[2];
    SocketPair(fds);
    SetNonBlock(fds[0]);
    SetNonBlock(fds[1]);
    return {
        MakeIntrusive<NInterconnect::TStreamSocket>(fds[0]),
        MakeIntrusive<NInterconnect::TStreamSocket>(fds[1])};
}

std::unique_ptr<IEventHandle> MakeEvent()
{
    return std::make_unique<IEventHandle>(0, 0, ActorId, ActorId, nullptr, 0);
}

struct TFixture
{
    TIntrusivePtr<TTestEngine> Engine = MakeIntrusive<TTestEngine>();
    ITransportPtr Transport = CreateTransport(TUringEnginePtr(Engine));
    std::shared_ptr<THandler> Handler = std::make_shared<THandler>();
    TSockets Sockets = MakeSockets();
    IConnectionPtr Connection = Transport->CreateConnection(
        Sockets[0],
        {},
        Handler,
        false,
        64 * 1024 * 1024);

    ~TFixture()
    {
        Transport->Stop();
    }
};

struct TEvTest final
    : TEventPB<
          TEvTest,
          TMessageWithPayload,
          EventSpaceBegin(TEvents::ES_PRIVATE)>
{
    std::shared_ptr<std::atomic<size_t>> Destroyed;

    ~TEvTest() override
    {
        if (Destroyed) {
            ++*Destroyed;
        }
    }
};

ITransportPtr MakeTransport()
{
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    common->Settings.V2.Threads = 1;
    common->Settings.V2.EnableSQPOLL = false;
    return CreateTransport(CreateUringEngine(std::move(common)));
}

void CheckCloseDuringWrite(bool xdc)
{
    if (!TUringContext::IsAvailable()) {
        Cerr << "io_uring not available; skipping" << Endl;
        return;
    }
    auto transport = MakeTransport();
    auto sockets = MakeSockets();
    auto handler = std::make_shared<THandler>();
    TSockets xdcSockets;
    if (xdc) {
        xdcSockets = MakeSockets();
    }
    auto connection = transport->CreateConnection(
        sockets[0],
        xdcSockets[0],
        handler,
        false,
        64 * 1024 * 1024);
    UNIT_ASSERT(connection->Activate());
    auto destroyed = std::make_shared<std::atomic<size_t>>(0);
    constexpr size_t count = 8;
    for (size_t i = 0; i < count; ++i) {
        auto ev = std::make_unique<TEvTest>();
        ev->Destroyed = destroyed;
        ev->AddPayload(TRope(TString(1024 * 1024, 'p')));
        UNIT_ASSERT(connection->Send(
            std::make_unique<IEventHandle>(ActorId, ActorId, ev.release())));
    }
    // The unread peer creates backpressure. Readability proves that network
    // writes have started before Close, rather than only testing queued sends.
    pollfd peer{static_cast<int>(*sockets[1]), POLLIN, 0};
    UNIT_ASSERT_VALUES_EQUAL(poll(&peer, 1, 10000), 1);
    UNIT_ASSERT(connection->GetOutputQueueSize() > 0);
    connection->Close();
    UNIT_ASSERT(connection->WhenClosed().HasValue());
    transport->Stop();
    UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), count);
    UNIT_ASSERT_VALUES_EQUAL(handler->Disconnects.load(), 0u);
}

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2TransportTest)
{
    Y_UNIT_TEST(OutputQueueLimitSerializesConcurrentAdmissionAndRecovers)
    {
        TFixture f;
        auto makeEvent = []
        {
            auto event = std::make_unique<TEvTest>();
            event->Record.SetMeta(TString(1024, 'x'));
            return std::make_unique<IEventHandle>(
                ActorId,
                ActorId,
                event.release());
        };
        const ui64 size = makeEvent()->GetSize();
        const ui64 limit = size * 4;
        f.Connection =
            f.Transport
                ->CreateConnection(f.Sockets[0], {}, f.Handler, false, limit);
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->TrackQueue = true;
        f.Engine->OutputQueueBytes = 0;
        std::atomic<size_t> admitted = 0;
        std::array<std::future<void>, 8> senders;
        for (auto& sender: senders) {
            sender = std::async(
                std::launch::async,
                [&]
                {
                    for (size_t i = 0; i < 16; ++i) {
                        admitted += f.Connection->Send(makeEvent());
                    }
                });
        }
        for (auto& sender: senders) {
            sender.get();
        }
        UNIT_ASSERT_VALUES_EQUAL(admitted.load(), 4);
        UNIT_ASSERT_VALUES_EQUAL(f.Connection->GetOutputQueueSize(), limit);
        f.Engine->OutputQueueBytes = size * 3;
        UNIT_ASSERT(f.Connection->Send(makeEvent()));
        UNIT_ASSERT(!f.Connection->Send(makeEvent()));
        f.Engine->OutputQueueBytes = 0;
        auto oversized = std::make_unique<TEvTest>();
        oversized->Record.SetMeta(TString(limit, 'x'));
        UNIT_ASSERT(!f.Connection->Send(
            std::make_unique<IEventHandle>(
                ActorId,
                ActorId,
                oversized.release())));
        UNIT_ASSERT(f.Connection->Send(makeEvent()));
    }

    Y_UNIT_TEST(AssignsIndependentConnectionIdsAndDoesNotReuseThem)
    {
        TFixture f;
        auto secondSockets = MakeSockets();
        auto secondHandler = std::make_shared<THandler>();
        auto second = f.Transport->CreateConnection(
            secondSockets[0],
            {},
            secondHandler,
            false,
            64 * 1024 * 1024);
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT(second->Activate());
        UNIT_ASSERT(f.Engine->Registrations[0].SessionId);
        UNIT_ASSERT(
            f.Engine->Registrations[0].SessionId !=
            f.Engine->Registrations[1].SessionId);
        f.Engine->Deliver(0);
        f.Engine->Deliver(1);
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(secondHandler->Events.load(), 1u);

        f.Connection->Close();
        f.Connection.reset();
        auto thirdSockets = MakeSockets();
        auto thirdHandler = std::make_shared<THandler>();
        auto third = f.Transport->CreateConnection(
            thirdSockets[0],
            {},
            thirdHandler,
            false,
            64 * 1024 * 1024);
        UNIT_ASSERT(third->Activate());
        UNIT_ASSERT(
            f.Engine->Registrations[2].SessionId !=
            f.Engine->Registrations[0].SessionId);
        UNIT_ASSERT(
            f.Engine->Registrations[2].SessionId !=
            f.Engine->Registrations[1].SessionId);
        f.Engine->Deliver(
            0);   // A late notification must not reach the new owner.
        UNIT_ASSERT_VALUES_EQUAL(thirdHandler->Events.load(), 0u);
        f.Engine->Deliver(2);
        UNIT_ASSERT_VALUES_EQUAL(thirdHandler->Events.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(secondHandler->Events.load(), 1u);
    }

    Y_UNIT_TEST(QueuesEarlyEventsAndDisconnectUntilRegistrationCompletes)
    {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Sends.load(), 0u);
        UNIT_ASSERT(!f.Connection->Send(MakeEvent()));
        f.Engine->OnRegister = [&]
        {
            f.Engine->Deliver();
            f.Engine->Disconnect(TDisconnectReason::EndOfStream());
            UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 0u);
        };
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 1u);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        f.Engine->Deliver();
        f.Engine->Disconnect(TDisconnectReason::EndOfStream());
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
    }

    Y_UNIT_TEST(CloseBeforeActivation)
    {
        TFixture f;
        f.Connection->Close();
        f.Connection->Close();
        UNIT_ASSERT(!f.Connection->Activate());
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 0u);
    }

    Y_UNIT_TEST(RegistrationFailureClosesConnection)
    {
        TFixture f;
        f.Engine->RegisterResult = 0;
        UNIT_ASSERT(!f.Connection->Activate());
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 0u);
    }

    Y_UNIT_TEST(CloseDuringSendWaitsForAdmission)
    {
        TFixture f;
        TSignal entered;
        TSignal release;
        f.Engine->OnSend = [&]
        {
            entered.Set();
            release.Wait();
        };
        UNIT_ASSERT(f.Connection->Activate());
        auto sending = std::async(
            std::launch::async,
            [&] { return f.Connection->Send(MakeEvent()); });
        entered.Wait();
        auto closing =
            std::async(std::launch::async, [&] { f.Connection->Close(); });
        const bool stillRegistered = !f.Engine->Unregistered.load();
        release.Set();
        UNIT_ASSERT(sending.get());
        closing.get();
        UNIT_ASSERT(stillRegistered);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Sends.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
    }

    Y_UNIT_TEST(ConcurrentSendAndClose)
    {
        TFixture f;
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT_VALUES_EQUAL(f.Connection->GetOutputQueueSize(), 17);
        TSignal start;
        std::array<std::thread, 4> senders;
        std::atomic<size_t> accepted = 0;
        for (auto& sender: senders) {
            sender = std::thread(
                [&]
                {
                    start.Wait();
                    for (size_t i = 0; i < 1000; ++i) {
                        accepted += f.Connection->Send(MakeEvent());
                    }
                });
        }
        std::thread closer(
            [&]
            {
                start.Wait();
                f.Connection->Close();
            });
        start.Set();
        for (auto& sender: senders) {
            sender.join();
        }
        closer.join();
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Sends.load(), accepted.load());
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
        UNIT_ASSERT(!f.Connection->Send(MakeEvent()));
        UNIT_ASSERT_VALUES_EQUAL(f.Connection->GetOutputQueueSize(), 0u);
    }

    Y_UNIT_TEST(CloseWaitsForActiveCallback)
    {
        TFixture f;
        TSignal entered;
        TSignal release;
        f.Handler->OnEvent = [&](IEventHandle&)
        {
            entered.Set();
            release.Wait();
        };
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->Worker = std::thread([&] { f.Engine->Deliver(); });
        entered.Wait();
        f.Connection->Close();
        const bool pending = !f.Connection->WhenClosed().HasValue();
        f.Engine->Deliver();
        release.Set();
        f.Engine->Worker.join();
        UNIT_ASSERT(pending);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 1u);
    }

    Y_UNIT_TEST(RequestStopFromCallbackAndConcurrentStop)
    {
        TFixture f;
        TSignal entered;
        TSignal release;
        f.Handler->OnEvent = [&](IEventHandle&)
        {
            f.Transport->RequestStop();
            entered.Set();
            release.Wait();
        };
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->Worker = std::thread([&] { f.Engine->Deliver(); });
        entered.Wait();
        auto first =
            std::async(std::launch::async, [&] { f.Transport->Stop(); });
        auto second =
            std::async(std::launch::async, [&] { f.Transport->Stop(); });
        const bool pending = !f.Connection->WhenClosed().HasValue();
        release.Set();
        UNIT_ASSERT(first.wait_for(10s) == std::future_status::ready);
        UNIT_ASSERT(second.wait_for(10s) == std::future_status::ready);
        first.get();
        second.get();
        UNIT_ASSERT(pending);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Stops.load(), 1u);
        UNIT_ASSERT(!f.Connection->Send(MakeEvent()));
    }

    Y_UNIT_TEST(OwnershipOnRejectionAndClose)
    {
        TFixture f;
        auto destroyed = std::make_shared<std::atomic<size_t>>(0);
        auto makeEvent = [&]
        {
            auto ev = std::make_unique<TEvTest>();
            ev->Destroyed = destroyed;
            return std::make_unique<IEventHandle>(
                ActorId,
                ActorId,
                ev.release());
        };
        UNIT_ASSERT(!f.Connection->Send(makeEvent()));
        UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), 1u);
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->HoldEvents = true;
        UNIT_ASSERT(f.Connection->Send(makeEvent()));
        f.Connection->Close();
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), 1u);
        UNIT_ASSERT(!f.Connection->Send(makeEvent()));
        UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), 2u);
        f.Transport->Stop();
        UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), 3u);
    }

    Y_UNIT_TEST(CallbackCanReleaseConnectionDuringActivation)
    {
        TFixture f;
        f.Handler->OnEvent = [&](IEventHandle&)
        {
            f.Connection.reset();
        };
        f.Engine->OnRegister = [&]
        {
            f.Engine->Deliver();
        };
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT(!f.Connection);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
    }

    Y_UNIT_TEST(StopWaitsForCallbackDispatchedByActivation)
    {
        TFixture f;
        TSignal entered;
        TSignal release;
        TSignal engineStopped;
        f.Engine->OnStop = [&]
        {
            engineStopped.Set();
        };
        f.Handler->OnEvent = [&](IEventHandle&)
        {
            entered.Set();
            release.Wait();
        };
        f.Engine->OnRegister = [&]
        {
            f.Engine->Deliver();
        };
        auto activation = std::async(
            std::launch::async,
            [&] { return f.Connection->Activate(); });
        entered.Wait();
        f.Transport->RequestStop();
        auto stopping =
            std::async(std::launch::async, [&] { f.Transport->Stop(); });
        engineStopped.Wait();
        const bool pending = !f.Connection->WhenClosed().HasValue();
        const bool stopWaits =
            stopping.wait_for(50ms) == std::future_status::timeout;
        release.Set();
        UNIT_ASSERT(activation.get());
        stopping.get();
        UNIT_ASSERT(pending);
        UNIT_ASSERT(stopWaits);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(CloseDuringWrite)
    {
        CheckCloseDuringWrite(false);
        CheckCloseDuringWrite(true);
    }

}   // Y_UNIT_TEST_SUITE(TIcV2TransportTest)

}   // namespace NCloud::NBlockStore::NIcV2
