#include "transport.h"

#include <contrib/ydb/library/actors/core/event_pb.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>
#include <contrib/ydb/library/actors/protos/unittests.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
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
#include <stdexcept>
#include <thread>
#include <utility>

namespace NCloud::NIcV2 {

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

    void HandleEvent(std::unique_ptr<IEventHandle> ev) noexcept override
    {
        ++Events;
        if (OnEvent) {
            OnEvent(*ev);
        }
    }

    void HandleDisconnect(const TString& reason) noexcept override
    {
        Y_UNUSED(reason);

        ++Disconnects;
        if (OnDisconnect) {
            OnDisconnect();
        }
    }
};

struct TTestEngine final: IUringEngine
{
    std::mutex Mutex;

    struct TRegistration
    {
        TActorId SessionId;
        bool Active;
    };

    IUringEngineClientPtr Client;
    TVector<TRegistration> Registrations;
    std::function<void(TDisconnectReason)> Disconnect;
    std::function<void()> OnRegister;
    std::function<void()> OnSend;
    std::function<void()> OnUnregister;
    std::function<void()> OnStop;
    std::atomic<size_t> Sends = 0;
    std::atomic<size_t> Unregisters = 0;
    std::atomic<size_t> Stops = 0;
    std::atomic<bool> Unregistered = false;
    ui64 RegisterResult = 1;
    std::thread Worker;
    bool HoldEvents = false;
    TVector<std::unique_ptr<IEventHandle>> PendingEvents;

    void SetClient(IUringEngineClientPtr client) override
    {
        Client = std::move(client);
        Client->Start(TUringEnginePtr(this));
    }

    ui64 Register(
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        const TActorId& sessionId,
        TScopeId peerScopeId,
        std::function<void(TDisconnectReason)> disconnect,
        bool sendPings,
        std::shared_ptr<std::atomic<int64_t>> clockSkew,
        std::shared_ptr<std::atomic<uint64_t>> pingRTT,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket) override
    {
        Y_UNUSED(socket);
        Y_UNUSED(peerScopeId);
        Y_UNUSED(sendPings);
        Y_UNUSED(clockSkew);
        Y_UNUSED(pingRTT);

        UNIT_ASSERT(!xdcSocket);
        ui64 handle = 0;
        {
            std::lock_guard guard(Mutex);
            Registrations.push_back({sessionId, RegisterResult != 0});
            handle = RegisterResult ? Registrations.size() : 0;
        }
        Disconnect = std::move(disconnect);
        if (OnRegister) {
            OnRegister();
        }
        return handle;
    }

    void Send(
        ui64 handle,
        std::unique_ptr<IEventHandle> ev,
        TIntrusivePtr<IReceiveCallback> callback) override
    {
        Y_UNUSED(callback);

        {
            std::lock_guard guard(Mutex);
            UNIT_ASSERT(Registrations.at(handle - 1).Active);
            ++Sends;
            if (HoldEvents) {
                PendingEvents.push_back(std::move(ev));
            }
        }
        if (OnSend) {
            OnSend();
        }
    }

    void Unregister(ui64 handle) override
    {
        {
            std::lock_guard guard(Mutex);
            UNIT_ASSERT(
                std::exchange(Registrations.at(handle - 1).Active, false));
            Unregistered = true;
            ++Unregisters;
        }
        if (OnUnregister) {
            OnUnregister();
        }
    }

    void RegisterReceiveCallback(
        ui64 handle,
        TActorId localActorId,
        TIntrusivePtr<IReceiveCallback> callback) override
    {
        Y_UNUSED(handle);
        Y_UNUSED(localActorId);
        Y_UNUSED(callback);

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

    void IssueMonRequest(ui64 handle, NMon::TEvHttpInfoRes::TPtr ev) override
    {
        Y_UNUSED(handle);
        Y_UNUSED(ev);
    }

    ui64 GetTotalOutputQueueSize(ui64 handle) override
    {
        Y_UNUSED(handle);

        UNIT_FAIL("Transport must not query the engine output queue size");
        return 0;
    }

    std::unique_ptr<IEventHandle> TakePendingEvent(size_t index)
    {
        std::lock_guard guard(Mutex);
        auto& pending = PendingEvents.at(index);
        UNIT_ASSERT(pending);
        return std::move(pending);
    }

    void CompletePendingEvent(size_t index)
    {
        auto ev = TakePendingEvent(index);
        if (ev->HasBuffer()) {
            Client->DestroyEvent({}, ev->ReleaseChainBuffer());
        } else {
            Client->DestroyEvent(
                std::unique_ptr<IEventBase>(ev->ReleaseBase().Release()),
                {});
        }
    }

    void Deliver(size_t registration = 0)
    {
        TActorId sessionId;
        {
            std::lock_guard guard(Mutex);
            sessionId = Registrations.at(registration).SessionId;
        }
        Client->Send(
            std::make_unique<IEventHandle>(
                sessionId,
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
    ITransportPtr Transport;
    std::shared_ptr<THandler> Handler = std::make_shared<THandler>();
    TSockets Sockets = MakeSockets();
    IConnectionPtr Connection =
        Transport->CreateConnection(Sockets[0], {}, Handler, false);

    explicit TFixture(
        ui64 maxOutputQueueBytes = 64 * 1024 * 1024,
        std::shared_ptr<IEventDestructor> eventDestructor = {})
        : Transport(CreateTransport(
              TUringEnginePtr(Engine),
              maxOutputQueueBytes,
              std::move(eventDestructor)))
    {}

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

std::unique_ptr<IEventHandle> MakeEvent(size_t metaSize)
{
    auto event = std::make_unique<TEvTest>();
    event->Record.SetMeta(TString(metaSize, 'x'));
    return std::make_unique<IEventHandle>(ActorId, ActorId, event.release());
}

ITransportPtr MakeTransport()
{
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    common->Settings.DeadPeer = TDuration::Minutes(1);
    common->Settings.V2.Threads = 1;
    common->Settings.V2.EnableSQPOLL = false;
    return CreateTransport(
        CreateUringEngine(std::move(common)),
        64 * 1024 * 1024,
        {});
}

struct TTestEventDestructor final: IEventDestructor
{
    std::unique_ptr<IEventBase> Event;
    TIntrusivePtr<TEventSerializedData> Buffer;
    size_t MetricReads = 0;

    void DestroyEvent(
        std::unique_ptr<IEventBase> event,
        TIntrusivePtr<TEventSerializedData> buffer) override
    {
        Event = std::move(event);
        Buffer = std::move(buffer);
    }

    ui64 GetAndResetDestroyEventsCycles() override
    {
        ++MetricReads;
        return 0;
    }
};

void CheckDeferredDestruction(bool serialized)
{
    auto event = MakeEvent(100);
    auto destroyed = std::make_shared<std::atomic<size_t>>(0);
    event->Get<TEvTest>()->Destroyed = destroyed;
    if (serialized) {
        event->GetChainBuffer();
    }
    const ui64 limit = event->GetSize();
    auto destructor = std::make_shared<TTestEventDestructor>();
    TFixture f(limit, destructor);
    UNIT_ASSERT(f.Connection->Activate());
    f.Engine->HoldEvents = true;
    UNIT_ASSERT(f.Connection->Send(std::move(event)));
    UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), 0u);
    f.Engine->CompletePendingEvent(0);
    UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(bool(destructor->Event), !serialized);
    UNIT_ASSERT_VALUES_EQUAL(bool(destructor->Buffer), serialized);
    UNIT_ASSERT(!f.Connection->Send(MakeEvent(1)));
    std::async(
        std::launch::async,
        [&destructor]
        {
            destructor->Event.reset();
            destructor->Buffer.Reset();
        })
        .get();
    UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), 1u);
    UNIT_ASSERT(f.Connection->Send(MakeEvent(1)));
    f.Engine->Client->GetAndResetDestroyEventsCycles();
    UNIT_ASSERT_VALUES_EQUAL(destructor->MetricReads, 0u);
    f.Engine->CompletePendingEvent(1);
    f.Transport->Stop();
    UNIT_ASSERT(destructor->Event);
}

void CheckReservationAfterHandleRelease(bool serialized)
{
    auto event = MakeEvent(100);
    if (serialized) {
        event->GetChainBuffer();
    }
    const ui64 limit = event->GetSize();
    TFixture f(limit);
    UNIT_ASSERT(f.Connection->Activate());
    f.Engine->HoldEvents = true;
    UNIT_ASSERT(f.Connection->Send(std::move(event)));
    auto pending = f.Engine->TakePendingEvent(0);
    std::unique_ptr<IEventBase> payload;
    TIntrusivePtr<TEventSerializedData> buffer;
    if (pending->HasBuffer()) {
        buffer = pending->ReleaseChainBuffer();
    } else {
        payload.reset(pending->ReleaseBase().Release());
    }
    pending.reset();
    UNIT_ASSERT(!f.Connection->Send(MakeEvent(1)));
    f.Engine->Client->DestroyEvent(std::move(payload), std::move(buffer));
    UNIT_ASSERT(f.Connection->Send(MakeEvent(1)));
}

void CheckTransportExchange(bool serialized, bool xdc)
{
    if (!TUringContext::IsAvailable()) {
        Cerr << "io_uring not available; skipping" << Endl;
        return;
    }
    auto sender = MakeTransport();
    auto receiver = MakeTransport();
    auto sockets = MakeSockets();
    auto xdcSockets = xdc ? MakeSockets() : TSockets{};
    auto sendHandler = std::make_shared<THandler>();
    auto receiveHandler = std::make_shared<THandler>();
    const TActorId recipient(2, 0, 2, 2);
    const ui32 flags = IEventHandle::MakeFlags(3, 0);
    constexpr ui64 cookie = 42;
    const TString meta("tracked event");
    const TString payload(64_KB, 'p');
    std::promise<bool> received;
    auto result = received.get_future();
    receiveHandler->OnEvent =
        [&received, recipient, flags, meta, payload](IEventHandle& ev)
    {
        bool valid = ev.Type == TEvTest::EventType && ev.Sender == ActorId &&
                     ev.Recipient == recipient && ev.Flags == flags &&
                     ev.Cookie == cookie;
        if (valid) {
            const auto* message = ev.Get<TEvTest>();
            valid = message->Record.GetMeta() == meta &&
                    message->GetPayloadCount() == 1 &&
                    message->GetPayload(0).ConvertToString() == payload;
        }
        received.set_value(valid);
    };
    auto receiveConnection = receiver->CreateConnection(
        sockets[1],
        xdcSockets[1],
        receiveHandler,
        false);
    auto sendConnection =
        sender->CreateConnection(sockets[0], xdcSockets[0], sendHandler, false);
    UNIT_ASSERT(receiveConnection->Activate());
    UNIT_ASSERT(sendConnection->Activate());
    auto message = std::make_unique<TEvTest>();
    message->Record.SetMeta(meta);
    message->AddPayload(TRope(payload));
    auto event = std::make_unique<IEventHandle>(
        recipient,
        ActorId,
        message.release(),
        flags,
        cookie);
    if (serialized) {
        event->GetChainBuffer();
    }
    UNIT_ASSERT(sendConnection->Send(std::move(event)));
    const auto status = result.wait_for(10s);
    sender->Stop();
    receiver->Stop();
    UNIT_ASSERT(status == std::future_status::ready);
    UNIT_ASSERT(result.get());
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
    auto connection =
        transport->CreateConnection(sockets[0], xdcSockets[0], handler, false);
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
    // Wait for writes to start while the unread peer keeps data queued.
    pollfd peer{static_cast<int>(*sockets[1]), POLLIN, 0};
    UNIT_ASSERT_VALUES_EQUAL(poll(&peer, 1, 10000), 1);
    connection->Close();
    UNIT_ASSERT(connection->WhenClosed().HasValue());
    transport->Stop();
    UNIT_ASSERT_VALUES_EQUAL(destroyed->load(), count);
    UNIT_ASSERT_VALUES_EQUAL(handler->Disconnects.load(), 0u);
}

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2TransportTest)
{
    Y_UNIT_TEST(TransportQueueLimitSerializesConcurrentAdmissionAndRecovers)
    {
        auto makeEvent = []
        {
            return MakeEvent(1024);
        };
        const ui64 size = makeEvent()->GetSize();
        const ui64 limit = size * 4;
        TFixture f(limit);
        auto secondSockets = MakeSockets();
        auto second = f.Transport->CreateConnection(
            secondSockets[0],
            {},
            f.Handler,
            false);
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT(second->Activate());
        f.Engine->HoldEvents = true;
        std::atomic<size_t> admitted = 0;
        std::array<std::future<void>, 8> senders;
        size_t index = 0;
        for (auto& sender: senders) {
            auto connection = index++ % 2 ? second : f.Connection;
            sender = std::async(
                std::launch::async,
                [&, connection]
                {
                    for (size_t i = 0; i < 16; ++i) {
                        admitted += connection->Send(makeEvent());
                    }
                });
        }
        for (auto& sender: senders) {
            sender.get();
        }
        UNIT_ASSERT_VALUES_EQUAL(admitted.load(), 4);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->PendingEvents.size(), 4);
        f.Engine->CompletePendingEvent(0);
        UNIT_ASSERT(second->Send(makeEvent()));
        UNIT_ASSERT(!f.Connection->Send(makeEvent()));
        for (size_t i = 1; i < f.Engine->PendingEvents.size(); ++i) {
            f.Engine->CompletePendingEvent(i);
        }
        UNIT_ASSERT(!f.Connection->Send(MakeEvent(limit)));
        UNIT_ASSERT(f.Connection->Send(makeEvent()));
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 0u);
    }

    Y_UNIT_TEST(ClosedConnectionQueueCountsUntilEngineReleasesIt)
    {
        auto event = MakeEvent(1000);
        const ui64 limit = event->GetSize();
        TFixture f(limit);
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->HoldEvents = true;
        UNIT_ASSERT(f.Connection->Send(std::move(event)));
        f.Connection->Close();
        f.Connection.reset();

        auto sockets = MakeSockets();
        auto connection =
            f.Transport->CreateConnection(sockets[0], {}, f.Handler, false);
        UNIT_ASSERT(connection->Activate());
        UNIT_ASSERT(!connection->Send(MakeEvent(1)));
        f.Engine->TakePendingEvent(0).reset();
        UNIT_ASSERT(connection->Send(MakeEvent(1)));
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 0u);
    }

    Y_UNIT_TEST(RejectsNullEngine)
    {
        UNIT_ASSERT(!CreateTransport({}, 0, {}));
        UNIT_ASSERT(!CreateTransport({}, 1024, {}));
    }

    Y_UNIT_TEST(DelegatesEventDestruction)
    {
        CheckDeferredDestruction(false);
    }

    Y_UNIT_TEST(DelegatesBufferDestruction)
    {
        CheckDeferredDestruction(true);
    }

    Y_UNIT_TEST(KeepsEventDestructorAlive)
    {
        auto engine = MakeIntrusive<TTestEngine>();
        auto destructor = std::make_shared<TTestEventDestructor>();
        const std::weak_ptr<IEventDestructor> weak = destructor;
        auto transport =
            CreateTransport(TUringEnginePtr(engine), 0, destructor);
        destructor.reset();
        UNIT_ASSERT(!weak.expired());
        engine->Client->DestroyEvent(std::make_unique<TEvTest>(), {});
        transport->Stop();
        transport.reset();
        UNIT_ASSERT(!weak.expired());
        engine.Reset();
        UNIT_ASSERT(weak.expired());
    }

    Y_UNIT_TEST(UsesDefaultTransportQueueLimit)
    {
        TFixture f(0);
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->HoldEvents = true;
        const TRope chunk(TString(1_MB, 'x'));
        TRope payload;
        for (ui64 size = 0; size < DefaultMaxOutputQueueBytes;
             size += chunk.GetSize())
        {
            payload.Insert(payload.End(), TRope(chunk));
        }
        UNIT_ASSERT(f.Connection->Send(
            std::make_unique<IEventHandle>(
                TEvTest::EventType,
                0,
                ActorId,
                ActorId,
                MakeIntrusive<TEventSerializedData>(
                    std::move(payload),
                    TEventSerializationInfo{}),
                0)));
        UNIT_ASSERT(!f.Connection->Send(MakeEvent(1)));
        f.Engine->TakePendingEvent(0).reset();
        UNIT_ASSERT(f.Connection->Send(MakeEvent(1)));
    }

    Y_UNIT_TEST(EngineDropReleasesOutputQueueReservation)
    {
        const ui64 limit = MakeEvent(100)->GetSize();
        TFixture f(limit);
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT(f.Connection->Send(MakeEvent(100)));
        UNIT_ASSERT(f.Connection->Send(MakeEvent(100)));
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Sends.load(), 2u);
    }

    Y_UNIT_TEST(SerializedBufferRetainsReservationAfterHandleIsReleased)
    {
        CheckReservationAfterHandleRelease(true);
    }

    Y_UNIT_TEST(FailedEngineSendReleasesReservation)
    {
        const ui64 limit = MakeEvent(100)->GetSize();
        TFixture f(limit);
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->OnSend = []
        {
            throw std::runtime_error("send failed");
        };
        UNIT_ASSERT_EXCEPTION(
            f.Connection->Send(MakeEvent(100)),
            std::runtime_error);
        f.Engine->OnSend = {};
        UNIT_ASSERT(f.Connection->Send(MakeEvent(100)));
    }

    Y_UNIT_TEST(EventRetainsReservationAfterHandleIsReleased)
    {
        CheckReservationAfterHandleRelease(false);
    }

    Y_UNIT_TEST(ExchangeTrackedEvents)
    {
        CheckTransportExchange(false, false);
        CheckTransportExchange(false, true);
    }

    Y_UNIT_TEST(ExchangeTrackedSerializedEvents)
    {
        CheckTransportExchange(true, false);
        CheckTransportExchange(true, true);
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
            false);
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
            false);
        UNIT_ASSERT(third->Activate());
        UNIT_ASSERT(
            f.Engine->Registrations[2].SessionId !=
            f.Engine->Registrations[0].SessionId);
        UNIT_ASSERT(
            f.Engine->Registrations[2].SessionId !=
            f.Engine->Registrations[1].SessionId);
        // Simulate a late notification for the destroyed connection.
        f.Engine->Deliver(0);
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

    Y_UNIT_TEST(EarlyMessageHandlerCanSendReply)
    {
        TFixture f;
        bool sent = false;
        f.Handler->OnEvent = [&](IEventHandle&)
        {
            sent = f.Connection->Send(MakeEvent());
        };
        f.Engine->OnRegister = [&]
        {
            f.Engine->Deliver();
        };
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT(sent);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Sends.load(), 1u);
    }

    Y_UNIT_TEST(EarlyCallbackCanCloseAndDiscardRemainingMessages)
    {
        TFixture f;
        bool pendingDuringCallback = false;
        f.Handler->OnEvent = [&](IEventHandle&)
        {
            f.Connection->Close();
            pendingDuringCallback = !f.Connection->WhenClosed().HasValue();
        };
        f.Engine->OnRegister = [&]
        {
            f.Engine->Deliver();
            f.Engine->Deliver();
        };
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT(pendingDuringCallback);
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 1u);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
    }

    Y_UNIT_TEST(ReceiveDuringRegistrationDoesNotLoseMessages)
    {
        TFixture f;
        TSignal firstQueued;
        constexpr size_t eventCount = 256;
        f.Engine->OnRegister = [&]
        {
            f.Engine->Worker = std::thread(
                [&]
                {
                    for (size_t i = 0; i < eventCount; ++i) {
                        f.Engine->Deliver();
                        if (i == 0) {
                            firstQueued.Set();
                        }
                    }
                });
            firstQueued.Wait();
            UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 0u);
        };
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->Worker.join();
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), eventCount);
    }

    Y_UNIT_TEST(ReceiveDuringConnectionRegistryUpdates)
    {
        TFixture f;
        UNIT_ASSERT(f.Connection->Activate());
        TSignal start;
        constexpr size_t eventCount = 1000;
        constexpr size_t connectionCount = 32;
        f.Engine->Worker = std::thread(
            [&]
            {
                start.Wait();
                for (size_t i = 0; i < eventCount; ++i) {
                    f.Engine->Deliver();
                }
            });
        start.Set();
        for (size_t i = 0; i < connectionCount; ++i) {
            auto sockets = MakeSockets();
            auto handler = std::make_shared<THandler>();
            auto connection =
                f.Transport->CreateConnection(sockets[0], {}, handler, false);
            UNIT_ASSERT(connection->Activate());
            f.Engine->Deliver(i + 1);
            UNIT_ASSERT_VALUES_EQUAL(handler->Events.load(), 1u);
        }
        f.Engine->Worker.join();
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), eventCount);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), connectionCount);
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
        bool pendingDuringCallback = false;
        f.Handler->OnDisconnect = [&]
        {
            pendingDuringCallback = !f.Connection->WhenClosed().HasValue();
        };
        f.Engine->RegisterResult = 0;
        UNIT_ASSERT(!f.Connection->Activate());
        UNIT_ASSERT(pendingDuringCallback);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 0u);
    }

    Y_UNIT_TEST(CloseDefersUnregisterUntilActiveSendReturns)
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
        const bool closeReturned =
            closing.wait_for(5s) == std::future_status::ready;
        const bool stillRegistered = !f.Engine->Unregistered.load();
        release.Set();
        UNIT_ASSERT(sending.get());
        closing.get();
        UNIT_ASSERT(closeReturned);
        UNIT_ASSERT(stillRegistered);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Sends.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
    }

    Y_UNIT_TEST(SendsCanEnterEngineConcurrently)
    {
        TFixture f;
        TSignal firstEntered;
        TSignal release;
        std::promise<void> secondEntered;
        auto secondEnteredFuture = secondEntered.get_future();
        std::atomic<size_t> entered = 0;
        f.Engine->OnSend = [&]
        {
            if (entered.fetch_add(1) == 0) {
                firstEntered.Set();
                release.Wait();
            } else {
                secondEntered.set_value();
            }
        };
        UNIT_ASSERT(f.Connection->Activate());
        auto first = std::async(
            std::launch::async,
            [&] { return f.Connection->Send(MakeEvent()); });
        firstEntered.Wait();
        auto second = std::async(
            std::launch::async,
            [&] { return f.Connection->Send(MakeEvent()); });
        const bool concurrent =
            secondEnteredFuture.wait_for(5s) == std::future_status::ready;
        release.Set();
        UNIT_ASSERT(first.get());
        UNIT_ASSERT(second.get());
        UNIT_ASSERT(concurrent);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Sends.load(), 2u);
    }

    Y_UNIT_TEST(StopWaitsForActiveSendBeforeStoppingEngine)
    {
        TFixture f;
        TSignal entered;
        TSignal release;
        std::atomic<bool> sendFinished = false;
        bool stoppedAfterSend = false;
        f.Engine->OnSend = [&]
        {
            entered.Set();
            release.Wait();
            sendFinished = true;
        };
        f.Engine->OnStop = [&]
        {
            stoppedAfterSend =
                sendFinished.load() && f.Engine->Unregisters.load() == 1;
        };
        UNIT_ASSERT(f.Connection->Activate());
        auto sending = std::async(
            std::launch::async,
            [&] { return f.Connection->Send(MakeEvent()); });
        entered.Wait();
        f.Transport->RequestStop();
        auto stopping =
            std::async(std::launch::async, [&] { f.Transport->Stop(); });
        const bool waits =
            stopping.wait_for(50ms) == std::future_status::timeout;
        const bool rejected = !f.Connection->Send(MakeEvent());
        release.Set();
        UNIT_ASSERT(sending.get());
        stopping.get();
        UNIT_ASSERT(waits);
        UNIT_ASSERT(rejected);
        UNIT_ASSERT(stoppedAfterSend);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Stops.load(), 1u);
    }

    Y_UNIT_TEST(RequestStopClosesConnectionsSequentially)
    {
        TFixture f;
        auto sockets = MakeSockets();
        auto second =
            f.Transport->CreateConnection(sockets[0], {}, f.Handler, false);
        UNIT_ASSERT(f.Connection->Activate());
        UNIT_ASSERT(second->Activate());
        TSignal firstClosed;
        TSignal release;
        f.Engine->OnUnregister = [&]
        {
            if (f.Engine->Unregisters.load() == 1) {
                firstClosed.Set();
                release.Wait();
            }
        };
        auto stopping =
            std::async(std::launch::async, [&] { f.Transport->RequestStop(); });
        firstClosed.Wait();
        bool firstActive;
        {
            std::lock_guard guard(f.Engine->Mutex);
            firstActive = f.Engine->Registrations[0].Active;
        }
        const auto open = firstActive ? f.Connection : second;
        const auto closed = firstActive ? second : f.Connection;
        const bool accepted = open->Send(MakeEvent());
        const bool rejected = !closed->Send(MakeEvent());
        const bool creationRejected =
            !f.Transport->CreateConnection(sockets[1], {}, f.Handler, false);
        release.Set();
        stopping.get();
        UNIT_ASSERT(accepted);
        UNIT_ASSERT(rejected);
        UNIT_ASSERT(creationRejected);
        UNIT_ASSERT(!f.Connection->Send(MakeEvent()));
        UNIT_ASSERT(!second->Send(MakeEvent()));
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 2u);
    }

    Y_UNIT_TEST(ConcurrentSendAndClose)
    {
        TFixture f;
        UNIT_ASSERT(f.Connection->Activate());
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

    Y_UNIT_TEST(ConcurrentCallbacksKeepConnectionOpenUntilBothReturn)
    {
        TFixture f;
        TSignal firstEntered;
        TSignal releaseFirst;
        TSignal releaseSecond;
        std::promise<void> secondEntered;
        auto secondEnteredFuture = secondEntered.get_future();
        std::atomic<size_t> entered = 0;
        f.Handler->OnEvent = [&](IEventHandle&)
        {
            if (entered.fetch_add(1) == 0) {
                firstEntered.Set();
                releaseFirst.Wait();
            } else {
                secondEntered.set_value();
                releaseSecond.Wait();
            }
        };
        UNIT_ASSERT(f.Connection->Activate());
        auto first =
            std::async(std::launch::async, [&] { f.Engine->Deliver(); });
        firstEntered.Wait();
        auto second =
            std::async(std::launch::async, [&] { f.Engine->Deliver(); });
        const bool concurrent =
            secondEnteredFuture.wait_for(10s) == std::future_status::ready;
        f.Connection->Close();
        const bool waitsForBoth = !f.Connection->WhenClosed().HasValue();
        releaseFirst.Set();
        first.get();
        const bool waitsForSecond = !f.Connection->WhenClosed().HasValue();
        releaseSecond.Set();
        second.get();
        UNIT_ASSERT(concurrent);
        UNIT_ASSERT(waitsForBoth);
        UNIT_ASSERT(waitsForSecond);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 2u);
    }

    Y_UNIT_TEST(DisconnectCallbackCanOverlapEventCallback)
    {
        TFixture f;
        TSignal entered;
        TSignal release;
        std::atomic<bool> eventReturned = false;
        bool overlapped = false;
        f.Handler->OnEvent = [&](IEventHandle&)
        {
            entered.Set();
            release.Wait();
            eventReturned = true;
        };
        f.Handler->OnDisconnect = [&]
        {
            overlapped = !eventReturned.load();
        };
        UNIT_ASSERT(f.Connection->Activate());
        f.Engine->Worker = std::thread([&] { f.Engine->Deliver(); });
        entered.Wait();
        f.Engine->Disconnect(TDisconnectReason::EndOfStream());
        const bool pending = !f.Connection->WhenClosed().HasValue();
        f.Engine->Disconnect(TDisconnectReason::EndOfStream());
        f.Engine->Deliver();
        release.Set();
        f.Engine->Worker.join();
        UNIT_ASSERT(overlapped);
        UNIT_ASSERT(pending);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Events.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
    }

    Y_UNIT_TEST(ConcurrentDisconnectsNotifyOnlyOnce)
    {
        TFixture f;
        TSignal start;
        TSignal entered;
        TSignal release;
        f.Handler->OnDisconnect = [&entered, &release]
        {
            entered.Set();
            release.Wait();
        };
        UNIT_ASSERT(f.Connection->Activate());
        std::array<std::future<void>, 8> disconnects;
        for (auto& disconnect: disconnects) {
            disconnect = std::async(
                std::launch::async,
                [&f, &start]
                {
                    start.Wait();
                    f.Engine->Disconnect(TDisconnectReason::EndOfStream());
                });
        }
        start.Set();
        entered.Wait();
        const bool pending = !f.Connection->WhenClosed().HasValue();
        f.Connection->Close();
        release.Set();
        for (auto& disconnect: disconnects) {
            disconnect.get();
        }
        UNIT_ASSERT(pending);
        UNIT_ASSERT_VALUES_EQUAL(f.Handler->Disconnects.load(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
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
        f.Engine->OnStop = [&]
        {
            UNIT_ASSERT_VALUES_EQUAL(f.Engine->Unregisters.load(), 1u);
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
        const bool pending = !f.Connection->WhenClosed().HasValue();
        const bool engineStillRunning = f.Engine->Stops.load() == 0;
        const bool stopWaits =
            stopping.wait_for(50ms) == std::future_status::timeout;
        release.Set();
        UNIT_ASSERT(activation.get());
        stopping.get();
        UNIT_ASSERT(pending);
        UNIT_ASSERT(stopWaits);
        UNIT_ASSERT(engineStillRunning);
        UNIT_ASSERT(f.Connection->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(CloseDuringWrite)
    {
        CheckCloseDuringWrite(false);
        CheckCloseDuringWrite(true);
    }

}   // Y_UNIT_TEST_SUITE(TIcV2TransportTest)

}   // namespace NCloud::NIcV2
