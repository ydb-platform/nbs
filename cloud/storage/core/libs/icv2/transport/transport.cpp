#include "transport.h"

#include <contrib/ydb/library/actors/interconnect/interconnect_direct_session.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/util/datetime.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <functional>
#include <optional>
#include <utility>

namespace NCloud::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GetSerializedEventSize(NActors::IEventHandle& ev)
{
    if (ev.HasBuffer()) {
        return ev.GetChainBuffer()->GetSize();
    }
    if (ev.HasEvent()) {
        return ev.GetBase()->CalculateSerializedSizeCached();
    }
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

// Shared byte budget for outgoing events, including events retained after
// Close().
class TOutputQueueBudget final: public TAtomicRefCount<TOutputQueueBudget>
{
private:
    const ui64 Limit;
    std::atomic<ui64> Bytes = 0;

public:
    explicit TOutputQueueBudget(ui64 limit)
        : Limit(limit)
    {}

    bool TryAcquire(ui64 size)
    {
        if (size > Limit) {
            return false;
        }
        ui64 current = Bytes.load(std::memory_order_relaxed);
        for (;;) {
            if (current > Limit - size) {
                return false;
            }
            if (Bytes.compare_exchange_weak(
                    current,
                    current + size,
                    std::memory_order_relaxed))
            {
                return true;
            }
        }
    }

    void Release(ui64 size)
    {
        const ui64 previous = Bytes.fetch_sub(size, std::memory_order_relaxed);
        Y_DEBUG_ABORT_UNLESS(previous >= size);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Returns reserved bytes when the last owner of an outgoing payload releases
// it.
class TOutputQueueReservation final
{
private:
    TIntrusivePtr<TOutputQueueBudget> Budget;
    const ui64 Size;

public:
    TOutputQueueReservation(TIntrusivePtr<TOutputQueueBudget> budget, ui64 size)
        : Budget(std::move(budget))
        , Size(size)
    {}

    TOutputQueueReservation(TOutputQueueReservation&&) noexcept = default;
    TOutputQueueReservation(const TOutputQueueReservation&) = delete;
    TOutputQueueReservation& operator=(const TOutputQueueReservation&) = delete;

    ~TOutputQueueReservation()
    {
        if (Budget) {
            Budget->Release(Size);
        }
    }

    ui64 GetSize() const
    {
        return Size;
    }
};

////////////////////////////////////////////////////////////////////////////////

// Keeps the reservation until the engine releases an unserialized event.
class TQueuedEvent final: public NActors::IEventBase
{
private:
    TOutputQueueReservation Reservation;
    std::unique_ptr<NActors::IEventHandle> Event;

public:
    TQueuedEvent(
        std::unique_ptr<NActors::IEventHandle> event,
        TOutputQueueReservation reservation)
        : Reservation(std::move(reservation))
        , Event(std::move(event))
    {}

    TString ToStringHeader() const override
    {
        return Event->GetBase()->ToStringHeader();
    }

    TString ToString() const override
    {
        return Event->GetBase()->ToString();
    }

    ui32 Type() const override
    {
        return Event->Type;
    }

    ui32 CalculateSerializedSize() const override
    {
        return Reservation.GetSize();
    }

    ui32 CalculateSerializedSizeCached() const override
    {
        return Reservation.GetSize();
    }

    bool SerializeToArcadiaStream(
        NActors::TChunkSerializer* serializer) const override
    {
        return Event->GetBase()->SerializeToArcadiaStream(serializer);
    }

    std::optional<TRope> SerializeToRope(
        IRcBufAllocator* allocator) const override
    {
        return Event->GetBase()->SerializeToRope(allocator);
    }

    bool IsSerializable() const override
    {
        return Event->GetBase()->IsSerializable();
    }

    NActors::TEventSerializationInfo CreateSerializationInfo() const override
    {
        return Event->GetBase()->CreateSerializationInfo();
    }
};

////////////////////////////////////////////////////////////////////////////////

// Tracks a serialized buffer even after the engine releases its event handle.
class TQueuedBuffer final: public NActors::TEventSerializedData
{
private:
    TOutputQueueReservation Reservation;
    std::unique_ptr<NActors::IEventHandle> Event;

public:
    TQueuedBuffer(
        std::unique_ptr<NActors::IEventHandle> event,
        TOutputQueueReservation reservation)
        : NActors::TEventSerializedData(
              event->GetChainBuffer()->GetRope(),
              NActors::TEventSerializationInfo(
                  event->GetChainBuffer()->GetSerializationInfo()))
        , Reservation(std::move(reservation))
        , Event(std::move(event))
    {}
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IEventHandle> TrackOutputEvent(
    std::unique_ptr<NActors::IEventHandle> event,
    TOutputQueueReservation reservation)
{
    auto* const original = event.get();
    std::unique_ptr<NActors::IEventHandle> tracked;
    if (event->HasBuffer()) {
        auto buffer = MakeIntrusive<TQueuedBuffer>(
            std::move(event),
            std::move(reservation));
        tracked = std::make_unique<NActors::IEventHandle>(
            original->InterconnectSession,
            original->Type,
            original->Flags,
            original->Recipient,
            original->Sender,
            std::move(buffer),
            original->Cookie,
            original->OriginScopeId,
            std::move(original->TraceId));
    } else {
        auto payload = std::make_unique<TQueuedEvent>(
            std::move(event),
            std::move(reservation));
        const auto forward = original->GetForwardOnNondeliveryRecipient();
        tracked = std::make_unique<NActors::IEventHandle>(
            original->Recipient,
            original->Sender,
            payload.release(),
            original->Flags,
            original->Cookie,
            forward ? &forward : nullptr,
            std::move(original->TraceId));
    }
    tracked->Rewrite(
        original->GetTypeRewrite(),
        original->GetRecipientRewrite());
    return tracked;
}

////////////////////////////////////////////////////////////////////////////////

class TConnection;

using TConnections = THashMap<NActors::TActorId, std::weak_ptr<TConnection>>;

// Immutable routing table shared by concurrent readers.
class TConnectionRegistry final: public TAtomicRefCount<TConnectionRegistry>
{
private:
    const TConnections Connections;

public:
    explicit TConnectionRegistry(TConnections connections)
        : Connections(std::move(connections))
    {}

    const TConnections& GetConnections() const
    {
        return Connections;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEngineClient final: public NActors::IUringEngineClient
{
    const std::shared_ptr<NActors::IEventDestructor> EventDestructor;
    TAdaptiveLock UpdateMutex;
    THotSwap<TConnectionRegistry> Registry{
        MakeIntrusive<TConnectionRegistry>(TConnections{})};
    std::atomic<ui64> DestroyCycles = 0;

public:
    explicit TEngineClient(
        std::shared_ptr<NActors::IEventDestructor> eventDestructor)
        : EventDestructor(std::move(eventDestructor))
    {}

    void Start(const NActors::TUringEnginePtr& engine) override
    {
        Y_UNUSED(engine);
    }

    void Stop() override
    {}

    void Add(
        NActors::TActorId id,
        const std::shared_ptr<TConnection>& connection)
    {
        TGuard<TAdaptiveLock> guard(UpdateMutex);
        auto connections = Registry.AtomicLoad()->GetConnections();
        connections.emplace(id, connection);
        Registry.AtomicStore(
            MakeIntrusive<TConnectionRegistry>(std::move(connections)));
    }

    void Remove(NActors::TActorId id)
    {
        TGuard<TAdaptiveLock> guard(UpdateMutex);
        auto connections = Registry.AtomicLoad()->GetConnections();
        connections.erase(id);
        Registry.AtomicStore(
            MakeIntrusive<TConnectionRegistry>(std::move(connections)));
    }

    TVector<std::shared_ptr<TConnection>> GetConnections()
    {
        TVector<std::shared_ptr<TConnection>> connections;
        const auto registry = Registry.AtomicLoad();
        for (const auto& [id, weak]: registry->GetConnections()) {
            if (auto connection = weak.lock()) {
                connections.push_back(std::move(connection));
            }
        }
        return connections;
    }

    void Send(std::unique_ptr<NActors::IEventHandle> ev) override;

    void DestroyEvent(
        std::unique_ptr<NActors::IEventBase> event,
        TIntrusivePtr<NActors::TEventSerializedData> buffer) override
    {
        const ui64 start = GetCycleCountFast();
        if (EventDestructor) {
            EventDestructor->DestroyEvent(std::move(event), std::move(buffer));
        } else {
            event.reset();
            buffer.Reset();
        }
        DestroyCycles.fetch_add(
            GetCycleCountFast() - start,
            std::memory_order_relaxed);
    }

    ui64 GetAndResetDestroyEventsCycles() override
    {
        return DestroyCycles.exchange(0, std::memory_order_relaxed);
    }
};

struct TTransportState
{
    const TIntrusivePtr<TOutputQueueBudget> OutputQueueBudget;
    // Do not hold while calling user code or joining workers.
    TMutex Mutex;
    NActors::TUringEnginePtr Engine;
    const std::shared_ptr<TEngineClient> Client;
    bool Stopping = false;
    bool EngineStopping = false;
    // Includes closure subscribers; Stop() releases the initial reference after
    // connection creation is disabled.
    std::atomic<size_t> PendingConnections = 1;
    NThreading::TPromise<void> ConnectionsDrained =
        NThreading::NewPromise<void>();

    TTransportState(
        ui64 maxOutputQueueBytes,
        std::shared_ptr<NActors::IEventDestructor> eventDestructor)
        : OutputQueueBudget(
              MakeIntrusive<TOutputQueueBudget>(maxOutputQueueBytes))
        , Client(std::make_shared<TEngineClient>(std::move(eventDestructor)))
    {}

    void ReleaseConnection()
    {
        const size_t previous =
            PendingConnections.fetch_sub(1, std::memory_order_acq_rel);
        Y_DEBUG_ABORT_UNLESS(previous);

        if (previous == 1) {
            ConnectionsDrained.SetValue();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Keeps the handle and handler alive until all admitted operations return.
class TActiveConnectionState final
    : public TAtomicRefCount<TActiveConnectionState>
{
private:
    const std::shared_ptr<TTransportState> State;
    NActors::TUringEnginePtr Engine;
    std::shared_ptr<IConnectionHandler> Handler;
    NThreading::TPromise<void> Closed;
    // Set once, before publication.
    ui64 Handle = 0;

public:
    TActiveConnectionState(
        std::shared_ptr<TTransportState> state,
        std::shared_ptr<IConnectionHandler> handler,
        NThreading::TPromise<void> closed)
        : State(std::move(state))
        , Engine(State->Engine)
        , Handler(std::move(handler))
        , Closed(std::move(closed))
    {}

    ~TActiveConnectionState()
    {
        if (Handle) {
            Engine->Unregister(Handle);
        }
        Engine.Reset();
        Handler.reset();
        Closed.SetValue();
        State->ReleaseConnection();
    }

    bool Register(
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        NActors::TActorId sessionId,
        std::function<void(NActors::TDisconnectReason)> onDisconnect,
        bool sendPings,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket)
    {
        Y_DEBUG_ABORT_UNLESS(!Handle);

        Handle = Engine->Register(
            std::move(socket),
            sessionId,
            NActors::TScopeId(),
            std::move(onDisconnect),
            sendPings,
            std::make_shared<std::atomic<int64_t>>(0),
            std::make_shared<std::atomic<uint64_t>>(0),
            std::move(xdcSocket));
        return Handle != 0;
    }

    void Send(std::unique_ptr<NActors::IEventHandle> ev) const
    {
        Engine->Send(Handle, std::move(ev));
    }

    void HandleEvent(std::unique_ptr<NActors::IEventHandle> ev) const noexcept
    {
        Handler->HandleEvent(std::move(ev));
    }

    void HandleDisconnect(const TString& reason) const noexcept
    {
        Handler->HandleDisconnect(reason);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConnection final
    : public IConnection
    , public std::enable_shared_from_this<TConnection>
{
    enum class EConnectionState
    {
        Created,
        Activating,
        Active,
        Closing,
    };

    struct TNotification
    {
        std::unique_ptr<NActors::IEventHandle> Event;
        std::optional<TString> Disconnect;
    };

    const std::shared_ptr<TTransportState> State;
    TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
    TIntrusivePtr<NInterconnect::TStreamSocket> XdcSocket;
    const NActors::TActorId SessionId;
    const bool SendPings;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();
    THotSwap<TActiveConnectionState> ActiveState;

    // Fields below are guarded by Mutex.
    TAdaptiveLock Mutex;
    std::shared_ptr<IConnectionHandler> Handler;
    TDeque<TNotification> Pending;
    EConnectionState ConnectionState = EConnectionState::Created;

public:
    TConnection(
        std::shared_ptr<TTransportState> state,
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket,
        NActors::TActorId sessionId,
        std::shared_ptr<IConnectionHandler> handler,
        bool sendPings)
        : State(std::move(state))
        , Socket(std::move(socket))
        , XdcSocket(std::move(xdcSocket))
        , SessionId(sessionId)
        , SendPings(sendPings)
        , Handler(std::move(handler))
    {
        State->PendingConnections.fetch_add(1, std::memory_order_relaxed);
    }

    ~TConnection() override
    {
        Close();
        State->Client->Remove(SessionId);
    }

    bool Activate() override
    {
        const auto keepAlive = shared_from_this();
        TIntrusivePtr<TActiveConnectionState> active;
        TDeque<TNotification> pending;
        bool completed = false;

        Y_DEFER
        {
            if (active && !completed) {
                Close();
            }
        };

        bool accepted = false;
        {
            TGuard<TMutex> admission(State->Mutex);
            {
                TGuard<TAdaptiveLock> guard(Mutex);
                if (ConnectionState != EConnectionState::Created ||
                    State->Stopping)
                {
                    return false;
                }
                active = MakeIntrusive<TActiveConnectionState>(
                    State,
                    Handler,
                    Closed);
                Handler.reset();
                ConnectionState = EConnectionState::Activating;
            }

            accepted = active->Register(
                Socket,
                SessionId,
                [weak = weak_from_this()]   //
                (const NActors::TDisconnectReason& reason)
                {
                    if (auto connection = weak.lock()) {
                        connection->OnDisconnect(reason.ToString());
                    }
                },
                SendPings,
                XdcSocket);
            {
                TGuard<TAdaptiveLock> guard(Mutex);
                if (accepted) {
                    ActiveState.AtomicStore(active);
                    ConnectionState = EConnectionState::Active;
                }
                pending.swap(Pending);
            }
        }
        completed = true;
        if (!accepted) {
            Close();
            active->HandleDisconnect("Interconnect V2 refused the connection");
        } else {
            for (auto& notification: pending) {
                if (notification.Disconnect) {
                    OnDisconnect(std::move(*notification.Disconnect));
                } else {
                    Receive(std::move(notification.Event));
                }
            }
        }
        return accepted;
    }

    bool Send(std::unique_ptr<NActors::IEventHandle> ev) override
    {
        Y_DEBUG_ABORT_UNLESS(ev);

        const auto active = ActiveState.AtomicLoad();
        if (!active) {
            return false;
        }

        if (const ui64 size = GetSerializedEventSize(*ev)) {
            if (!State->OutputQueueBudget->TryAcquire(size)) {
                return false;
            }
            ev = TrackOutputEvent(
                std::move(ev),
                TOutputQueueReservation(State->OutputQueueBudget, size));
        }
        active->Send(std::move(ev));
        return true;
    }

    void Close() override
    {
        const auto keepAlive = weak_from_this().lock();
        CloseConnection();
    }

    NThreading::TFuture<void> WhenClosed() const override
    {
        return Closed.GetFuture();
    }

    void Receive(std::unique_ptr<NActors::IEventHandle> ev)
    {
        auto active = ActiveState.AtomicLoad();
        if (!active) {
            TGuard<TAdaptiveLock> guard(Mutex);
            if (ConnectionState == EConnectionState::Closing) {
                return;
            }
            if (ConnectionState != EConnectionState::Active) {
                Pending.push_back({.Event = std::move(ev)});
                return;
            }
            active = ActiveState.AtomicLoad();
        }
        if (active) {
            active->HandleEvent(std::move(ev));
        }
    }

private:
    // Only the caller that closes the connection receives its active state.
    TIntrusivePtr<TActiveConnectionState> CloseConnection()
    {
        TDeque<TNotification> discarded;
        TIntrusivePtr<TActiveConnectionState> active;
        std::shared_ptr<IConnectionHandler> handler;
        bool complete = false;
        {
            TGuard<TMutex> admission(State->Mutex);
            {
                TGuard<TAdaptiveLock> guard(Mutex);
                if (ConnectionState == EConnectionState::Closing) {
                    return {};
                }
                complete = ConnectionState == EConnectionState::Created;
                ConnectionState = EConnectionState::Closing;
                handler = std::move(Handler);
                discarded.swap(Pending);
            }
            active = ActiveState.AtomicLoad();
            ActiveState.AtomicStore(nullptr);
            // Shutdown is required by the existing engine to drain writes.
            if (Socket) {
                Socket->Shutdown(SHUT_RDWR);
            }
            if (XdcSocket) {
                XdcSocket->Shutdown(SHUT_RDWR);
            }
            Socket.Reset();
            XdcSocket.Reset();
        }
        discarded.clear();
        handler.reset();
        if (complete) {
            Closed.SetValue();
            State->ReleaseConnection();
        }
        return active;
    }

    void OnDisconnect(TString reason)
    {
        {
            TGuard<TAdaptiveLock> guard(Mutex);
            if (ConnectionState == EConnectionState::Closing) {
                return;
            }
            if (ConnectionState != EConnectionState::Active) {
                Pending.push_back({.Disconnect = std::move(reason)});
                return;
            }
        }
        if (const auto active = CloseConnection()) {
            active->HandleDisconnect(reason);
        }
    }
};

void TEngineClient::Send(std::unique_ptr<NActors::IEventHandle> ev)
{
    std::shared_ptr<TConnection> connection;
    {
        const auto registry = Registry.AtomicLoad();
        const auto& connections = registry->GetConnections();
        if (auto it = connections.find(ev->InterconnectSession);
            it != connections.end())
        {
            connection = it->second.lock();
        }
    }
    if (connection) {
        connection->Receive(std::move(ev));
    }
}

class TTransport final: public ITransport
{
    const std::shared_ptr<TTransportState> State;
    TMutex StopMutex;
    // IDs must not be reused: notifications can outlive their connection.
    ui64 NextSessionId = 1;

public:
    TTransport(
        NActors::TUringEnginePtr engine,
        ui64 maxOutputQueueBytes,
        std::shared_ptr<NActors::IEventDestructor> eventDestructor)
        : State(
              std::make_shared<TTransportState>(
                  maxOutputQueueBytes,
                  std::move(eventDestructor)))
    {
        State->Engine = std::move(engine);
        State->Engine->SetClient(State->Client);
    }

    ~TTransport() override
    {
        Stop();
    }

    IConnectionPtr CreateConnection(
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket,
        std::shared_ptr<IConnectionHandler> handler,
        bool sendPings) override
    {
        TGuard<TMutex> admission(State->Mutex);
        if (State->Stopping || !socket || !handler) {
            return {};
        }

        const NActors::TActorId sessionId(0, 0, NextSessionId++, 0);
        auto connection = std::make_shared<TConnection>(
            State,
            std::move(socket),
            std::move(xdcSocket),
            sessionId,
            std::move(handler),
            sendPings);
        State->Client->Add(sessionId, connection);
        return connection;
    }

    void RequestStop() override
    {
        CloseConnections();
    }

    void Stop() override
    {
        TGuard<TMutex> stop(StopMutex);
        auto connections = CloseConnections();
        NActors::TUringEnginePtr engine;
        {
            TGuard<TMutex> admission(State->Mutex);
            if (State->EngineStopping) {
                return;
            }
            State->EngineStopping = true;
            State->ReleaseConnection();
        }
        State->ConnectionsDrained.GetFuture().GetValueSync();
        {
            TGuard<TMutex> admission(State->Mutex);
            engine = std::move(State->Engine);
        }
        engine->Stop();
        for (const auto& connection: connections) {
            connection->WhenClosed().GetValueSync();
        }
    }

private:
    TVector<std::shared_ptr<TConnection>> CloseConnections()
    {
        TVector<std::shared_ptr<TConnection>> connections;
        {
            TGuard<TMutex> admission(State->Mutex);
            State->Stopping = true;
            connections = State->Client->GetConnections();
        }
        for (const auto& connection: connections) {
            connection->Close();
        }
        return connections;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITransportPtr CreateTransport(
    NActors::TUringEnginePtr engine,
    ui64 maxOutputQueueBytes,
    std::shared_ptr<NActors::IEventDestructor> eventDestructor)
{
    return engine ? std::make_shared<TTransport>(
                        std::move(engine),
                        maxOutputQueueBytes ? maxOutputQueueBytes
                                            : DefaultMaxOutputQueueBytes,
                        std::move(eventDestructor))
                  : nullptr;
}

}   // namespace NCloud::NIcV2
