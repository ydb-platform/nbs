#include "transport.h"

#include <contrib/ydb/library/actors/interconnect/interconnect_direct_session.h>
#include <contrib/ydb/library/actors/util/datetime.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <optional>
#include <utility>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TConnection;

class TEngineClient final: public NActors::IUringEngineClient
{
    TAdaptiveLock Mutex;
    THashMap<NActors::TActorId, std::weak_ptr<TConnection>> Connections;
    std::atomic<ui64> DestroyCycles = 0;

public:
    void Start(const NActors::TUringEnginePtr& /*engine*/) override
    {}

    void Stop() override
    {}

    void Add(
        NActors::TActorId id,
        const std::shared_ptr<TConnection>& connection)
    {
        TGuard<TAdaptiveLock> guard(Mutex);
        Connections.emplace(id, connection);
    }

    void Remove(NActors::TActorId id)
    {
        TGuard<TAdaptiveLock> guard(Mutex);
        Connections.erase(id);
    }

    TVector<std::shared_ptr<TConnection>> GetConnections()
    {
        TVector<std::shared_ptr<TConnection>> connections;
        TGuard<TAdaptiveLock> guard(Mutex);
        for (auto& [id, weak]: Connections) {
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
        event.reset();
        buffer.Reset();
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
    // Serializes engine admission, including registration, sends, and
    // unregister. Never held while invoking a user handler or joining a worker.
    TMutex Mutex;
    NActors::TUringEnginePtr Engine;
    const std::shared_ptr<TEngineClient> Client =
        std::make_shared<TEngineClient>();
    bool Stopping = false;
    bool EngineStopping = false;
};

class TConnection final
    : public IConnection
    , public std::enable_shared_from_this<TConnection>
{
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
    const ui64 MaxOutputQueueBytes;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

    // Handle is used only under State->Mutex. Other fields below are guarded by
    // Mutex. Callbacks arriving during Register are queued until Ready is set.
    ui64 Handle = 0;
    TAdaptiveLock Mutex;
    std::shared_ptr<IConnectionHandler> Handler;
    TDeque<TNotification> Pending;
    bool Activated = false;
    bool Ready = false;
    bool Closing = false;
    bool Dispatching = false;
    bool Published = false;
    bool DisconnectReceived = false;

public:
    TConnection(
        std::shared_ptr<TTransportState> state,
        TIntrusivePtr<NInterconnect::TStreamSocket> socket,
        TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket,
        NActors::TActorId sessionId,
        std::shared_ptr<IConnectionHandler> handler,
        bool sendPings,
        ui64 maxOutputQueueBytes)
        : State(std::move(state))
        , Socket(std::move(socket))
        , XdcSocket(std::move(xdcSocket))
        , SessionId(sessionId)
        , SendPings(sendPings)
        , MaxOutputQueueBytes(maxOutputQueueBytes)
        , Handler(std::move(handler))
    {}

    ~TConnection() override
    {
        Close();
        TGuard<TMutex> admission(State->Mutex);
        State->Client->Remove(SessionId);
    }

    bool Activate() override
    {
        const auto keepAlive = shared_from_this();
        bool accepted = false;
        {
            TGuard<TMutex> admission(State->Mutex);
            {
                TGuard<TAdaptiveLock> guard(Mutex);
                if (Activated || Closing || State->Stopping) {
                    return false;
                }
                Activated = true;
            }

            const std::weak_ptr<TConnection> weak = shared_from_this();
            Handle = State->Engine->Register(
                Socket,
                SessionId,
                NActors::TScopeId(),
                [weak](const NActors::TDisconnectReason& reason)
                {
                    if (auto connection = weak.lock()) {
                        connection->OnDisconnect(reason.ToString());
                    }
                },
                SendPings,
                std::make_shared<std::atomic<int64_t>>(0),
                std::make_shared<std::atomic<uint64_t>>(0),
                XdcSocket);
            accepted = Handle != 0;
            {
                TGuard<TAdaptiveLock> guard(Mutex);
                Ready = true;
            }
        }
        if (!accepted) {
            OnDisconnect("Interconnect V2 refused the connection");
        }
        Dispatch();
        return accepted;
    }

    bool Send(std::unique_ptr<NActors::IEventHandle> ev) override
    {
        TGuard<TMutex> admission(State->Mutex);
        {
            TGuard<TAdaptiveLock> guard(Mutex);
            if (!Ready || Closing || State->Stopping || !Handle) {
                return false;
            }
        }
        if (!ev) {
            return false;
        }
        const ui64 size = ev->GetSize();
        const ui64 queued = State->Engine->GetTotalOutputQueueSize(Handle);
        if (size > MaxOutputQueueBytes || queued > MaxOutputQueueBytes - size) {
            return false;
        }
        State->Engine->Send(Handle, std::move(ev));
        return true;
    }

    void Close() override
    {
        const auto keepAlive = weak_from_this().lock();
        TDeque<TNotification> discarded;
        {
            TGuard<TMutex> admission(State->Mutex);
            {
                TGuard<TAdaptiveLock> guard(Mutex);
                Closing = true;
                discarded.swap(Pending);
            }
            // Shutdown is required by the existing engine to drain writes. No
            // engine call may touch this raw handle after Unregister.
            if (Socket) {
                Socket->Shutdown(SHUT_RDWR);
            }
            if (XdcSocket) {
                XdcSocket->Shutdown(SHUT_RDWR);
            }
            if (const ui64 handle = std::exchange(Handle, 0);
                handle && !State->EngineStopping)
            {
                State->Engine->Unregister(handle);
            }
            Socket.Reset();
            XdcSocket.Reset();
        }
        discarded.clear();
        FinishClose();
    }

    NThreading::TFuture<void> WhenClosed() const override
    {
        return Closed.GetFuture();
    }

    ui64 GetOutputQueueSize() const override
    {
        TGuard<TMutex> admission(State->Mutex);
        return Handle && !State->Stopping
                   ? State->Engine->GetTotalOutputQueueSize(Handle)
                   : 0;
    }

    void Receive(std::unique_ptr<NActors::IEventHandle> ev)
    {
        Enqueue({.Event = std::move(ev)});
    }

private:
    void OnDisconnect(TString reason)
    {
        Enqueue({.Disconnect = std::move(reason)});
    }

    void Enqueue(TNotification notification)
    {
        {
            TGuard<TAdaptiveLock> guard(Mutex);
            if (Closing || (notification.Disconnect && DisconnectReceived)) {
                return;
            }
            DisconnectReceived |= notification.Disconnect.has_value();
            Pending.push_back(std::move(notification));
        }
        Dispatch();
    }

    void Dispatch()
    {
        {
            TGuard<TAdaptiveLock> guard(Mutex);
            if (!Ready || Closing || Dispatching) {
                return;
            }
            Dispatching = true;
        }
        for (;;) {
            TNotification notification;
            std::shared_ptr<IConnectionHandler> handler;
            {
                TGuard<TAdaptiveLock> guard(Mutex);
                if (Closing || Pending.empty()) {
                    Dispatching = false;
                    break;
                }
                notification = std::move(Pending.front());
                Pending.pop_front();
                handler = Handler;
            }
            if (notification.Disconnect) {
                Close();
                handler->HandleDisconnect(*notification.Disconnect);
            } else {
                handler->HandleEvent(std::move(notification.Event));
            }
        }
        FinishClose();
    }

    void FinishClose()
    {
        std::shared_ptr<IConnectionHandler> handler;
        {
            TGuard<TAdaptiveLock> guard(Mutex);
            if (!Closing || Dispatching || Published) {
                return;
            }
            Published = true;
            handler = std::move(Handler);
        }
        handler.reset();
        Closed.SetValue();
    }
};

void TEngineClient::Send(std::unique_ptr<NActors::IEventHandle> ev)
{
    std::shared_ptr<TConnection> connection;
    {
        TGuard<TAdaptiveLock> guard(Mutex);
        if (auto it = Connections.find(ev->InterconnectSession);
            it != Connections.end())
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
    const std::shared_ptr<TTransportState> State =
        std::make_shared<TTransportState>();
    TMutex StopMutex;
    // Guarded by State->Mutex. Never reuse IDs: the engine may still deliver
    // notifications from a closed connection after its wrapper is destroyed.
    ui64 NextSessionId = 1;

public:
    explicit TTransport(NActors::TUringEnginePtr engine)
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
        bool sendPings,
        ui64 maxOutputQueueBytes) override
    {
        TGuard<TMutex> admission(State->Mutex);
        if (State->Stopping || !socket || !handler || !maxOutputQueueBytes) {
            return {};
        }
        Y_ABORT_UNLESS(
            NextSessionId,
            "Interconnect V2 connection IDs exhausted");
        const NActors::TActorId sessionId(0, 0, NextSessionId++, 0);
        auto connection = std::make_shared<TConnection>(
            State,
            std::move(socket),
            std::move(xdcSocket),
            sessionId,
            std::move(handler),
            sendPings,
            maxOutputQueueBytes);
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
            // A last connection reference can expire before its destructor
            // obtains admission. Such late destructors must not call Unregister
            // concurrently with engine shutdown.
            State->EngineStopping = true;
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

ITransportPtr CreateTransport(NActors::TUringEnginePtr engine)
{
    return engine ? std::make_shared<TTransport>(std::move(engine)) : nullptr;
}

}   // namespace NCloud::NBlockStore::NIcV2
