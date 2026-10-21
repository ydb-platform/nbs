#pragma once

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/event.h>

#include <util/generic/ptr.h>

namespace NActors {

    // Refcounted receiver for incoming events delivered outside the actor system. An instance is
    // registered for a local ActorId; the session invokes Receive() (on its input thread) for every
    // incoming event addressed to that ActorId. Held via TIntrusivePtr, so the session co-owns the
    // receiver for as long as it stays registered.
    class IReceiveCallback : public TThrRefBase {
    public:
        virtual void Receive(TAutoPtr<IEventHandle> ev) = 0;
    };

    // Thread-safe handle to an established interconnect session that allows sending and receiving
    // events to/from the remote peer without going through the actor system. It is delivered to
    // subscribers inside TEvInterconnect::TEvNodeConnected as a std::shared_ptr and is universal: the
    // same interface is exposed by both the classic (v1) and the newer (v2) session implementations,
    // so consumers do not need to change when upgrading to a newer protocol.
    //
    // Lifecycle / races: both the session actor and every user hold a std::shared_ptr to the same
    // object. When the connection is lost the session calls Shutdown(), which transitions the handle
    // into a disconnected state (Send() starts returning false and all registered callbacks are
    // dropped). All methods are safe to call concurrently from arbitrary threads.
    class IDirectSession {
    public:
        virtual ~IDirectSession() = default;

        // Sends an event to the remote peer. The event's Recipient identifies the destination actor on
        // the peer node. If replyCallback is set, it is registered for ev->Sender BEFORE the event is
        // dispatched (equivalent to RegisterReceiveCallback(ev->Sender, replyCallback) followed by
        // Send(ev), but saving one virtual call and guaranteeing the reply cannot race ahead of the
        // registration). Returns false if the session is no longer connected. Thread-safe.
        virtual bool Send(TAutoPtr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback = nullptr) = 0;

        // Registers (or replaces) a callback invoked for incoming events addressed to localActorId.
        // Has no effect once the session is disconnected. Thread-safe.
        virtual void RegisterReceiveCallback(const TActorId& localActorId, TIntrusivePtr<IReceiveCallback> callback) = 0;

        // Removes a previously registered callback. Thread-safe.
        virtual void UnregisterReceiveCallback(const TActorId& localActorId) = 0;
    };

}
