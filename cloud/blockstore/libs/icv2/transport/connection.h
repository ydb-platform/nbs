#pragma once

#include <contrib/ydb/library/actors/core/event.h>

#include <library/cpp/threading/future/future.h>

#include <memory>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct IConnectionHandler
{
    virtual ~IConnectionHandler() = default;

    // Serialized per connection, outside transport locks. Normally called on an
    // engine worker; early notifications may be dispatched by Activate().
    // Must not block waiting for Close/Stop or throw exceptions.
    virtual void HandleEvent(std::unique_ptr<NActors::IEventHandle> ev) = 0;
    virtual void HandleDisconnect(const TString& reason) = 0;
};

struct IConnection
{
    virtual ~IConnection() = default;

    // Call after the owner and handler are ready. May invoke callbacks before
    // returning. Exactly one activation is accepted; false also means closed or
    // stopping. No user callbacks occur before activation.
    virtual bool Activate() = 0;

    // Consumes ev even on rejection. True means accepted, not delivered.
    // Concurrent sends are ordered by admission; Close may discard queued data.
    // Rejects events exceeding the connection output queue byte limit.
    virtual bool Send(std::unique_ptr<NActors::IEventHandle> ev) = 0;

    // Thread-safe, nonblocking and idempotent, including from a callback.
    // Stops admitting callbacks; an already admitted callback may still run.
    // Shuts down the socket and requests engine unregistration.
    // Local close does not call HandleDisconnect; remote failure calls it once.
    virtual void Close() = 0;

    // Ready after closure and the last user callback has returned. The engine
    // may still be draining I/O and holding outgoing events/buffers. Received
    // events already transferred to the handler belong to that handler.
    // Do not wait for this future from a callback of this connection.
    virtual NThreading::TFuture<void> WhenClosed() const = 0;

    virtual ui64 GetOutputQueueSize() const = 0;
};

using IConnectionPtr = std::shared_ptr<IConnection>;

}   // namespace NCloud::NBlockStore::NIcV2
