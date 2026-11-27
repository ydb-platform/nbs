#pragma once

#include <contrib/ydb/library/actors/core/event.h>

#include <library/cpp/threading/future/future.h>

#include <memory>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

enum class ESendResult
{
    Accepted,
    NotActive,
    QueueFull,
};

////////////////////////////////////////////////////////////////////////////////

struct IConnectionHandler
{
    virtual ~IConnectionHandler() = default;

    // Callbacks may overlap and arrive out of order for the same connection.
    virtual void HandleEvent(
        std::unique_ptr<NActors::IEventHandle> ev) noexcept = 0;
    // Called at most once per connection.
    virtual void HandleDisconnect(const TString& reason) noexcept = 0;
};

struct IConnection
{
    virtual ~IConnection() = default;

    // Enables callbacks, which may start before return. Returns false if
    // already activated, closed or stopping. A connection setup failure also
    // calls HandleDisconnect().
    virtual bool Activate() = 0;

    // Consumes a non-null event even on failure. Accepted does not guarantee
    // delivery. NotActive means not activated or closed; QueueFull means the
    // transport's output limit would be exceeded.
    virtual ESendResult Send(std::unique_ptr<NActors::IEventHandle> ev) = 0;

    // Begins closure without waiting; safe in callbacks. Local closure does
    // not call HandleDisconnect(). Await WhenClosed() outside callbacks.
    virtual void Close() = 0;

    // Ready after closure, pending operations and callbacks. Does not guarantee
    // release of outgoing events. Wait outside this connection's callbacks;
    // subscribers run inline and must not throw.
    virtual NThreading::TFuture<void> WhenClosed() const = 0;
};

using IConnectionPtr = std::shared_ptr<IConnection>;

}   // namespace NCloud::NIcV2
