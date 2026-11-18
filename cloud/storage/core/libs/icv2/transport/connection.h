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

    // Callbacks may start before this method returns, but never before
    // activation. Repeated activation returns false.
    // Registration failure returns false and invokes HandleDisconnect().
    virtual bool Activate() = 0;

    // Consumes a non-null ev on every result. Accepted does not guarantee
    // delivery. NotActive includes calls before activation and after closure.
    // QueueFull means the shared output byte limit would be exceeded.
    virtual ESendResult Send(std::unique_ptr<NActors::IEventHandle> ev) = 0;

    // Does not wait for callbacks; use WhenClosed() to await completion.
    // Safe from callbacks. Local closure does not invoke HandleDisconnect().
    virtual void Close() = 0;

    // Ready after closure and completion of all admitted operations and
    // callbacks. Outgoing events may still be retained after this future is
    // ready. Do not wait for this future from a callback of this connection.
    // Subscribers run inline when the future becomes ready, or when subscribing
    // to an already ready future, and must not throw exceptions.
    virtual NThreading::TFuture<void> WhenClosed() const = 0;
};

using IConnectionPtr = std::shared_ptr<IConnection>;

}   // namespace NCloud::NIcV2
