#pragma once

#include <contrib/ydb/library/actors/core/event.h>

#include <library/cpp/threading/future/future.h>

#include <memory>

namespace NCloud::NBlockStore::NIcV2 {

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

    // Returns true if accepted for sending. Delivery is not guaranteed.
    // Rejects events exceeding the transport's shared output queue byte limit.
    virtual bool Send(std::unique_ptr<NActors::IEventHandle> ev) = 0;

    // Does not wait for callbacks; use WhenClosed() to await completion.
    // Safe from callbacks. Local closure does not invoke HandleDisconnect().
    virtual void Close() = 0;

    // Ready after closure and completion of all admitted operations and
    // callbacks. Outgoing events may still be retained after this future is
    // ready. Do not wait for this future from a callback of this connection.
    virtual NThreading::TFuture<void> WhenClosed() const = 0;

    virtual ui64 GetOutputQueueSize() const = 0;
};

using IConnectionPtr = std::shared_ptr<IConnection>;

}   // namespace NCloud::NBlockStore::NIcV2
