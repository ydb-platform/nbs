#pragma once

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/icv2/tcp/socket.h>
#include <cloud/storage/core/libs/icv2/transport/transport.h>

#include <util/generic/vector.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TRequestEventTypes
{
    ui32 RequestType = 0;
    ui32 ResponseType = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestChannelConfig
{
    // At least one pair; each event type must be nonzero and occur only once.
    TVector<TRequestEventTypes> EventTypes;
    bool SendPings = false;
    // Includes queued results; a slot is freed before its continuation runs.
    ui32 MaxPendingRequests = 1024;
};

using TResponseResult = TResultOrError<std::unique_ptr<NActors::IEventHandle>>;
// Consume the result once with ExtractValue(); do not call GetValue() first.
using TResponseFuture = NThreading::TFuture<TResponseResult>;

////////////////////////////////////////////////////////////////////////////////

// Read the event and optionally reply once. Dropping it sends no response.
struct IIncomingRequest
{
    virtual ~IIncomingRequest();

    virtual NActors::IEventHandle& GetEvent() = 0;

    // One attempt, including failures. Use the matching response type.
    // Consumes response; success means accepted for sending, not delivered.
    // Overload returns E_REJECTED; an unavailable channel returns
    // E_INVALID_STATE.
    virtual NProto::TError Reply(
        std::unique_ptr<NActors::IEventBase> response) = 0;

    virtual void CloseChannel() = 0;
};

using IIncomingRequestPtr = std::unique_ptr<IIncomingRequest>;

////////////////////////////////////////////////////////////////////////////////

// Handles incoming requests. Calls may overlap; do not throw or wait for
// closure.
struct IRequestChannelHandler
{
    virtual ~IRequestChannelHandler();

    virtual void HandleRequest(IIncomingRequestPtr request) = 0;
};

using IRequestChannelHandlerPtr = std::shared_ptr<IRequestChannelHandler>;

////////////////////////////////////////////////////////////////////////////////

struct IRequestChannel
{
    virtual ~IRequestChannel();

    // Call after the request handler owner is ready. No callbacks before this.
    virtual bool Activate() = 0;

    // Send a serializable event and await its configured response type.
    // After E_TIMEOUT, the request may still be sent or executed remotely.
    // Refusals: E_REJECTED for overload, E_INVALID_STATE for an inactive/closed
    // channel. Refused requests are not sent.
    // A mismatched response closes the channel: E_BADMSG for that request and
    // WhenClosed(), E_ABORTED for the other pending requests.
    virtual TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) = 0;

    // Safe from callbacks. Closes the connection and fails pending requests.
    // Does not wait for callbacks. Admitted results are completed on the
    // completion executor. Handlers and continuations must not throw or wait
    // for WhenClosed(), or stop/destroy the executor from its own worker.
    virtual void Close() = 0;

    // Ready after connection callbacks and result continuations finish, and
    // discarded events are released. An external thread may then stop and
    // release the completion executor. Returns the first closure reason; local
    // Close succeeds.
    [[nodiscard]] virtual NThreading::TFuture<NProto::TError>
    WhenClosed() const = 0;
};

using IRequestChannelPtr = std::shared_ptr<IRequestChannel>;

// Pass connected sockets after handshake; call Activate when the handler is
// ready. The channel keeps the handler alive until destruction.
// Use the same clock for timer and scheduler. Keep both scheduler and
// completions running until WhenClosed(), retaining an owning reference to
// completions. Completions must enqueue asynchronously.
TResultOrError<IRequestChannelPtr> CreateRequestChannel(
    const ITransportPtr& transport,
    TSocketPtr socket,
    TSocketPtr xdcSocket,
    const TRequestChannelConfig& config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    IRequestChannelHandlerPtr handler);

}   // namespace NCloud::NIcV2
