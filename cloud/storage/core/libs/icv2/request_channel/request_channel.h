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
    // A mismatched response to a pending request closes the channel with
    // E_BADMSG. If the timeout expires before send admission, the event is not
    // sent. After admission, timeout does not prevent sending or remote
    // execution. Overload returns E_REJECTED without sending the request.
    virtual TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) = 0;

    // Safe from callbacks. Closes the connection and fails pending requests.
    // Does not wait for callbacks. Admitted results are completed on the
    // completion executor. Handlers and continuations must not throw or wait
    // for WhenClosed(), or stop/destroy the executor from its own worker.
    virtual void Close() = 0;

    // Ready after connection callbacks and all admitted result continuations
    // finish. No new handler/result callbacks from previous work after this.
    // Returns the first closure reason; local Close succeeds.
    // Rejected calls made after Close return already completed futures.
    [[nodiscard]] virtual NThreading::TFuture<NProto::TError>
    WhenClosed() const = 0;
};

using IRequestChannelPtr = std::shared_ptr<IRequestChannel>;

// Pass connected sockets after handshake; call Activate when the handler is
// ready. The channel keeps the handler alive until destruction.
// Use the same clock for timer and scheduler. Keep scheduler and completions
// running until WhenClosed(); completions must enqueue asynchronously.
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
