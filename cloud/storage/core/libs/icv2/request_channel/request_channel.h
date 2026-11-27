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
    // Maximum requests awaiting result delivery.
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
    // Overload or an unavailable channel returns E_REJECTED.
    virtual NProto::TError Reply(
        std::unique_ptr<NActors::IEventBase> response) = 0;

    virtual void CloseChannel() = 0;
};

using IIncomingRequestPtr = std::unique_ptr<IIncomingRequest>;

////////////////////////////////////////////////////////////////////////////////

// Calls may overlap. Handlers must not throw or await channel closure.
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
    // Overload or an inactive/closed channel returns E_REJECTED.
    // Refused requests are not sent.
    // A mismatched response closes the channel: E_BADMSG for that request and
    // WhenClosed(), E_ABORTED for the other pending requests.
    virtual TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) = 0;

    // Closes the connection and fails pending requests without waiting.
    // Safe in callbacks. Callbacks must not throw, await WhenClosed(), or
    // stop or destroy the completion executor.
    virtual void Close() = 0;

    // Ready after callbacks finish and discarded events are released; the
    // completion executor may then be stopped. Returns the first closure
    // error, or success after local Close().
    [[nodiscard]] virtual NThreading::TFuture<NProto::TError>
    WhenClosed() const = 0;
};

using IRequestChannelPtr = std::shared_ptr<IRequestChannel>;

// Pass sockets after handshake; call Activate() when the handler is ready.
// Use the same clock for timer and scheduler. Keep scheduler and completions
// alive and running until WhenClosed(). Completions must not run tasks inline.
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
