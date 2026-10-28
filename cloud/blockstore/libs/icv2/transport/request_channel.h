#pragma once

#include <cloud/storage/core/libs/icv2/tcp/socket.h>

#include <cloud/storage/core/libs/icv2/transport/transport.h>

#include <util/generic/vector.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <functional>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IConnection;
using NCloud::NIcV2::IConnectionHandler;
using NCloud::NIcV2::IConnectionPtr;
using NCloud::NIcV2::ITransport;
using NCloud::NIcV2::ITransportPtr;
using NCloud::NIcV2::TSocketPtr;

////////////////////////////////////////////////////////////////////////////////

struct TRequestEventTypes
{
    ui32 RequestType = 0;
    ui32 ResponseType = 0;
};

struct TRequestChannelConfig
{
    // Nonempty list of pairs. Every event type is nonzero and occurs once,
    // so incoming requests and responses can be distinguished unambiguously.
    TVector<TRequestEventTypes> EventTypes;
    bool SendPings = false;
    // Includes results queued for completion and running continuations.
    ui32 MaxPendingRequests = 1024;
};

using TResponseResult = TResultOrError<std::shared_ptr<NActors::IEventHandle>>;
using TResponseFuture = NThreading::TFuture<TResponseResult>;
using TRequestHandler =
    std::function<void(std::unique_ptr<NActors::IEventHandle>)>;

struct IRequestChannel
{
    virtual ~IRequestChannel() = default;

    // Call after the request handler owner is ready. No callbacks before this.
    virtual bool Activate() = 0;

    // Consumes a serializable event, assigns a unique cookie and waits for
    // the response type configured for this request type. A different
    // configured response type for a live cookie closes the channel with
    // E_BADMSG and fails pending requests.
    // Both directions have independent cookie namespaces. Cookies are never
    // reused on this channel. Late/duplicate responses are discarded.
    // Timeout includes send admission; it does not cancel remote execution.
    // Expiration is checked at most every 10 ms while requests are pending.
    // Overload returns E_REJECTED without sending the request.
    virtual TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) = 0;

    // Echo the cookie of the received request and supply its corresponding
    // response type. Rejects unconfigured response types. Consumes the event
    // even on rejection. True means accepted for sending, not delivered.
    virtual bool Reply(
        ui64 cookie,
        std::unique_ptr<NActors::IEventBase> event) = 0;

    // Safe from callbacks. Closes the connection and fails pending requests.
    // Does not wait for callbacks. Admitted results are completed on the
    // completion executor. Handlers and continuations must not throw or wait
    // for WhenClosed(), or stop/destroy the executor from its own worker.
    virtual void Close() = 0;

    // Ready after connection callbacks and all admitted result continuations
    // finish. No new handler/result callbacks from previous work after this.
    // Rejected calls made after Close return already completed futures.
    virtual NThreading::TFuture<void> WhenClosed() const = 0;
};

using IRequestChannelPtr = std::shared_ptr<IRequestChannel>;

// All configured request/response pairs share the channel's cookie namespace
// and admission limit. socket must have completed hello. Timer and scheduler
// must use the same clock; their lifecycle belongs to the caller.
// Timer callbacks hold only weak references and never retain
// events. At most one timeout callback is scheduled per channel. The completion
// executor must enqueue asynchronously and outlive WhenClosed(); the caller
// starts/stops it. It must keep processing tasks until all channels are closed.
// The handler runs outside transport locks and may run concurrently with
// other callbacks. Capture the channel weakly if it needs to Reply.
TResultOrError<IRequestChannelPtr> CreateRequestChannel(
    ITransportPtr transport,
    TSocketPtr socket,
    TSocketPtr xdcSocket,
    TRequestChannelConfig config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    TRequestHandler onRequest);

}   // namespace NCloud::NBlockStore::NIcV2
