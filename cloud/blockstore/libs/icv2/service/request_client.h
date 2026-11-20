#pragma once

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/icv2/peer/peer.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Requests for one peer share a subscription while waiting for its channel.
// The channel receives the remaining request time at send. Each event is sent
// at most once. Consume the response with ExtractValue().
// Asynchronous results are dispatched through completions.
// RequestStop closes the peer to complete requests already sent to a channel.
// WhenClosed waits for completion tasks and calls that enqueue them.
// Keep the completion queue running through external Stop.
// Calls outside the running lifetime and unpublished results at shutdown get
// E_CANCELLED.
struct IPeerRequestClient: public IStartable
{
    virtual NCloud::NIcV2::TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TInstant deadline) = 0;
    virtual void RequestStop() = 0;
    [[nodiscard]] virtual NThreading::TFuture<void> WhenClosed() const = 0;
};

using IPeerRequestClientPtr = std::shared_ptr<IPeerRequestClient>;

TResultOrError<IPeerRequestClientPtr> CreatePeerRequestClient(
    NCloud::NIcV2::IPeerPtr peer,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions);

}   // namespace NCloud::NBlockStore::NIcV2
