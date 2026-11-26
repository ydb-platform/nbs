#pragma once

#include "public.h"

#include <cloud/storage/core/libs/icv2/peer/iface/status.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

using TChannelResult = TResultOrError<IRequestChannelPtr>;
using TChannelFuture = NThreading::TFuture<TChannelResult>;

////////////////////////////////////////////////////////////////////////////////

// Keep the peer returned by CreatePeer() to maintain a connection to that
// host. The manager retries failed or lost connections while both are active.
// Releasing the last peer reference closes the channel and stops reconnecting.
struct IPeer
{
    virtual ~IPeer();

    // Call concurrently to reuse an open channel or share a connection attempt.
    // Use the returned channel to send requests. It is already active.
    // The future may be ready immediately or wait for one connection attempt.
    // It can fail during retry backoff. Call again to get a replacement after
    // a disconnect. A channel obtained earlier does not follow reconnections.
    // Close() on an open peer rejects channel acquisition with E_REJECTED;
    // retry through a replacement peer or another transport.
    virtual TChannelFuture GetChannel() = 0;

    // Read a snapshot for monitoring without starting a connection. A closed
    // peer reports Disconnected with zero counters.
    virtual TPeerStatus GetStatus() = 0;

    // Close the current channel and stop reconnecting. Safe to repeat or call
    // from callbacks. Already pending callbacks may still run.
    virtual void Close() = 0;
};

}   // namespace NCloud::NIcV2
