#pragma once

#include "public.h"

#include <cloud/storage/core/libs/icv2/peer/iface/status.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

using TRequestChannelResult = TResultOrError<IRequestChannelPtr>;
using TRequestChannelFuture = NThreading::TFuture<TRequestChannelResult>;

////////////////////////////////////////////////////////////////////////////////

// Keep the peer and manager alive to reconnect after connection failures.
// Releasing the last peer reference closes its connection.
struct IPeer
{
    virtual ~IPeer();

    // Safe to call concurrently. Returns an active channel or a connection
    // error after at most one attempt; may fail while reconnecting.
    // Call again after disconnection to get a new channel.
    // After Close(), returns E_REJECTED; create a new peer to reconnect.
    virtual TRequestChannelFuture GetRequestChannel() = 0;

    // Does not start a connection. Closed peers report Disconnected and zero
    // counters.
    virtual TPeerStatus GetStatus() = 0;

    // Closes the connection and stops reconnecting. Safe in callbacks and
    // repeatable; pending callbacks may still run.
    virtual void Close() = 0;
};

}   // namespace NCloud::NIcV2
