#pragma once

#include "config.h"

#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Supports ReadBlocksLocal, WriteBlocksLocal and ZeroBlocks without retries.
// Activate the channel before sending requests; Start is optional.
// Set a positive Headers.RequestTimeout in milliseconds for each request.
// Write buffers may be reused after the call; keep read buffers accessible
// until the returned future is ready, or close their Sglist before reuse.
// Call Stop outside callbacks before releasing the last reference. It closes
// the channel and waits for admitted requests and their continuations.
// Continuations must not throw or call Stop. Neither timeout nor shutdown
// cancels operations already sent to remote storage.
struct IDataEndpoint: public IBlockStore
{
    // Rejects new requests with E_CANCELLED and closes the channel without
    // waiting. Safe to call from a result continuation.
    virtual void RequestStop() = 0;
};

using IDataEndpointPtr = std::shared_ptr<IDataEndpoint>;

TResultOrError<IDataEndpointPtr> CreateDataEndpoint(
    NCloud::NIcV2::IRequestChannelPtr channel,
    ITimerPtr timer,
    TDataConfig config);

}   // namespace NCloud::NBlockStore::NIcV2
