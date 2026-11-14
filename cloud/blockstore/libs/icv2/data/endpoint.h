#pragma once

#include "config.h"

#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Supports ReadBlocks, WriteBlocks, their Local variants and ZeroBlocks
// without retries.
// Activate the channel before sending requests; Start is optional.
// Set a positive Headers.RequestTimeout in milliseconds for each request.
// Write buffers may be reused after the call. Keep Local read buffers
// accessible until the future is ready, or close their Sglist before reuse.
// Release without Stop is allowed; the channel owner must outlive cleanup.
// Optional Stop waits for requests and continuations; call it outside
// callbacks. Continuations must not throw or call Stop. Neither timeout nor
// shutdown cancels operations already sent to remote storage.
struct IDataEndpoint: public IBlockStore
{
    // Rejects new requests with E_CANCELLED and closes the channel without
    // waiting. Safe to call from a result continuation.
    virtual void RequestStop() = 0;
};

using IDataEndpointPtr = std::shared_ptr<IDataEndpoint>;

TResultOrError<IDataEndpointPtr> CreateDataEndpoint(
    NCloud::NIcV2::IRequestChannelPtr channel,
    TDataConfig config);

}   // namespace NCloud::NBlockStore::NIcV2
