#pragma once

#include "config.h"

#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Use for reads, writes and zeroes. Set a positive Headers.RequestTimeout
// in milliseconds. Reuse write buffers after the call returns. For
// ReadBlocksLocal, keep buffers valid until the result is ready or close
// their Sglist before reuse. Call Stop() outside callbacks. Keep callbacks
// non-throwing. Requests already sent may still run after timeout or shutdown.
struct IDataEndpoint: public IBlockStore
{
    // Start shutdown without waiting. Safe from result callbacks.
    // Wait on the returned future outside callbacks.
    virtual NThreading::TFuture<void> RequestStop() = 0;
};

using IDataEndpointPtr = std::shared_ptr<IDataEndpoint>;

// Activate the channel before sending I/O. Keep its dependencies running until
// shutdown finishes.
TResultOrError<IDataEndpointPtr> CreateDataEndpoint(
    NCloud::NIcV2::IRequestChannelPtr channel,
    TDataConfig config);

}   // namespace NCloud::NBlockStore::NIcV2
