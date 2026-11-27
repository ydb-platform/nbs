#pragma once

#include "config.h"
#include "request_sender.h"

#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Use for reads, writes and zeroes. Set a positive Headers.RequestTimeout
// in milliseconds. Reuse write buffers after the call returns. For
// ReadBlocksLocal, keep buffers valid until the result is ready or close
// their Sglist before reuse. Call Stop() outside callbacks. Keep callbacks
// non-throwing. Requests already sent may still run after timeout or shutdown.
struct IDataEndpoint: public IBlockStore
{
    // Safe to call from result callbacks. Wait on the future outside callbacks.
    virtual NThreading::TFuture<void> RequestStop() = 0;

    // Observe shutdown completion without initiating it.
    [[nodiscard]] virtual NThreading::TFuture<void> WhenClosed() const = 0;
};

using IDataEndpointPtr = std::shared_ptr<IDataEndpoint>;

// Keep the channel's dependencies running until shutdown finishes.
IDataEndpointPtr CreateDataEndpoint(
    IDataRequestSenderPtr sender,
    TDataConfig config);

}   // namespace NCloud::NBlockStore::NIcV2
