#pragma once

#include "public.h"

#include "host_connection.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Creates per-host data endpoints. Start the transport before creating
// endpoints; its owner is responsible for shutdown.
struct IDataEndpointFactory
{
    virtual ~IDataEndpointFactory();

    // Call from a setup thread: host resolution may block. Each successful
    // call creates a separate binding; connection may wait for the first I/O.
    // Supports ReadBlocksLocal, WriteBlocksLocal and ZeroBlocks, not control
    // requests. The endpoint retains handler; use a weak reference back to its
    // owner. A binding can reconnect and report more than one disconnection.
    // Set a positive Headers.RequestTimeout in milliseconds on every request.
    // Write buffers may be reused after the call; keep read buffers accessible
    // until the future is ready, or close their Sglist before reuse.
    // The endpoint needs no Start. Call its Stop outside callbacks to reject
    // new data requests and drain admitted requests and continuations. Keep the
    // transport alive until those requests and continuations finish. Neither
    // timeout nor shutdown cancels operations already sent to remote storage.
    virtual TResultOrError<IBlockStorePtr> CreateDataEndpoint(
        const TString& cellId,
        const TString& host,
        IHostConnectionHandlerPtr handler) = 0;

    // Pass an endpoint created by this factory. Reading its status does not
    // start a connection.
    virtual THostChannelStatus GetDataEndpointStatus(
        const IBlockStorePtr& endpoint) = 0;
};

}   // namespace NCloud::NBlockStore::NIcV2
