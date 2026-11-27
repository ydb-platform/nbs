#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/connection_handler.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/icv2/peer/iface/status.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Start the transport first. Its owner drains retired endpoints at shutdown.
// Endpoints support ReadBlocks, ReadBlocksLocal, WriteBlocks, WriteBlocksLocal,
// and ZeroBlocks.
struct IDataEndpointFactory
{
    virtual ~IDataEndpointFactory();

    // Starts connection and retries without waiting for DNS or the peer.
    // Handler may run before return. Use a positive Headers.RequestTimeout
    // (ms). Writes copy buffers; keep read buffers valid until completion or
    // close Sglist. The endpoint may be released in callbacks; the transport
    // finishes cleanup asynchronously. Optional endpoint Stop waits for
    // callbacks and must be called outside them. Timeout and shutdown do not
    // cancel remote I/O.
    virtual TResultOrError<IBlockStorePtr> CreateDataEndpoint(
        const TString& cellId,
        const TString& host,
        IClientConnectionHandlerPtr handler) = 0;

    // Pass an endpoint created by this factory.
    virtual NCloud::NIcV2::TPeerStatus GetDataEndpointStatus(
        const IBlockStorePtr& endpoint) = 0;
};

}   // namespace NCloud::NBlockStore::NIcV2
