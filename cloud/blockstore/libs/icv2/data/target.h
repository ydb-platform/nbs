#pragma once

#include "config.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Forwards incoming read/write/zero requests to the supplied IBlockStore.
// Route channel request callbacks to HandleRequest before activating channels.
// Keep the service and executor running until Stop returns; the target does
// not start or stop them. Call Stop before releasing the last reference.
struct IDataTarget
{
    virtual ~IDataTarget();

    // Replies with the result; returns without waiting for storage completion.
    virtual void HandleRequest(NCloud::NIcV2::IIncomingRequestPtr request) = 0;

    // Rejects new work without waiting or cancelling storage operations.
    // Safe to call from callbacks.
    virtual void RequestStop() = 0;

    // First close the source channels and wait for their WhenClosed futures.
    // Call outside callbacks; waits for accepted work and replies to finish.
    virtual void Stop() = 0;
};

using IDataTargetPtr = std::shared_ptr<IDataTarget>;

// The executor must enqueue tasks asynchronously.
TResultOrError<IDataTargetPtr> CreateDataTarget(
    IBlockStorePtr service,
    ITaskQueuePtr executor,
    TDataConfig config);

}   // namespace NCloud::NBlockStore::NIcV2
