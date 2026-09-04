#pragma once

#include <cloud/blockstore/libs/service/service.h>

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Forwards every request to the endpoint currently selected for it. Knows
// nothing about what makes one endpoint preferable to another: the target is
// chosen from the outside and may be replaced at any moment, from any thread.
//
// A request keeps the target it started on alive until it completes, so
// replacing the target never disturbs the requests already in flight.
//
// Start() and Stop() do not reach the target: the router forwards requests,
// not lifetime. Targets are torn down by being released, which is how this
// path has always worked - nothing above ever called Stop() on a cell data
// endpoint, and TControlService next to it is a no-op in the same way.
// Forwarding them would also be wrong rather than merely useless: a target may
// be an endpoint shared with others (the control service doubles as the data
// endpoint when no separate one can be made), and a replaced target would be
// missed regardless.
//
// Buffers, unlike requests, belong to the target that handed them out:
// AllocateBuffer goes to whichever target is current, and nothing ties the
// result to the target that will serve the request it is used for. Callers
// that need registered memory must not allocate through the router.
struct IEndpointRouter: public IBlockStore
{
    virtual void SetTarget(IBlockStorePtr target) = 0;
};

using IEndpointRouterPtr = std::shared_ptr<IEndpointRouter>;

IEndpointRouterPtr CreateEndpointRouter(IBlockStorePtr target);

}   // namespace NCloud::NBlockStore::NCells
