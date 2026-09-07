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
// not lifetime. Forwarding them would be wrong rather than merely useless - a
// target can be an endpoint shared with others.
//
// AllocateBuffer goes to whichever target is current, with nothing tying the
// buffer to the target that will serve the request it is used for - callers
// needing registered memory must not allocate through the router.
struct IEndpointRouter: public IBlockStore
{
    virtual void SetTarget(IBlockStorePtr target) = 0;
};

using IEndpointRouterPtr = std::shared_ptr<IEndpointRouter>;

IEndpointRouterPtr CreateEndpointRouter(IBlockStorePtr target);

}   // namespace NCloud::NBlockStore::NCells
