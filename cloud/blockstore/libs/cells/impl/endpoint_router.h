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
struct IEndpointRouter: public IBlockStore
{
    virtual void SetTarget(IBlockStorePtr target) = 0;
};

using IEndpointRouterPtr = std::shared_ptr<IEndpointRouter>;

IEndpointRouterPtr CreateEndpointRouter(IBlockStorePtr target);

}   // namespace NCloud::NBlockStore::NCells
