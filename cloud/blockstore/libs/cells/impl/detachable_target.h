#pragma once

#include "endpoint_router.h"

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Stands between a transport switcher and the router of a connection: the
// router outlives the host the switcher belongs to, and a switcher left over
// from an abandoned host must not be able to point the router back at it.
struct IDetachableTarget: public ITransportTarget
{
    virtual void Detach() = 0;
};

using IDetachableTargetPtr = std::shared_ptr<IDetachableTarget>;

IDetachableTargetPtr CreateDetachableTarget(ITransportTargetPtr target);

}   // namespace NCloud::NBlockStore::NCells
