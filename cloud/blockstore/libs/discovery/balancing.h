#pragma once

#include "public.h"

#include "instance.h"

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct IBalancingPolicy
{
    virtual ~IBalancingPolicy() = default;

    virtual void Reorder(TInstanceList& instances) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IBalancingPolicyPtr CreateBalancingPolicy();

}   // namespace NCloud::NBlockStore::NDiscovery
