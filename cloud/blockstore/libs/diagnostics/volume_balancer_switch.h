#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IVolumeBalancerSwitch
{
    virtual ~IVolumeBalancerSwitch() = default;

    virtual void EnableVolumeBalancer() = 0;
    virtual bool IsBalancerEnabled() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IVolumeBalancerSwitchPtr CreateVolumeBalancerSwitch();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
