#pragma once

#include <cloud/blockstore/libs/throttling/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IThrottlerTrackerPtr CreateServiceThrottlerTracker();

}   // namespace NCloud::NBlockStore
