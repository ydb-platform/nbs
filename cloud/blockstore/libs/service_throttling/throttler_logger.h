#pragma once

#include <cloud/blockstore/libs/throttling/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IThrottlerLoggerPtr CreateServiceThrottlerLogger(
    IRequestStatsPtr requestStats,
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore
