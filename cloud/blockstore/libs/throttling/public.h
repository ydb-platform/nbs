#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/public.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration UPDATE_THROTTLER_USED_QUOTA_INTERVAL =
    TDuration::Seconds(1);

constexpr TDuration TRIM_THROTTLER_METRICS_INTERVAL = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

struct IThrottler;
using IThrottlerPtr = std::shared_ptr<IThrottler>;

struct IThrottlerLogger;
using IThrottlerLoggerPtr = std::shared_ptr<IThrottlerLogger>;

struct IThrottlerMetrics;
using IThrottlerMetricsPtr = std::shared_ptr<IThrottlerMetrics>;

struct IThrottlerPolicy;
using IThrottlerPolicyPtr = std::shared_ptr<IThrottlerPolicy>;

struct IThrottlerTracker;
using IThrottlerTrackerPtr = std::shared_ptr<IThrottlerTracker>;

}   // namespace NCloud::NBlockStore
