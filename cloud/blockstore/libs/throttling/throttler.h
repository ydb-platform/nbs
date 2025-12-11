#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IThrottler: IStartable
{
    virtual ~IThrottler() = default;

    virtual void UpdateThrottlerPolicy(IThrottlerPolicyPtr throttlerPolicy) = 0;

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                     \
    virtual NThreading::TFuture<NProto::T##name##Response> name( \
        const IBlockStorePtr& dataClient,                        \
        TCallContextPtr callContext,                             \
        std::shared_ptr<NProto::T##name##Request> request) = 0;  \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

IThrottlerPtr CreateThrottler(
    IThrottlerLoggerPtr throttlerLogger,
    IThrottlerMetricsPtr throttlerMetrics,
    IThrottlerPolicyPtr throttlerPolicy,
    IThrottlerTrackerPtr throttlerTracker,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IVolumeStatsPtr volumeStats);

}   // namespace NCloud::NBlockStore
