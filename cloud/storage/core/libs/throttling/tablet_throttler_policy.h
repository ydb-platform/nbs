#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// The tablet throttler retries postponed requests no more often than this.
constexpr TDuration MinPostponeQueueFlushInterval = TDuration::MilliSeconds(1);

////////////////////////////////////////////////////////////////////////////////

struct ITabletThrottlerPolicy
{
    virtual ~ITabletThrottlerPolicy() = default;

    virtual bool TryPostpone(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) = 0;
    virtual TMaybe<TDuration> SuggestDelay(
        TInstant ts,
        TDuration queueTime,
        const TThrottlingRequestInfo& requestInfo) = 0;

    virtual void OnPostponedEvent(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) = 0;

    // Optional estimate of the original limits' share of this request's cost.
    // Captured once on arrival, including requests joining a postponed queue.
    // Nothing() means unsupported or unknown; must not affect admission.
    virtual TMaybe<double> GetQuotaCostShare(
        const TThrottlingRequestInfo& requestInfo) const
    {
        Y_UNUSED(requestInfo);
        return Nothing();
    }
};

}   // namespace NCloud
