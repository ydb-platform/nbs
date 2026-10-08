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

// How a request would have been throttled if only the original performance
// profile of the tablet had limited it.
struct TQuotaReference
{
    TDuration Delay;
    bool Rejected = false;
};

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

    // Called once per request when it reaches the throttler, before
    // SuggestDelay or TryPostpone. Must not affect throttling decisions.
    // Nothing() means the quota delay is not measured.
    virtual TMaybe<TQuotaReference> RegisterQuotaReference(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo)
    {
        Y_UNUSED(ts);
        Y_UNUSED(requestInfo);
        return Nothing();
    }
};

}   // namespace NCloud
