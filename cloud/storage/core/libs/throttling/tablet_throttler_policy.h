#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <util/datetime/base.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ITabletThrottlerPolicy
{
    virtual ~ITabletThrottlerPolicy() = default;

    virtual bool TryPostpone(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) = 0;
    [[nodiscard]] virtual TDuration GetRequestCost(
        const TThrottlingRequestInfo& requestInfo) const = 0;
    virtual TMaybe<TDuration> SuggestDelay(
        TInstant ts,
        TDuration queueTime,
        const TThrottlingRequestInfo& requestInfo) = 0;

    virtual void OnPostponedEvent(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) = 0;
};

}   // namespace NCloud
