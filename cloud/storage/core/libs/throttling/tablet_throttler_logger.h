#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

#include <util/datetime/base.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ITabletThrottlerLogger
{
    virtual ~ITabletThrottlerLogger() = default;

    virtual void LogRequestPostponedBeforeSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        TDuration delay,
        const char * methodName) const = 0;
    virtual void LogRequestPostponedAfterSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        ui32 postponedCount,
        const char * methodName) const = 0;
    virtual void LogRequestPostponed(TCallContextBase& callContext) const = 0;

    virtual void LogPostponedRequestAdvanced(
        TCallContextBase& callContext,
        ui32 opType) const = 0;
    virtual void LogRequestAdvanced(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        const char * methodName) const = 0;

    virtual void UpdateDelayCounter(ui32 opType, TDuration time) = 0;
};

}   // namespace NCloud
