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
        const char* methodName) const = 0;
    virtual void LogRequestPostponedAfterSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        ui32 postponedCount,
        const char* methodName) const = 0;

    virtual void LogRequestAdvanced(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        const char* methodName,
        ui32 opType,
        TDuration delay) const = 0;
};

}   // namespace NCloud
