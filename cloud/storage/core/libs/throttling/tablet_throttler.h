#pragma once

#include "public.h"

#include "tablet_throttler_logger.h"
#include "tablet_throttler_policy.h"

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/kikimr/public.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class ETabletThrottlerStatus : ui8
{
    POSTPONED = 0,
    ADVANCED,
    REJECTED
};

////////////////////////////////////////////////////////////////////////////////

struct ITabletThrottler
{
    virtual ~ITabletThrottler() = default;

    virtual ui64 GetPostponedRequestsCount() const = 0;

    virtual void ResetPolicy(ITabletThrottlerPolicy& policy) = 0;

    virtual void OnShutDown(const NActors::TActorContext& ctx) = 0;

    virtual void StartFlushing(const NActors::TActorContext& ctx) = 0;

    virtual ETabletThrottlerStatus Throttle(
        const NActors::TActorContext& ctx,
        TCallContextBasePtr callContext,
        const TThrottlingRequestInfo& requestInfo,
        const std::function<NActors::IEventHandlePtr(void)>& eventReleaser,
        const char* methodName,
        TDuration* requestCost) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITabletThrottlerPtr CreateTabletThrottler(
    NActors::IActor& owner,
    ITabletThrottlerLogger& logger,
    ITabletThrottlerPolicy& policy);

}   // namespace NCloud
