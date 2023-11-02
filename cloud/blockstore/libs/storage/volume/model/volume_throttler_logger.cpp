#include "volume_throttler_logger.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/throttling/tablet_throttler_logger.h>

#include <library/cpp/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

struct TVolumeThrottlerLogger::TImpl
{
private:
    ui64 TabletId;
    std::function<void(ui32, TDuration)> UpdateDelayCounterFunc;

public:
    TImpl(
            ui64 tabletId,
            std::function<void(ui32, TDuration)> updateDelayCounter)
        : TabletId(tabletId)
        , UpdateDelayCounterFunc(std::move(updateDelayCounter))
    {}

    void SetupTabletId(ui64 tabletId)
    {
        TabletId = tabletId;
    }

    void LogRequestPostponedBeforeSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        TDuration delay,
        const char* methodName) const
    {
        TrackPostponedRequest(callContext, methodName);

        LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Postponed %s request by %lu us",
            TabletId,
            methodName,
            delay.MicroSeconds());
    }

    void LogRequestPostponedAfterSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        ui32 postponedCount,
        const char* methodName) const
    {
        TrackPostponedRequest(callContext, methodName);

        LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Added %s request to the postponed queue, queue size: %lu",
            TabletId,
            methodName,
            postponedCount);
    }

    void LogPostponedRequestAdvanced(
        TCallContextBase& callContext,
        ui32 opType,
        TDuration delay) const
    {
        Y_UNUSED(callContext);
        UpdateDelayCounterFunc(opType, delay);
    }

    void LogRequestAdvanced(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        const char* methodName) const
    {
        TrackAdvancedRequest(callContext, methodName);

        LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Advanced %s request",
            TabletId,
            methodName);
    }

    void UpdateDelayCounter(ui32 opType, TDuration time)
    {
        UpdateDelayCounterFunc(opType, time);
    }

private:
    void TrackPostponedRequest(
        TCallContextBase& callContext,
        const char* methodName) const
    {
        LWTRACK(
            RequestPostponed_Volume,
            callContext.LWOrbit,
            methodName,
            callContext.RequestId);
    }

    void TrackAdvancedRequest(
        TCallContextBase& callContext,
        const char* methodName) const
    {
        LWTRACK(
            RequestAdvanced_Volume,
            callContext.LWOrbit,
            methodName,
            callContext.RequestId);
    }
};

////////////////////////////////////////////////////////////////////////////////

TVolumeThrottlerLogger::TVolumeThrottlerLogger(
        ui64 tabletId,
        std::function<void(ui32, TDuration)> updateDelayCounter)
    : Impl(std::make_unique<TImpl>(tabletId, std::move(updateDelayCounter)))
{}

TVolumeThrottlerLogger::~TVolumeThrottlerLogger() = default;

void TVolumeThrottlerLogger::SetupTabletId(ui64 tabletId)
{
    Impl->SetupTabletId(tabletId);
}

void TVolumeThrottlerLogger::LogRequestPostponedBeforeSchedule(
    const NActors::TActorContext& ctx,
    TCallContextBase& callContext,
    TDuration delay,
    const char* methodName) const
{
    Impl->LogRequestPostponedBeforeSchedule(
        ctx,
        callContext,
        delay,
        methodName);
}

void TVolumeThrottlerLogger::LogRequestPostponedAfterSchedule(
    const NActors::TActorContext& ctx,
    TCallContextBase& callContext,
    ui32 postponedCount,
    const char* methodName) const
{
    Impl->LogRequestPostponedAfterSchedule(
        ctx,
        callContext,
        postponedCount,
        methodName);
}

void TVolumeThrottlerLogger::LogPostponedRequestAdvanced(
    TCallContextBase& callContext,
    ui32 opType,
    TDuration delay) const
{
    Impl->LogPostponedRequestAdvanced(callContext, opType, delay);
}

void TVolumeThrottlerLogger::LogRequestAdvanced(
    const NActors::TActorContext& ctx,
    TCallContextBase& callContext,
    const char* methodName) const
{
    Impl->LogRequestAdvanced(ctx, callContext, methodName);
}

void TVolumeThrottlerLogger::UpdateDelayCounter(ui32 opType, TDuration time)
{
    Impl->UpdateDelayCounter(opType, time);
}

}   // namespace NCloud::NBlockStore::NStorage
