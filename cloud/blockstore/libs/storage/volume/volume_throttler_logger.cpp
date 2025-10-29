#include "volume_throttler_logger.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/throttling/tablet_throttler_logger.h>

#include <ydb/library/actors/core/log.h>

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
            "[%lu] Postponed %s request %lu by %lu us",
            TabletId,
            methodName,
            callContext.RequestId,
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
            "[%lu] Added %s request %lu to the postponed queue, queue size: %lu",
            TabletId,
            methodName,
            callContext.RequestId,
            postponedCount);
    }

    void LogRequestAdvanced(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        const char* methodName,
        ui32 opType,
        TDuration delay) const
    {
        TrackAdvancedRequest(callContext, methodName);
        UpdateDelayCounterFunc(opType, delay);

        LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] Advanced %s request %lu, with delay: %lu us",
            TabletId,
            methodName,
            callContext.RequestId,
            delay.MicroSeconds());
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

void TVolumeThrottlerLogger::LogRequestAdvanced(
    const NActors::TActorContext& ctx,
    TCallContextBase& callContext,
    const char* methodName,
    ui32 opType,
    TDuration delay) const
{
    Impl->LogRequestAdvanced(ctx, callContext, methodName, opType, delay);
}

}   // namespace NCloud::NBlockStore::NStorage
