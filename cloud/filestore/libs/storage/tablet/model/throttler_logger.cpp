#include "throttler_logger.h"

#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/context.h>

#include <library/cpp/actors/core/log.h>

namespace NCloud::NFileStore::NStorage {

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

struct TThrottlerLogger::TImpl
{
private:
    TString LogTag;
    std::function<void(ui32, TDuration)> UpdateDelayCounterFunc;

public:
    TImpl(std::function<void(ui32, TDuration)> updateDelayCounter)
        : UpdateDelayCounterFunc(std::move(updateDelayCounter))
    {}

    void SetupLogTag(TString logTag)
    {
        LogTag = std::move(logTag);
    }

    void LogRequestPostponedBeforeSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        TDuration delay,
        const char* methodName) const
    {
        TrackPostponedRequest(callContext, methodName);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Postponed %s request by %lu us",
            LogTag.c_str(),
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

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Added %s request to the postponed queue, queue size: %lu",
            LogTag.c_str(),
            methodName,
            postponedCount);
    }

    void LogRequestPostponed(TCallContextBase& callContext) const
    {
        callContext.Postpone(GetCycleCount());
    }

    void LogPostponedRequestAdvanced(
        TCallContextBase& callContext,
        ui32 opType) const
    {
        const auto delay = callContext.Advance(GetCycleCount());
        UpdateDelayCounterFunc(opType, delay);
    }

    void LogRequestAdvanced(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        const char* methodName) const
    {
        TrackAdvancedRequest(callContext, methodName);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Advanced %s request",
            LogTag.c_str(),
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
            RequestPostponed_Tablet,
            callContext.LWOrbit,
            methodName,
            callContext.RequestId);
    }

    void TrackAdvancedRequest(
        TCallContextBase& callContext,
        const char* methodName) const
    {
        LWTRACK(
            RequestAdvanced_Tablet,
            callContext.LWOrbit,
            methodName,
            callContext.RequestId);
    }
};

////////////////////////////////////////////////////////////////////////////////

TThrottlerLogger::TThrottlerLogger(std::function<void(ui32, TDuration)> updateDelayCounter)
    : Impl(std::make_unique<TImpl>(std::move(updateDelayCounter)))
{}

TThrottlerLogger::~TThrottlerLogger() = default;

void TThrottlerLogger::SetupLogTag(TString logTag)
{
    Impl->SetupLogTag(std::move(logTag));
}

void TThrottlerLogger::LogRequestPostponedBeforeSchedule(
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

void TThrottlerLogger::LogRequestPostponedAfterSchedule(
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

void TThrottlerLogger::LogRequestPostponed(TCallContextBase& callContext) const
{
    Impl->LogRequestPostponed(callContext);
}

void TThrottlerLogger::LogPostponedRequestAdvanced(
    TCallContextBase& callContext,
    ui32 opType) const
{
    Impl->LogPostponedRequestAdvanced(callContext, opType);
}

void TThrottlerLogger::LogRequestAdvanced(
    const NActors::TActorContext& ctx,
    TCallContextBase& callContext,
    const char* methodName) const
{
    Impl->LogRequestAdvanced(ctx, callContext, methodName);
}

void TThrottlerLogger::UpdateDelayCounter(ui32 opType, TDuration time)
{
    Impl->UpdateDelayCounter(opType, time);
}

}   // namespace NCloud::NFileStore::NStorage
