#include "throttler_logger.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/context.h>

#include <ydb/library/actors/core/log.h>

namespace NCloud::NFileStore::NStorage {

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

struct TThrottlerLogger::TImpl
{
private:
    TString LogTag{};
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
            "%s Postponed %s request %lu by %lu us",
            LogTag.c_str(),
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

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Added %s request %lu to the postponed queue, queue size: %lu",
            LogTag.c_str(),
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

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Advanced %s request %lu",
            LogTag.c_str(),
            methodName,
            callContext.RequestId);
    }

private:
    void TrackPostponedRequest(
        TCallContextBase& callContext,
        const char* methodName) const
    {
        FILESTORE_TRACK(
            RequestPostponed_Tablet,
            (&callContext),
            methodName);
    }

    void TrackAdvancedRequest(
        TCallContextBase& callContext,
        const char* methodName) const
    {
        FILESTORE_TRACK(
            RequestAdvanced_Tablet,
            (&callContext),
            methodName);
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

void TThrottlerLogger::LogRequestAdvanced(
    const NActors::TActorContext& ctx,
    TCallContextBase& callContext,
    const char* methodName,
    ui32 opType,
    TDuration delay) const
{
    Impl->LogRequestAdvanced(ctx, callContext, methodName, opType, delay);
}

}   // namespace NCloud::NFileStore::NStorage
