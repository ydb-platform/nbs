#pragma once

#include "public.h"

#include <cloud/storage/core/libs/throttling/tablet_throttler_logger.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TVolumeThrottlerLogger final
    : public ITabletThrottlerLogger
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TVolumeThrottlerLogger(
        ui64 tabletId,
        std::function<void(ui32, TDuration)> updateDelayCounter);

    ~TVolumeThrottlerLogger();

    void SetupTabletId(ui64 tabletId);

    void LogRequestPostponedBeforeSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        TDuration delay,
        const char* methodName) const override;

    void LogRequestPostponedAfterSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        ui32 postponedCount,
        const char* methodName) const override;

    void LogRequestAdvanced(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        const char* methodName,
        ui32 opType,
        TDuration delay) const override;
};

}   // namespace NCloud::NBlockStore::NStorage
