#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/public.h>
#include <cloud/blockstore/public/api/protos/volume_throttling.pb.h>

#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TShapingThrottlerConfig
{
    // The maximum amount of "budget" that can accumulate in the standard bucket.
    const TDuration BurstTime;
    // The speed of boost bucket consumption. (1.0 - normal rate only, 2.0 - consume "budget" at 2x the normal rate when boost is available)
    const ui32 BoostRate;
    // The maximum capacity of the boost bucket.
    // const TDuration BoostTime;
    // How long it takes to fully refill the boost bucket from empty.
    // const TDuration BoostRefillTime;


    // The multiplier that applies to the standard volume's "budget"
    const ui32 StandardBudgetMultiplier;

    // The multiplier that affects the total "budget" of the boost bucket.
    const ui32 BoostBudgetMultiplier;

    // The multiplier that affects the refill rate of the boost bucket.
    const ui32 BoostRefillBudgetMultiplier;

    TShapingThrottlerConfig(
        TDuration burstTime,
        ui32 boostRate,
        ui32 standardBudgetMultiplier,
        ui32 boostBudgetMultiplier,
        ui32 boostRefillBudgetMultiplier)
        : BurstTime(burstTime)
        , BoostRate(boostRate)
        , StandardBudgetMultiplier(standardBudgetMultiplier)
        , BoostBudgetMultiplier(boostBudgetMultiplier)
        , BoostRefillBudgetMultiplier(boostRefillBudgetMultiplier)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeShapingThrottler
{
private:
    // const TShapingThrottlerConfig Config;
    std::unique_ptr<TBoostedTimeBucket> Bucket;

public:
    enum class EOpType
    {
        Read,
        Write,
        Zero,
        Describe,
        Last = Describe,
    };

    explicit TVolumeShapingThrottler(
        const TShapingThrottlerConfig& throttlerConfig);
    ~TVolumeShapingThrottler();

    TDuration SuggestDelay(TInstant ts, TDuration requestCost);

    void Reset(const TShapingThrottlerConfig& config);

    [[nodiscard]] TDuration GetCurrentBoostBudget() const;
};

}   // namespace NCloud::NBlockStore::NStorage
