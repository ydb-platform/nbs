#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/public.h>
#include <cloud/blockstore/public/api/protos/volume_throttling.pb.h>

#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDuration CalculateBoostTime(const NProto::TVolumePerformanceProfile& config);

////////////////////////////////////////////////////////////////////////////////

struct TShapingThrottlerConfig
{
    const TDuration BurstTime;
    const double BoostRate;
    const TDuration BoostTime;
    const TDuration BoostRefillTime;

    TShapingThrottlerConfig(
        TDuration burstTime,
        double boostRate,
        TDuration boostTime,
        TDuration boostRefillTime)
        : BurstTime(burstTime)
        , BoostRate(boostRate)
        , BoostTime(boostTime)
        , BoostRefillTime(boostRefillTime)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeShapingThrottler
{
private:
    const TShapingThrottlerConfig Config;
    TBoostedTimeBucket Bucket;

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
};

}   // namespace NCloud::NBlockStore::NStorage
