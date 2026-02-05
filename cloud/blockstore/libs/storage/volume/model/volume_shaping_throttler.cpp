#include "volume_shaping_throttler.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/throttling/helpers.h>
#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

TDuration CalculateBoostTime(const TShapingThrottlerConfig& config)
{
    if (config.BoostRate <= 1.) {
        return TDuration::Zero();
    }

    return (config.BoostRate - 1.) * config.BoostTime;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVolumeShapingThrottler::TVolumeShapingThrottler(
    const TShapingThrottlerConfig& throttlerConfig)
    : Config(throttlerConfig)
    , Bucket(
          Config.BurstTime,
          Config.BoostRate,
          CalculateBoostTime(Config),
          Config.BoostRefillTime,
          CalculateBoostTime(
              Config))   // TODO(komarevtsev-d): don't reset initial boost time
{}

TVolumeShapingThrottler::~TVolumeShapingThrottler() = default;

TDuration TVolumeShapingThrottler::SuggestDelay(
    TInstant ts,
    TDuration requestCost)
{
    return Bucket.Register(ts, requestCost);
}

}   // namespace NCloud::NBlockStore::NStorage
