#include "volume_shaping_throttler.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/throttling/helpers.h>
#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage {

namespace {


}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVolumeShapingThrottler::TVolumeShapingThrottler(
    const TShapingThrottlerConfig& config)
    // TODO(komarevtsev-d): don't reset initial boost time
    : Bucket(
          std::make_unique<TBoostedTimeBucket>(
              config.BurstTime * (config.StandardBudgetMultiplier / 100.),
              config.BoostRate / 100.,
              config.BurstTime * (config.BoostBudgetMultiplier / 100.),
              config.BurstTime * (config.BoostRefillBudgetMultiplier / 100.),
              config.BurstTime * (config.BoostBudgetMultiplier / 100.)))
{}

TVolumeShapingThrottler::~TVolumeShapingThrottler() = default;

TDuration TVolumeShapingThrottler::SuggestDelay(
    TInstant ts,
    TDuration requestCost)
{
    if (requestCost <= TDuration::Zero()) {
        return TDuration::Zero();
    }
    return Bucket->Register(ts, requestCost);
}

void TVolumeShapingThrottler::Reset(const TShapingThrottlerConfig& config)
{
    Bucket = std::make_unique<TBoostedTimeBucket>(
        config.BurstTime * (config.StandardBudgetMultiplier / 100.),
        config.BoostRate / 100.,
        config.BurstTime * (config.BoostBudgetMultiplier / 100.),
        config.BurstTime * (config.BoostRefillBudgetMultiplier / 100.),
        GetCurrentBoostBudget());
}

TDuration TVolumeShapingThrottler::GetCurrentBoostBudget() const
{
    return Bucket->GetCurrentBoostBudget();
}

}   // namespace NCloud::NBlockStore::NStorage
