#include "throttler_policy.h"

#include <cloud/blockstore/libs/throttling/throttler_policy.h>

#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerPolicy final
    : public IThrottlerPolicy
{
private:
    TThrottlingServiceConfig Config;
    const double BurstRate;
    TLeakyBucket Bucket;

    THashMap<NProto::EStorageMediaKind, TDuration> UsedQuota;

public:
    TThrottlerPolicy(const TThrottlingServiceConfig& config)
        : Config(config)
        , BurstRate(Config.MaxBurstTime.MicroSeconds() / 1e6)
        , Bucket(1.0, BurstRate, BurstRate)
    {}

    TDuration SuggestDelay(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        size_t byteCount) override
    {
        Y_UNUSED(mediaKind);

        if (!byteCount) {
            return TDuration::Zero();
        }

        ui64 maxBandwidth = 0;
        ui32 maxIops = 0;

        switch (requestType) {
            case EBlockStoreRequest::ReadBlocks:
            case EBlockStoreRequest::ReadBlocksLocal: {
                maxBandwidth = Config.MaxReadBandwidth;
                maxIops = Config.MaxReadIops;

                break;
            }

            default: {
                maxBandwidth = Config.MaxWriteBandwidth;
                maxIops = Config.MaxWriteIops;

                break;
            }
        }

        if (!maxIops) {
            return TDuration::Zero();
        }

        TDuration update = CostPerIO(maxIops, maxBandwidth, byteCount);

        double delay = Bucket.Register(now, update.MicroSeconds() / 1e6);
        if (delay == 0) {
            UsedQuota[mediaKind] += update;
        }
        return SecondsToDuration(delay);
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const override
    {
        return Bucket.CalculateCurrentSpentBudgetShare(ts);
    }

    TUsedQuota TakeUsedQuota() override
    {
        TUsedQuota result(UsedQuota, BurstRate);
        UsedQuota.clear();
        return result;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerPolicyPtr CreateServiceThrottlerPolicy(
    const TThrottlingServiceConfig& config)
{
    return std::make_shared<TThrottlerPolicy>(config);
}

}   // namespace NCloud::NBlockStore
