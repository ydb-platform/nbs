#include "throttling_policy.h"

#include <cloud/storage/core/libs/throttling/helpers.h>
#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/generic/ymath.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TDuration CalculateBurstTime(const TThrottlerConfig& config)
{
    return SecondsToDuration(
        (config.BurstPercentage
             ? static_cast<double>(config.BurstPercentage)
             : 10.0)
        / 100.0);
}

////////////////////////////////////////////////////////////////////////////////

double CalculateBoostRate(const TThrottlerConfig& config)
{
    return static_cast<double>(config.BoostParameters.BoostPercentage) / 100.0;
}

TDuration CalculateBoostTime(const TThrottlerConfig& config)
{
    const auto rate = CalculateBoostRate(config);
    if (rate <= 1.0) {
        return TDuration::MilliSeconds(0);
    }

    return TDuration::MilliSeconds(
        static_cast<ui64>((rate - 1.0)
            * config.BoostParameters.BoostTime.MilliSeconds()));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TThrottlingPolicy::TImpl
{
    const ui32 Version = 0;
    const TThrottlerConfig Config;

    TBoostedTimeBucket Bucket;

    ui32 PostponedWeight = 0;
    double WriteCostMultiplier = 1.0;

    TImpl(const TThrottlerConfig& config, ui32 version)
        : Version(version)
        , Config(config)
        , Bucket(
            CalculateBurstTime(Config),
            CalculateBoostRate(Config),
            CalculateBoostTime(Config),
            Config.BoostParameters.BoostRefillTime,
            CalculateBoostTime(Config))
    {}

    void OnPostponedEvent(const TThrottlingRequestInfo& requestInfo)
    {
        if (requestInfo.PolicyVersion < Version) {
            return;
        }

        const auto weight = PostponedRequestWeight(
            static_cast<EOpType>(requestInfo.OpType),
            requestInfo.ByteCount);

        if (PostponedWeight < weight) {
            Y_DEBUG_ABORT_UNLESS(false);
            PostponedWeight = 0;
        } else {
            PostponedWeight -= weight;
        }
    }

    bool TryPostpone(const TThrottlingRequestInfo& requestInfo)
    {
        return TryPostpone(
            PostponedRequestWeight(
                static_cast<EOpType>(requestInfo.OpType),
                requestInfo.ByteCount)
        );
    }

    TMaybe<TDuration> SuggestDelay(
        TInstant ts,
        TDuration queueTime,
        const TThrottlingRequestInfo& requestInfo)
    {
        if (requestInfo.PolicyVersion < Version) {
            return TMaybe<TDuration>();
        }

        if (!requestInfo.ByteCount) {
            return TDuration::Zero();
        }

        const auto bandwidthUpdate = requestInfo.ByteCount;
        const auto m = static_cast<EOpType>(requestInfo.OpType) == EOpType::Read
            ? 1.0
            : WriteCostMultiplier;

        const auto maxBandwidth =
            MaxBandwidth(static_cast<EOpType>(requestInfo.OpType));
        const auto maxIops = MaxIops(static_cast<EOpType>(requestInfo.OpType));

        const auto d = Bucket.Register(
            ts,
            m * CostPerIO(
                    CalculateThrottlerC1(maxIops, maxBandwidth),
                    CalculateThrottlerC2(maxIops, maxBandwidth),
                    bandwidthUpdate)
        );

        if (!d.GetValue()) {
            return TDuration::Zero();
        }

        if (d + queueTime > Config.DefaultThresholds.MaxPostponedTime) {
            return TMaybe<TDuration>();
        }

        const auto postponed = TryPostpone(
            PostponedRequestWeight(
                static_cast<EOpType>(requestInfo.OpType),
                requestInfo.ByteCount)
        );

        return postponed ? d : TMaybe<TDuration>();
    }

    ui32 MaxIops(EOpType opType) const
    {
        if (opType == EOpType::Write && Config.DefaultParameters.MaxWriteIops) {
            return Config.DefaultParameters.MaxWriteIops;
        }

        return Config.DefaultParameters.MaxReadIops;
    }

    ui64 MaxBandwidth(EOpType opType) const
    {
        if (opType == EOpType::Write &&
            Config.DefaultParameters.MaxWriteBandwidth) {
            return Config.DefaultParameters.MaxWriteBandwidth;
        }

        return Config.DefaultParameters.MaxReadBandwidth;
    }

    double CalculateCurrentSpentBudgetShare(TInstant now) const
    {
        return Bucket.CalculateCurrentSpentBudgetShare(now);
    }

private:
    bool TryPostpone(ui64 weight)
    {
        const auto newWeight = PostponedWeight + weight;
        if (newWeight <= Config.DefaultThresholds.MaxPostponedWeight) {
            PostponedWeight = newWeight;
            return true;
        }

        return false;
    }

    ui64 PostponedRequestWeight(EOpType opType, ui64 byteCount) const
    {
        return opType == EOpType::Write
            ? byteCount
            : Config.DefaultPostponedRequestWeight;
    }
};

////////////////////////////////////////////////////////////////////////////////

TThrottlingPolicy::TThrottlingPolicy(const TThrottlerConfig& config)
{
    Reset(config);
}

TThrottlingPolicy::~TThrottlingPolicy() = default;

void TThrottlingPolicy::Reset(const TThrottlerConfig& config)
{
    Impl = std::make_unique<TImpl>(config, ++Version);
}

bool TThrottlingPolicy::TryPostpone(
    TInstant ts,
    const TThrottlingRequestInfo& requestInfo)
{
    Y_UNUSED(ts);
    return Impl->TryPostpone(requestInfo);
}

TMaybe<TDuration> TThrottlingPolicy::SuggestDelay(
    TInstant ts,
    TDuration queueTime,
    const TThrottlingRequestInfo& requestInfo)
{
    return Impl->SuggestDelay(ts, queueTime, requestInfo);
}

void TThrottlingPolicy::OnPostponedEvent(
    TInstant ts,
    const TThrottlingRequestInfo& requestInfo)
{
    Y_UNUSED(ts);
    return Impl->OnPostponedEvent(requestInfo);
}

const TThrottlerConfig& TThrottlingPolicy::GetConfig() const
{
    return Impl->Config;
}

ui32 TThrottlingPolicy::GetVersion() const
{
    return Version;
}

ui32 TThrottlingPolicy::CalculatePostponedWeight() const
{
    return Impl->PostponedWeight;
}

double TThrottlingPolicy::GetWriteCostMultiplier() const
{
    return Impl->WriteCostMultiplier;
}

TDuration TThrottlingPolicy::GetCurrentBoostBudget() const
{
    return Impl->Bucket.GetCurrentBoostBudget();
}

double TThrottlingPolicy::CalculateCurrentSpentBudgetShare(TInstant now) const
{
    return Impl->CalculateCurrentSpentBudgetShare(now);
}

ui64 TThrottlingPolicy::C1(EOpType opType) const
{
    return CalculateThrottlerC1(
        Impl->MaxIops(opType),
        Impl->MaxBandwidth(opType));
}

ui64 TThrottlingPolicy::C2(EOpType opType) const
{
    return CalculateThrottlerC2(
        Impl->MaxIops(opType),
        Impl->MaxBandwidth(opType));
}

}   // namespace NCloud::NFileStore::NStorage
