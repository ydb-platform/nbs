#include "volume_throttling_policy.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/throttling/helpers.h>
#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////
// IOPS throttle
//
// Read ops are limited based solely on the requested MaxRead{Bandwidth,Iops}.
// Write ops are limited based on some 'Target{Bandwidth,Iops}', which equals
// MaxWrite{Bandwidth,Iops} when the partition's health is fine and is
// gradually decreased if the partition's not feeling OK until it actually starts
// feeling OK

double CalculateWriteCostMultiplier(const TBackpressureReport& lastReport)
{
    const auto features = {
        lastReport.FreshIndexScore,
        lastReport.CompactionScore,
        lastReport.DiskSpaceScore,
        lastReport.CleanupScore,
    };

    double x = 1;
    for (const auto f: features) {
        x = Max(x, f);
    }

    return x;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

double CalculateBoostRate(const NProto::TVolumePerformanceProfile& config)
{
    return config.GetBoostPercentage() / 100.;
}

TDuration CalculateBoostTime(const NProto::TVolumePerformanceProfile& config)
{
    const auto rate = CalculateBoostRate(config);
    if (rate <= 1.) {
        return TDuration::MilliSeconds(0);
    }

    return TDuration::MilliSeconds(
        static_cast<ui64>((rate - 1.) * config.GetBoostTime()));
}

TDuration CalculateBoostRefillTime(const NProto::TVolumePerformanceProfile& config)
{
    return TDuration::MilliSeconds(config.GetBoostRefillTime());
}

////////////////////////////////////////////////////////////////////////////////

struct TVolumeThrottlingPolicy::TImpl
{
    const NProto::TVolumePerformanceProfile Config;
    const ui32 PolicyVersion;
    const TDuration MaxDelay;
    const ui32 MaxWriteCostMultiplier;
    const ui32 DefaultPostponedRequestWeight;
    const bool UseDiskSpaceScore;
    TBoostedTimeBucket Bucket;
    TVector<TBackpressureReport> PartitionBackpressures;
    TBackpressureReport CurrentBackpressure;
    double WriteCostMultiplier = 1;
    ui32 PostponedWeight = 0;

    double UsedIopsQuota = 0;
    double UsedBandwidthQuota = 0;

    TImpl(
            const NProto::TVolumePerformanceProfile& config,
            const ui32 policyVersion,
            const TDuration maxDelay,
            const ui32 maxWriteCostMultiplier,
            const ui32 defaultPostponedRequestWeight,
            const TDuration initialBoostBudget,
            const bool useDiskSpaceScore)
        : Config(config)
        , PolicyVersion(policyVersion)
        , MaxDelay(maxDelay)
        , MaxWriteCostMultiplier(maxWriteCostMultiplier)
        , DefaultPostponedRequestWeight(defaultPostponedRequestWeight)
        , UseDiskSpaceScore(useDiskSpaceScore)
        , Bucket(
            CalcBurstTime(),
            CalculateBoostRate(Config),
            CalculateBoostTime(Config),
            CalculateBoostRefillTime(Config),
            initialBoostBudget
        )
    {
    }

    TDuration CalcBurstTime() const
    {
        return SecondsToDuration(
            (Config.GetBurstPercentage() ? Config.GetBurstPercentage() : 10)
            / 100.);
    }

    void OnBackpressureReport(
        TInstant ts,
        const TBackpressureReport& report,
        ui32 partitionIdx)
    {
        Y_UNUSED(ts);
        Y_ABORT_UNLESS(partitionIdx < 256);

        if (PartitionBackpressures.size() <= partitionIdx) {
            PartitionBackpressures.resize(partitionIdx + 1);
        }

        PartitionBackpressures[partitionIdx] = report;

        CurrentBackpressure = {};
        for (const auto& report: PartitionBackpressures) {
            if (CurrentBackpressure.CompactionScore < report.CompactionScore) {
                CurrentBackpressure.CompactionScore = report.CompactionScore;
            }

            if (CurrentBackpressure.DiskSpaceScore < report.DiskSpaceScore) {
                CurrentBackpressure.DiskSpaceScore = report.DiskSpaceScore;
            }

            if (CurrentBackpressure.FreshIndexScore < report.FreshIndexScore) {
                CurrentBackpressure.FreshIndexScore = report.FreshIndexScore;
            }

            if (CurrentBackpressure.CleanupScore < report.CleanupScore) {
                CurrentBackpressure.CleanupScore = report.CleanupScore;
            }
        }

        auto bp = CurrentBackpressure;
        if (!UseDiskSpaceScore) {
            bp.DiskSpaceScore = 0;
        }
        WriteCostMultiplier = Min(
            CalculateWriteCostMultiplier(bp),
            double(Max(MaxWriteCostMultiplier, 1u))
        );
    }

    void OnPostponedEvent(TInstant ts, const TThrottlingRequestInfo& requestInfo)
    {
        Y_UNUSED(ts);

        if (requestInfo.PolicyVersion < PolicyVersion) {
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

    bool TryPostpone(TInstant ts, ui32 weight)
    {
        Y_UNUSED(ts);

        const auto newWeight = PostponedWeight + weight;
        if (newWeight <= Config.GetMaxPostponedWeight()) {
            PostponedWeight = newWeight;
            return true;
        }

        return false;
    }

    bool TryPostpone(TInstant ts, const TThrottlingRequestInfo& requestInfo)
    {
        return TryPostpone(
            ts,
            PostponedRequestWeight(
                static_cast<EOpType>(requestInfo.OpType),
                requestInfo.ByteCount)
        );
    }

    ui32 MaxIops(EOpType opType) const
    {
        if (opType == EOpType::Write && Config.GetMaxWriteIops()) {
            return Config.GetMaxWriteIops();
        }

        return Config.GetMaxReadIops();
    }

    ui32 MaxBandwidth(EOpType opType) const
    {
        if (opType == EOpType::Write && Config.GetMaxWriteBandwidth()) {
            return Config.GetMaxWriteBandwidth();
        }

        if (opType == EOpType::Describe) {
            // Disabling throttling by bandwidth for DescribeBlocks requests -
            // they will be throttled only by iops
            // See NBS-2733
            return 0;
        }

        return Config.GetMaxReadBandwidth();
    }

    TMaybe<TDuration> SuggestDelay(
        TInstant ts,
        TDuration queueTime,
        const TThrottlingRequestInfo& requestInfo)
    {
        if (requestInfo.PolicyVersion < PolicyVersion) {
            // could be VERIFY_DEBUG, but it's easier to test it this way
            // requests with old versions are expected only in OnPostponedEvent
            return TMaybe<TDuration>();
        }

        if (!requestInfo.ByteCount) {
            return TDuration::Zero();
        }

        const ui32 bandwidthUpdate = requestInfo.ByteCount;
        double m = static_cast<EOpType>(requestInfo.OpType) == EOpType::Read
            ? 1.0
            : WriteCostMultiplier;

        const auto maxBandwidth =
            MaxBandwidth(static_cast<EOpType>(requestInfo.OpType));
        const auto maxIops = MaxIops(static_cast<EOpType>(requestInfo.OpType));

        const auto recalculatedMaxIops =
            CalculateThrottlerC1(maxIops, maxBandwidth);
        const auto recalculatedMaxBandwidth =
            CalculateThrottlerC2(maxIops, maxBandwidth);
        auto d = Bucket.Register(
            ts,
            m * CostPerIO(
                    recalculatedMaxIops,
                    recalculatedMaxBandwidth,
                    bandwidthUpdate));

        if (!d.GetValue()) {
            // 0 is special value which disables throttling by byteCount
            if (maxBandwidth) {
                UsedBandwidthQuota +=
                    m * (static_cast<double>(bandwidthUpdate) /
                         static_cast<double>(recalculatedMaxBandwidth));
            }
            UsedIopsQuota += m * (1.0 / recalculatedMaxIops);
            return TDuration::Zero();
        }

        if (d + queueTime > MaxDelay) {
            return TMaybe<TDuration>();
        }

        const auto postponed = TryPostpone(
            ts,
            PostponedRequestWeight(
                static_cast<EOpType>(requestInfo.OpType),
                requestInfo.ByteCount)
        );

        return postponed ? d : TMaybe<TDuration>();
    }

    ui32 PostponedRequestWeight(EOpType opType, ui32 byteCount) const
    {
        return opType == EOpType::Write
            ? byteCount
            : DefaultPostponedRequestWeight;
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const
    {
        return Bucket.CalculateCurrentSpentBudgetShare(ts);
    }

    std::pair<double, double> TakeUsedQuota()
    {
        auto result = std::make_pair(UsedIopsQuota, UsedBandwidthQuota);
        UsedIopsQuota = 0;
        UsedBandwidthQuota = 0;

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

TVolumeThrottlingPolicy::TVolumeThrottlingPolicy(
    const NProto::TVolumePerformanceProfile& config,
    const TThrottlerConfig& throttlerConfig)
{
    Reset(config, throttlerConfig);
}

TVolumeThrottlingPolicy::~TVolumeThrottlingPolicy()
{}

void TVolumeThrottlingPolicy::Reset(
    const NProto::TVolumePerformanceProfile& config,
    TDuration maxDelay,
    ui32 maxWriteCostMultiplier,
    ui32 defaultPostponedRequestWeight,
    TDuration initialBoostBudget,
    bool useDiskSpaceScore)
{
    Impl.reset(new TImpl(
        config,
        ++PolicyVersion,
        maxDelay,
        maxWriteCostMultiplier,
        defaultPostponedRequestWeight,
        initialBoostBudget,
        useDiskSpaceScore));
}

void TVolumeThrottlingPolicy::Reset(
    const NProto::TVolumePerformanceProfile& config,
    const TThrottlerConfig& throttlerConfig)
{
    Reset(
        config,
        throttlerConfig.MaxDelay,
        throttlerConfig.MaxWriteCostMultiplier,
        throttlerConfig.DefaultPostponedRequestWeight,
        throttlerConfig.InitialBoostBudget,
        throttlerConfig.UseDiskSpaceScore);
}

void TVolumeThrottlingPolicy::Reset(
    const TVolumeThrottlingPolicy& policy)
{
    Reset(
        policy.Impl->Config,
        policy.Impl->MaxDelay,
        policy.Impl->MaxWriteCostMultiplier,
        policy.Impl->DefaultPostponedRequestWeight,
        policy.Impl->Bucket.GetCurrentBoostBudget(),
        policy.Impl->UseDiskSpaceScore);
}

void TVolumeThrottlingPolicy::OnBackpressureReport(
    TInstant ts,
    const TBackpressureReport& report,
    ui32 partitionIdx)
{
    Impl->OnBackpressureReport(ts, report, partitionIdx);
}

void TVolumeThrottlingPolicy::OnPostponedEvent(
    TInstant ts,
    const TThrottlingRequestInfo& requestInfo)
{
    Impl->OnPostponedEvent(ts, requestInfo);
}

bool TVolumeThrottlingPolicy::TryPostpone(
    TInstant ts,
    const TThrottlingRequestInfo& requestInfo)
{
    return Impl->TryPostpone(ts, requestInfo);
}

TMaybe<TDuration> TVolumeThrottlingPolicy::SuggestDelay(
    TInstant ts,
    TDuration queueTime,
    const TThrottlingRequestInfo& requestInfo)
{
    return Impl->SuggestDelay(ts, queueTime, requestInfo);
}

double TVolumeThrottlingPolicy::GetWriteCostMultiplier() const
{
    return Impl->WriteCostMultiplier;
}

TDuration TVolumeThrottlingPolicy::GetCurrentBoostBudget() const
{
    return Impl->Bucket.GetCurrentBoostBudget();
}

ui32 TVolumeThrottlingPolicy::CalculatePostponedWeight() const
{
    return Impl->PostponedWeight;
}

double TVolumeThrottlingPolicy::CalculateCurrentSpentBudgetShare(TInstant ts) const
{
    return Impl->CalculateCurrentSpentBudgetShare(ts);
}

std::pair<double, double> TVolumeThrottlingPolicy::TakeUsedQuota()
{
    return Impl->TakeUsedQuota();
}

const TBackpressureReport& TVolumeThrottlingPolicy::GetCurrentBackpressure() const
{
    return Impl->CurrentBackpressure;
}

const NProto::TVolumePerformanceProfile& TVolumeThrottlingPolicy::GetConfig() const
{
    return Impl->Config;
}

ui32 TVolumeThrottlingPolicy::C1(EOpType opType) const
{
    return CalculateThrottlerC1(Impl->MaxIops(opType), Impl->MaxBandwidth(opType));
}

ui32 TVolumeThrottlingPolicy::C2(EOpType opType) const
{
    return CalculateThrottlerC2(Impl->MaxIops(opType), Impl->MaxBandwidth(opType));
}

}   // namespace NCloud::NBlockStore::NStorage
