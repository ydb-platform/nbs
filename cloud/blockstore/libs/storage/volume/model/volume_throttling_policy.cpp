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
    // Recalculated values are slightly larger and are used to improve the user
    // experience by delivering the exact maximum bandwidth and IOPS quotas.
    struct TMaxQuota
    {
        ui64 Bandwidth = 0;
        ui64 Iops = 0;
        ui64 RecalculatedBandwidth = 0;
        ui64 RecalculatedIops = 0;
    };

    using TMaxQuotas =
        std::array<TMaxQuota, static_cast<size_t>(EOpType::Last) + 1>;

    const NProto::TVolumeThrottlingRule ThrottlingRule;
    const NProto::TVolumePerformanceProfile OriginalConfig;
    const NProto::TVolumePerformanceProfile Config;
    const ui32 PolicyVersion;
    const ui32 VolatileVersion;
    const TDuration MaxDelay;
    const ui32 MaxWriteCostMultiplier;
    const ui64 DefaultPostponedRequestWeight;
    const bool UseDiskSpaceScore;
    const TMaxQuotas MaxQuotas;
    TBoostedTimeBucket Bucket;
    TVector<TBackpressureReport> PartitionBackpressures;
    TBackpressureReport CurrentBackpressure;
    double WriteCostMultiplier = 1;
    ui64 PostponedWeight = 0;

    double UsedIopsQuota = 0;
    double UsedBandwidthQuota = 0;

    TImpl(
            NProto::TVolumePerformanceProfile config,
            NProto::TVolumeThrottlingRule throttlingRule,
            const ui32 policyVersion,
            const ui32 volatileVersion,
            const TDuration maxDelay,
            const ui32 maxWriteCostMultiplier,
            const ui64 defaultPostponedRequestWeight,
            const TDuration initialBoostBudget,
            const bool useDiskSpaceScore)
        : ThrottlingRule(std::move(throttlingRule))
        , OriginalConfig(std::move(config))
        , Config(CalculateProfile(ThrottlingRule))
        , PolicyVersion(policyVersion)
        , VolatileVersion(volatileVersion)
        , MaxDelay(maxDelay)
        , MaxWriteCostMultiplier(maxWriteCostMultiplier)
        , DefaultPostponedRequestWeight(defaultPostponedRequestWeight)
        , UseDiskSpaceScore(useDiskSpaceScore)
        , MaxQuotas(CalculateMaxQuotas())
        , Bucket(
              CalcBurstTime(),
              CalculateBoostRate(Config),
              CalculateBoostTime(Config),
              CalculateBoostRefillTime(Config),
              initialBoostBudget)
    {}

    NProto::TVolumePerformanceProfile CalculateProfile(
        const NProto::TVolumeThrottlingRule& throttlingRule) const
    {
        NProto::TVolumePerformanceProfile result = OriginalConfig;

        const auto& coefficients = throttlingRule.GetCoefficients();
        // Volatile throttling can override ThrottlingEnabled
        if (coefficients.HasThrottlingEnabled()) {
            result.SetThrottlingEnabled(coefficients.GetThrottlingEnabled());
        }
        // Volatile throttling can multiply PerformanceProfile by coefficients
        if (coefficients.HasMaxReadBandwidth()) {
            result.SetMaxReadBandwidth(
                OriginalConfig.GetMaxReadBandwidth() *
                coefficients.GetMaxReadBandwidth());
        }
        if (coefficients.HasMaxPostponedWeight()) {
            result.SetMaxPostponedWeight(
                OriginalConfig.GetMaxPostponedWeight() *
                coefficients.GetMaxPostponedWeight());
        }
        if (coefficients.HasMaxReadIops()) {
            result.SetMaxReadIops(
                OriginalConfig.GetMaxReadIops() *
                coefficients.GetMaxReadIops());
        }
        if (coefficients.HasBoostTime()) {
            result.SetBoostTime(
                OriginalConfig.GetBoostTime() * coefficients.GetBoostTime());
        }
        if (coefficients.HasBoostRefillTime()) {
            result.SetBoostRefillTime(
                OriginalConfig.GetBoostRefillTime() *
                coefficients.GetBoostRefillTime());
        }
        if (coefficients.HasBoostPercentage()) {
            result.SetBoostPercentage(
                OriginalConfig.GetBoostPercentage() *
                coefficients.GetBoostPercentage());
        }
        if (coefficients.HasMaxWriteBandwidth()) {
            result.SetMaxWriteBandwidth(
                OriginalConfig.GetMaxWriteBandwidth() *
                coefficients.GetMaxWriteBandwidth());
        }
        if (coefficients.HasMaxWriteIops()) {
            result.SetMaxWriteIops(
                OriginalConfig.GetMaxWriteIops() *
                coefficients.GetMaxWriteIops());
        }
        if (coefficients.HasBurstPercentage()) {
            result.SetBurstPercentage(
                OriginalConfig.GetBurstPercentage() *
                coefficients.GetBurstPercentage());
        }

        return result;
    }

    TMaxQuotas CalculateMaxQuotas() const
    {
        TMaxQuotas result;
        for (size_t i = 0; i <= static_cast<size_t>(EOpType::Last); ++i) {
            const ui64 bandwidth =
                CalculateMaxBandwidth(static_cast<EOpType>(i));
            const ui64 iops = CalculateMaxIops(static_cast<EOpType>(i));
            result[i] = TMaxQuota{
                .Bandwidth = bandwidth,
                .Iops = iops,
                .RecalculatedBandwidth = CalculateThrottlerC2(iops, bandwidth),
                .RecalculatedIops = CalculateThrottlerC1(iops, bandwidth)};
        }
        return result;
    }

    ui64 CalculateMaxBandwidth(EOpType opType) const
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

    ui32 CalculateMaxIops(EOpType opType) const
    {
        if (opType == EOpType::Write && Config.GetMaxWriteIops()) {
            return Config.GetMaxWriteIops();
        }

        return Config.GetMaxReadIops();
    }

    const NProto::TVolumePerformanceProfile& CurrentProfile() const
    {
        return Config;
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

    bool TryPostpone(TInstant ts, ui64 weight)
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

    ui64 GetMaxIops(EOpType opType) const
    {
        Y_ABORT_UNLESS(opType <= EOpType::Last);
        return MaxQuotas[static_cast<size_t>(opType)].Iops;
    }

    ui64 GetRecalculatedMaxIops(EOpType opType) const
    {
        Y_ABORT_UNLESS(opType <= EOpType::Last);
        return MaxQuotas[static_cast<size_t>(opType)].RecalculatedIops;
    }

    ui64 GetMaxBandwidth(EOpType opType) const
    {
        Y_ABORT_UNLESS(opType <= EOpType::Last);
        return MaxQuotas[static_cast<size_t>(opType)].Bandwidth;
    }

    ui64 GetRecalculatedMaxBandwidth(EOpType opType) const
    {
        Y_ABORT_UNLESS(opType <= EOpType::Last);
        return MaxQuotas[static_cast<size_t>(opType)].RecalculatedBandwidth;
    }

    double GetMultiplier(EOpType opType) const
    {
        return opType == EOpType::Read ? 1.0 : WriteCostMultiplier;
    }

    [[nodiscard]] TDuration GetRequestCost(
        const TThrottlingRequestInfo& requestInfo) const
    {
        const auto opType = static_cast<EOpType>(requestInfo.OpType);
        const double multiplier = GetMultiplier(opType);
        const ui64 recalculatedMaxIops = GetRecalculatedMaxIops(opType);
        const ui64 recalculatedMaxBandwidth =
            GetRecalculatedMaxBandwidth(opType);

        return multiplier * CostPerIO(
                                recalculatedMaxIops,
                                recalculatedMaxBandwidth,
                                requestInfo.ByteCount);
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

        auto d = Bucket.Register(ts, GetRequestCost(requestInfo));
        if (!d.GetValue()) {
            // 0 is special value which disables throttling by byteCount
            const auto opType = static_cast<EOpType>(requestInfo.OpType);
            const double multiplier = GetMultiplier(opType);
            if (ui64 recalculatedMaxBandwidth =
                    GetRecalculatedMaxBandwidth(opType))
            {
                UsedBandwidthQuota +=
                    multiplier *
                    (static_cast<double>(requestInfo.ByteCount) /
                     static_cast<double>(recalculatedMaxBandwidth));
            }
            UsedIopsQuota +=
                multiplier * (1.0 / GetRecalculatedMaxIops(opType));
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

    ui64 PostponedRequestWeight(EOpType opType, ui64 byteCount) const
    {
        return opType == EOpType::Write
            ? byteCount
            : DefaultPostponedRequestWeight;
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const
    {
        return Bucket.CalculateCurrentSpentBudgetShare(ts);
    }

    TSplittedUsedQuota TakeSplittedUsedQuota()
    {
        auto result = TSplittedUsedQuota(UsedIopsQuota, UsedBandwidthQuota);
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

TVolumeThrottlingPolicy::~TVolumeThrottlingPolicy() = default;

void TVolumeThrottlingPolicy::Reset(
    const NProto::TVolumePerformanceProfile& config,
    const NProto::TVolumeThrottlingRule& throttlingRule,
    ui32 coefficientsVersion,
    TDuration maxDelay,
    ui32 maxWriteCostMultiplier,
    ui32 defaultPostponedRequestWeight,
    TDuration initialBoostBudget,
    bool useDiskSpaceScore)
{
    Impl = std::make_unique<TImpl>(
        config,
        throttlingRule,
        ++PolicyVersion,
        coefficientsVersion,
        maxDelay,
        maxWriteCostMultiplier,
        defaultPostponedRequestWeight,
        initialBoostBudget,
        useDiskSpaceScore);
}

void TVolumeThrottlingPolicy::Reset(
    const NProto::TVolumePerformanceProfile& config,
    const TThrottlerConfig& throttlerConfig)
{
    auto coefficients = Impl ? Impl->ThrottlingRule
                             : NProto::TVolumeThrottlingRule{};
    auto volatileVersion = Impl ? Impl->VolatileVersion : 0;
    Reset(
        config,
        coefficients,
        volatileVersion,
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
        policy.Impl->OriginalConfig,
        policy.Impl->ThrottlingRule,
        policy.Impl->VolatileVersion,
        policy.Impl->MaxDelay,
        policy.Impl->MaxWriteCostMultiplier,
        policy.Impl->DefaultPostponedRequestWeight,
        policy.Impl->Bucket.GetCurrentBoostBudget(),
        policy.Impl->UseDiskSpaceScore);
}

void TVolumeThrottlingPolicy::Reset(
    const NProto::TVolumeThrottlingRule& throttlingRule,
    ui32 volatileVersion)
{
    Reset(
        Impl->OriginalConfig,
        throttlingRule,
        volatileVersion,
        Impl->MaxDelay,
        Impl->MaxWriteCostMultiplier,
        Impl->DefaultPostponedRequestWeight,
        Impl->Bucket.GetCurrentBoostBudget(),
        Impl->UseDiskSpaceScore);
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

TDuration TVolumeThrottlingPolicy::GetRequestCost(
    const TThrottlingRequestInfo& requestInfo) const
{
    return Impl->GetRequestCost(requestInfo);
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

ui64 TVolumeThrottlingPolicy::CalculatePostponedWeight() const
{
    return Impl->PostponedWeight;
}

double TVolumeThrottlingPolicy::CalculateCurrentSpentBudgetShare(TInstant ts) const
{
    return Impl->CalculateCurrentSpentBudgetShare(ts);
}

TVolumeThrottlingPolicy::TSplittedUsedQuota
TVolumeThrottlingPolicy::TakeSplittedUsedQuota()
{
    return Impl->TakeSplittedUsedQuota();
}

const TBackpressureReport& TVolumeThrottlingPolicy::GetCurrentBackpressure() const
{
    return Impl->CurrentBackpressure;
}

const NProto::TVolumePerformanceProfile& TVolumeThrottlingPolicy::GetConfig() const
{
    return Impl->Config;
}

ui32 TVolumeThrottlingPolicy::GetVolatileThrottlingVersion() const
{
    return Impl->VolatileVersion;
}

const NProto::TVolumeThrottlingRule& TVolumeThrottlingPolicy::GetVolatileThrottlingRule() const {
    return Impl->ThrottlingRule;
}

NProto::TVolumePerformanceProfile TVolumeThrottlingPolicy::GetCurrentPerformanceProfile() const {
    return Impl->CurrentProfile();
}

ui64 TVolumeThrottlingPolicy::C1(EOpType opType) const
{
    return Impl->GetRecalculatedMaxIops(opType);
}

ui64 TVolumeThrottlingPolicy::C2(EOpType opType) const
{
    return Impl->GetRecalculatedMaxBandwidth(opType);
}

}   // namespace NCloud::NBlockStore::NStorage
