#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/public.h>

#include <cloud/storage/core/libs/throttling/tablet_throttler_policy.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDuration CalculateBoostTime(const NProto::TVolumePerformanceProfile& config);

////////////////////////////////////////////////////////////////////////////////

struct TThrottlerConfig
{
    const TDuration MaxDelay;
    const ui32 MaxWriteCostMultiplier;
    const ui32 DefaultPostponedRequestWeight;
    const TDuration InitialBoostBudget;
    const bool UseDiskSpaceScore;

    TThrottlerConfig(
            TDuration maxDelay,
            ui32 maxWriteCostMultiplier,
            ui32 defaultPostponedRequestWeight,
            TDuration initialBoostBudget,
            bool useDiskSpaceScore)
        : MaxDelay(maxDelay)
        , MaxWriteCostMultiplier(maxWriteCostMultiplier)
        , DefaultPostponedRequestWeight(defaultPostponedRequestWeight)
        , InitialBoostBudget(initialBoostBudget)
        , UseDiskSpaceScore(useDiskSpaceScore)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeThrottlingPolicy final
    : public ITabletThrottlerPolicy
{
public:
    enum class EOpType
    {
        Read,
        Write,
        Zero,
        Describe,
        Last,
    };

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
    ui32 PolicyVersion = 0;

public:
    TVolumeThrottlingPolicy(
        const NProto::TVolumePerformanceProfile& config,
        const TThrottlerConfig& throttlerConfig);
    ~TVolumeThrottlingPolicy();

    void Reset(
        const NProto::TVolumePerformanceProfile& config,
        TDuration maxDelay,
        ui32 maxWriteCostMultiplier,
        ui32 defaultPostponedRequestWeight,
        TDuration initialBoostBudget,
        bool useDiskSpaceScore);
    void Reset(
        const NProto::TVolumePerformanceProfile& config,
        const TThrottlerConfig& throttlerConfig);
    void Reset(const TVolumeThrottlingPolicy& policy);

public:
    bool TryPostpone(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) override;
    TMaybe<TDuration> SuggestDelay(
        TInstant ts,
        TDuration queueTime,
        const TThrottlingRequestInfo& requestInfo) override;

    void OnPostponedEvent(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) override;

    void OnBackpressureReport(
        TInstant ts,
        const TBackpressureReport& report,
        ui32 partitionIdx);

    double GetWriteCostMultiplier() const;
    TDuration GetCurrentBoostBudget() const;
    ui32 CalculatePostponedWeight() const;
    double CalculateCurrentSpentBudgetShare(TInstant ts) const;
    [[nodiscard]] double GetUsedIoBudget() const;
    [[nodiscard]] double GetUsedBandwidthBudget() const;
    void FlushSpentIoBudget() const;
    void FlushSpentBandwidthBudget() const;
    const TBackpressureReport& GetCurrentBackpressure() const;
    const NProto::TVolumePerformanceProfile& GetConfig() const;

    ui32 GetVersion() const
    {
        return PolicyVersion;
    }

    // the following funcs were made public to display the results on monpages
    ui32 C1(EOpType opType) const;
    ui32 C2(EOpType opType) const;
};

}   // namespace NCloud::NBlockStore::NStorage
