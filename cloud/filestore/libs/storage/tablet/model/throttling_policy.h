#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/api/public.h>

#include <cloud/storage/core/libs/throttling/tablet_throttler_policy.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultParameters
{
    ui32 MaxReadIops = Max<ui32>();
    ui32 MaxWriteIops = Max<ui32>();

    // Use max of signed to match i64 metric counters
    ui64 MaxReadBandwidth = Max<i64>();
    ui64 MaxWriteBandwidth = Max<i64>();
};

struct TBoostParameters
{
    TDuration BoostTime = TDuration::Minutes(30);
    TDuration BoostRefillTime = TDuration::Hours(12);
    ui32 BoostPercentage = 10;
};

struct TDefaultThresholds
{
    ui64 MaxPostponedWeight = 128_MB;
    ui32 MaxPostponedCount = 512;
    TDuration MaxPostponedTime = TDuration::Seconds(25);
    double MaxWriteCostMultiplier = 10.0;
};

struct TThrottlerConfig
{
    bool ThrottlingEnabled = false;
    ui32 BurstPercentage = 10;
    ui32 DefaultPostponedRequestWeight = 1_KB;

    TDefaultParameters DefaultParameters;
    TBoostParameters BoostParameters;
    TDefaultThresholds DefaultThresholds;
};

////////////////////////////////////////////////////////////////////////////////

class TThrottlingPolicy final
    : public ITabletThrottlerPolicy
{
public:
    enum class EOpType
    {
        Read,
        Write,
    };

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

    ui32 Version = 0;

public:
    TThrottlingPolicy(const TThrottlerConfig& config);
    ~TThrottlingPolicy();

    void Reset(const TThrottlerConfig& config);

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

    const TThrottlerConfig& GetConfig() const;
    ui32 GetVersion() const;
    ui32 CalculatePostponedWeight() const;
    double GetWriteCostMultiplier() const;
    TDuration GetCurrentBoostBudget() const;
    double CalculateCurrentSpentBudgetShare(TInstant now) const;
    ui64 C1(EOpType opType) const;
    ui64 C2(EOpType opType) const;
};

using TThrottlingPolicyPtr = std::shared_ptr<TThrottlingPolicy>;

}   // namespace NCloud::NFileStore::NStorage
