#include "throttler_metrics.h"

#include <cloud/blockstore/libs/diagnostics/quota_metrics.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/map.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerMetrics: public IThrottlerMetrics
{
private:
    // first - diskId
    // second - instanceId
    using TQuotaKey = std::pair<TString, TString>;

    struct TQuotaInfo
    {
        TUsedQuotaMetrics Metrics;
        TInstant MountTime;

        TQuotaInfo(NMonitoring::TDynamicCountersPtr counters, ITimerPtr timer);
    };

    const ITimerPtr Timer;
    const NMonitoring::TDynamicCountersPtr TotalGroup;
    const NMonitoring::TDynamicCountersPtr VolumeGroup;

    TRWMutex Lock;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxCalc;
    TQuotaInfo Total;
    TMap<TQuotaKey, TQuotaInfo> QuotaContainer;

public:
    TThrottlerMetrics(
        ITimerPtr timer,
        NMonitoring::TDynamicCountersPtr totalGroup,
        NMonitoring::TDynamicCountersPtr volumeGroup);

    void Register(const TString& diskId, const TString& instanceId) override;
    void Unregister(const TString& diskId, const TString& instanceId) override;

    void Trim(TInstant now) override;
    void UpdateUsedQuota(ui64 quota) override;
    void UpdateMaxUsedQuota() override;

private:
    void UnregisterAll();
};

////////////////////////////////////////////////////////////////////////////////

TThrottlerMetrics::TQuotaInfo::TQuotaInfo(
    NMonitoring::TDynamicCountersPtr counters,
    ITimerPtr timer)
    : Metrics(std::move(timer))
{
    Metrics.Register(counters);
}

TThrottlerMetrics::TThrottlerMetrics(
    ITimerPtr timer,
    NMonitoring::TDynamicCountersPtr totalGroup,
    NMonitoring::TDynamicCountersPtr volumeGroup)
    : Timer(std::move(timer))
    , TotalGroup(std::move(totalGroup))
    , VolumeGroup(std::move(volumeGroup))
    , MaxCalc(Timer)
    , Total(TotalGroup, Timer)
{}

void TThrottlerMetrics::Register(
    const TString& diskId,
    const TString& instanceId)
{
    TWriteGuard guard(Lock);

    TQuotaKey key{diskId, instanceId};
    const bool needToRegister = QuotaContainer.empty();
    auto [quotaItr, isInserted] = QuotaContainer.try_emplace(
        key,
        VolumeGroup->GetSubgroup("volume", key.first)
            ->GetSubgroup("instance", key.second),
        Timer);

    if (needToRegister && isInserted) {
        Total.Metrics.Register(TotalGroup);
    }

    quotaItr->second.MountTime = Timer->Now();
    Total.MountTime = Timer->Now();
}

void TThrottlerMetrics::Unregister(
    const TString& diskId,
    const TString& instanceId)
{
    TWriteGuard guard(Lock);

    TQuotaKey key{diskId, instanceId};
    auto it = QuotaContainer.find(key);
    if (it == QuotaContainer.end()) {
        return;
    }

    it->second.Metrics.Unregister();
    QuotaContainer.erase(it);

    if (QuotaContainer.empty()) {
        UnregisterAll();
    }
}

void TThrottlerMetrics::Trim(TInstant now)
{
    TWriteGuard guard(Lock);

    std::erase_if(
        QuotaContainer,
        [now](auto& kv)
        {
            auto& quotaInfo = kv.second;

            if (now - quotaInfo.MountTime >= TRIM_THROTTLER_METRICS_INTERVAL) {
                quotaInfo.Metrics.Unregister();
                return true;
            }

            return false;
        });

    if (QuotaContainer.empty()) {
        UnregisterAll();
    }
}

void TThrottlerMetrics::UpdateUsedQuota(ui64 quota)
{
    MaxCalc.Add(quota);
    Total.Metrics.UpdateQuota(quota);

    TReadGuard guard(Lock);

    for (auto& [_, quotaInfo]: QuotaContainer) {
        quotaInfo.Metrics.UpdateQuota(quota);
    }
}

void TThrottlerMetrics::UpdateMaxUsedQuota()
{
    const auto value = MaxCalc.NextValue();
    Total.Metrics.UpdateMaxQuota(value);

    TReadGuard guard(Lock);

    for (auto& [_, quotaInfo]: QuotaContainer) {
        quotaInfo.Metrics.UpdateMaxQuota(value);
    }
}

void TThrottlerMetrics::UnregisterAll()
{
    Total.Metrics.Unregister();
    Total.MountTime = TInstant::Zero();
    for (ui32 i = 0; i < DEFAULT_BUCKET_COUNT; ++i) {
        MaxCalc.Add(0);
        MaxCalc.NextValue();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TThrottlerMetricsStub final: public IThrottlerMetrics
{
public:
    void Register(const TString& diskId, const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void Unregister(const TString& diskId, const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void Trim(TInstant now) override
    {
        Y_UNUSED(now);
    }

    void UpdateUsedQuota(ui64 quota) override
    {
        Y_UNUSED(quota);
    }

    void UpdateMaxUsedQuota() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerMetricsPtr CreateThrottlerMetricsStub()
{
    return std::make_shared<TThrottlerMetricsStub>();
}

IThrottlerMetricsPtr CreateThrottlerMetrics(
    ITimerPtr timer,
    NMonitoring::TDynamicCountersPtr rootGroup,
    const TString& componentLabel)
{
    return std::make_shared<TThrottlerMetrics>(
        std::move(timer),
        rootGroup->GetSubgroup("component", componentLabel),
        rootGroup->GetSubgroup("component", componentLabel + "_volume")
            ->GetSubgroup("host", "cluster"));
}

}   // namespace NCloud::NBlockStore
