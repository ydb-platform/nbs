#pragma once

#include "throttler_metrics.h"

#include <cloud/blockstore/libs/diagnostics/quota_metrics.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/map.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived, typename TQuotaKey>
class TThrottlerMetricsBase
    : public IThrottlerMetrics
{
private:
    struct TQuotaInfo
    {
        TAtomic Initialized = 0;
        TMutex Lock;

        NMonitoring::TDynamicCountersPtr Counters;
        TUsedQuotaMetrics Metrics;
        TInstant MountTime;

        explicit TQuotaInfo(ITimerPtr timer)
            : TQuotaInfo(nullptr, std::move(timer))
        {}

        TQuotaInfo(NMonitoring::TDynamicCountersPtr counters, ITimerPtr timer)
            : Counters(std::move(counters))
            , Metrics(std::move(timer))
        {}

        bool IsInitialized() const
        {
            return AtomicGet(Initialized);
        }

        void AddCounters(NMonitoring::TDynamicCountersPtr counters)
        {
            auto guard = TGuard(Lock);
            if (!IsInitialized()) {
                Counters = std::move(counters);
                AtomicSet(Initialized, 1);
            }
        }
    };

    const ITimerPtr Timer;
    const NMonitoring::TDynamicCountersPtr VolumeGroup;

    TRWMutex Lock;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxCalc;
    TQuotaInfo Total;
    TMap<TQuotaKey, TQuotaInfo> QuotaContainer;

public:
    TThrottlerMetricsBase(
            ITimerPtr timer,
            NMonitoring::TDynamicCountersPtr totalGroup,
            NMonitoring::TDynamicCountersPtr volumeGroup)
        : Timer(std::move(timer))
        , VolumeGroup(std::move(volumeGroup))
        , MaxCalc(Timer)
        , Total(std::move(totalGroup), Timer)
    {}

    void RegisterQuota(const TQuotaKey& key)
    {
        TWriteGuard guard(Lock);

        auto [it, inserted] = QuotaContainer.try_emplace(key, Timer);
        if (!inserted) {
            return;
        }

        it->second.MountTime = Timer->Now();
    }

    void UnregisterQuota(const TQuotaKey& key)
    {
        TWriteGuard guard(Lock);

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

    void Trim(TInstant now) override
    {
        TWriteGuard guard(Lock);

        std::erase_if(QuotaContainer, [now] (auto& kv) {
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

    void UpdateUsedQuota(ui64 quota) override
    {
        {
            TReadGuard guard(Lock);

            for (auto& [key, quotaInfo]: QuotaContainer) {
                if (!quotaInfo.IsInitialized()) {
                    auto [diskId, clientId] = static_cast<TDerived*>(this)
                        ->GetMetricsPath(key);

                    quotaInfo.AddCounters(VolumeGroup
                        ->GetSubgroup("volume", diskId)
                        ->GetSubgroup("instance", clientId));
                }
                quotaInfo.Metrics.Register(quotaInfo.Counters);
                quotaInfo.MountTime = Timer->Now();
            }

            if (!QuotaContainer.empty()) {
                Total.Metrics.Register(Total.Counters);
                Total.MountTime = Timer->Now();
            }
        }

        MaxCalc.Add(quota);
        Total.Metrics.UpdateQuota(quota);

        TReadGuard guard(Lock);
        for (auto& [_, quotaInfo]: QuotaContainer) {
            quotaInfo.Metrics.UpdateQuota(quota);
        }
    }

    void UpdateMaxUsedQuota() override
    {
        const auto value = MaxCalc.NextValue();
        Total.Metrics.UpdateMaxQuota(value);

        TReadGuard guard(Lock);
        for (auto& [_, quotaInfo]: QuotaContainer) {
            quotaInfo.Metrics.UpdateMaxQuota(value);
        }
    }

private:
    void UnregisterAll()
    {
        Total.Metrics.Unregister();
        Total.MountTime = TInstant::Zero();
        for (ui32 i = 0; i < DEFAULT_BUCKET_COUNT; ++i) {
            MaxCalc.Add(0);
            MaxCalc.NextValue();
        }
    }
};

}   // namespace NCloud::NBlockStore
