#include "throttler_metrics.h"

#include <cloud/blockstore/libs/diagnostics/quota_metrics.h>

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/map.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr std::array<NProto::EStorageMediaKind, 6> MonitoredMediaKinds = {
    NProto::STORAGE_MEDIA_SSD,
    NProto::STORAGE_MEDIA_HDD,
    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
    NProto::STORAGE_MEDIA_SSD_MIRROR2,
    NProto::STORAGE_MEDIA_SSD_MIRROR3,
    NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
};

////////////////////////////////////////////////////////////////////////////////

NProto::EStorageMediaKind NormalizeMediaKind(NProto::EStorageMediaKind mediaKind)
{
    static_assert(NProto::EStorageMediaKind_MAX == 9);
    switch (mediaKind) {
        case NProto::STORAGE_MEDIA_HDD:
        case NProto::STORAGE_MEDIA_DEFAULT:
        case NProto::STORAGE_MEDIA_HYBRID:
            return NProto::STORAGE_MEDIA_HDD;
        default:
            return mediaKind;
    }
}

////////////////////////////////////////////////////////////////////////////////

THashMap<NProto::EStorageMediaKind, ui32> MergeQuotaByMediaKind(
    THashMap<NProto::EStorageMediaKind, ui32>& quota)
{
    THashMap<NProto::EStorageMediaKind, ui32> result;
    for (const auto& [mediaKind, quota]: quota) {
        result[NormalizeMediaKind(mediaKind)] += quota;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TThrottlerMetrics
    : public IThrottlerMetrics
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
    TMap<NProto::EStorageMediaKind, TQuotaInfo> QuotaPerMediaKind;
    TMap<NProto::EStorageMediaKind, TMaxCalculator<DEFAULT_BUCKET_COUNT>> MaxCalcPerMediaKind;

public:
    TThrottlerMetrics(
            ITimerPtr timer,
            NMonitoring::TDynamicCountersPtr totalGroup,
            NMonitoring::TDynamicCountersPtr volumeGroup);

    void Register(
        const TString& diskId,
        const TString& instanceId) override;
    void Unregister(
        const TString& diskId,
        const TString& instanceId) override;

    void Trim(TInstant now) override;
    void UpdateUsedQuota(TUsedQuota quota) override;
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
{
    for (auto mediaKind: MonitoredMediaKinds) {
        MaxCalcPerMediaKind.try_emplace(mediaKind, Timer);

        QuotaPerMediaKind.try_emplace(
            mediaKind,
            TotalGroup->GetSubgroup("type", MediaKindToString(mediaKind)),
            Timer);
    }
}

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

void TThrottlerMetrics::UpdateUsedQuota(TUsedQuota quota)
{
    MaxCalc.Add(quota.Total);
    Total.Metrics.UpdateQuota(quota.Total);

    TReadGuard guard(Lock);

    for (auto& [_, quotaInfo]: QuotaContainer) {
        quotaInfo.Metrics.UpdateQuota(quota.Total);
    }

    auto mergedQuota = MergeQuotaByMediaKind(quota.PerMediaKind);

    for (auto& [mediaKind, quotaInfo]: QuotaPerMediaKind) {
        quotaInfo.Metrics.UpdateQuota(mergedQuota[mediaKind]);
    }

    for (auto& [mediaKind, maxCalc]: MaxCalcPerMediaKind) {
        maxCalc.Add(mergedQuota[mediaKind]);
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

    for (auto& [mediaKind, quotaInfo]: QuotaPerMediaKind) {
        auto it = MaxCalcPerMediaKind.find(mediaKind);
        if (it == MaxCalcPerMediaKind.end()) {
            continue;
        }
        quotaInfo.Metrics.UpdateMaxQuota(it->second.NextValue());
    }
}

void TThrottlerMetrics::UnregisterAll()
{
    Total.MountTime = TInstant::Zero();
    for (ui32 i = 0; i < DEFAULT_BUCKET_COUNT; ++i) {
        MaxCalc.Add(0);
        MaxCalc.NextValue();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TThrottlerMetricsStub final
    : public IThrottlerMetrics
{
public:
    void Register(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void Unregister(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void Trim(TInstant now) override
    {
        Y_UNUSED(now);
    }

    void UpdateUsedQuota(TUsedQuota quota) override
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
        rootGroup
            ->GetSubgroup("component", componentLabel + "_volume")
            ->GetSubgroup("host", "cluster"));
}


}   // namespace NCloud::NBlockStore
