#pragma once

#include <ydb/core/tablet/tablet_metrics.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/system/types.h>


namespace NCloud::NBlockStore::NStorage::NBlobMetrics {

////////////////////////////////////////////////////////////////////////////////

struct TBlobLoadMetrics
{
    struct TValue
    {
        ui64 ByteCount = 0;
        ui64 Iops      = 0;
    };

    struct TTabletMetric
    {
        TValue ReadOperations;
        TValue WriteOperations;
    };

    using TGroupId = ui32;
    using TChannel = ui8;
    using TTabletOperations =
        THashMap<std::pair<TChannel, TGroupId>, TTabletMetric>;

    THashMap<TString, TTabletOperations> PoolKind2TabletOps;
};

////////////////////////////////////////////////////////////////////////////////

TBlobLoadMetrics::TValue& operator+=(
    TBlobLoadMetrics::TValue& lhs,
    const TBlobLoadMetrics::TValue& rhs);

TBlobLoadMetrics::TTabletMetric& operator+=(
    TBlobLoadMetrics::TTabletMetric& lhs,
    const TBlobLoadMetrics::TTabletMetric& rhs);

TBlobLoadMetrics& operator+=(
    TBlobLoadMetrics& lhs,
    const TBlobLoadMetrics& rhs);

TBlobLoadMetrics TakeDelta(
    const TBlobLoadMetrics& prevMetricsValue,
    const TBlobLoadMetrics& newMetricsValue);

template<typename ProfilesContainer>
TBlobLoadMetrics MakeBlobLoadMetrics(
    const ProfilesContainer& profiles,
    const NKikimr::NMetrics::TResourceMetrics& data)
{
    auto getPoolKind = [&profiles](ui32 channel) {
        return profiles.size() > static_cast<int>(channel)
               ? profiles[channel].GetPoolKind()
               : "";
    };

    TBlobLoadMetrics result;

    for (const auto& operation: data.ReadThroughput) {
        const auto key = operation.first;
        auto& tabletOps = result.PoolKind2TabletOps[getPoolKind(key.first)];
        auto& tabletReadOps = tabletOps[key].ReadOperations;

        tabletReadOps.ByteCount += operation.second.GetRawValue();
    }

    for (const auto& operation: data.ReadIops) {
        const auto key = operation.first;
        auto& tabletOps = result.PoolKind2TabletOps[getPoolKind(key.first)];
        auto& tabletReadOps = tabletOps[key].ReadOperations;

        tabletReadOps.Iops += operation.second.GetRawValue();
    }

    for (const auto& operation: data.WriteThroughput) {
        const auto key = operation.first;
        auto& tabletOps = result.PoolKind2TabletOps[getPoolKind(key.first)];
        auto& tabletWriteOps = tabletOps[key].WriteOperations;

        tabletWriteOps.ByteCount  += operation.second.GetRawValue();
    }

    for (const auto& operation: data.WriteIops) {
        const auto key = operation.first;
        auto& tabletOps = result.PoolKind2TabletOps[getPoolKind(key.first)];
        auto& tabletWriteOps = tabletOps[key].WriteOperations;

        tabletWriteOps.Iops += operation.second.GetRawValue();
    }

    return result;
}

} // namespace NCloud::NBlockStore::NStorage::NBlobMetrics
