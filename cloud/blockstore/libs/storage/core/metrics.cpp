#include "metrics.h"

namespace NCloud::NBlockStore::NStorage::NBlobMetrics {

////////////////////////////////////////////////////////////////////////////////

TBlobLoadMetrics::TValue& operator+=(
    TBlobLoadMetrics::TValue& lhs,
    const TBlobLoadMetrics::TValue& rhs)
{
    lhs.ByteCount += rhs.ByteCount;
    lhs.Iops += rhs.Iops;

    return lhs;
}

TBlobLoadMetrics::TTabletMetric& operator+=(
    TBlobLoadMetrics::TTabletMetric& lhs,
    const TBlobLoadMetrics::TTabletMetric& rhs)
{
    lhs.ReadOperations += rhs.ReadOperations;
    lhs.WriteOperations += rhs.WriteOperations;
    return lhs;
}

TBlobLoadMetrics& operator+=(TBlobLoadMetrics& lhs, const TBlobLoadMetrics& rhs)
{
    for (const auto& kind: rhs.PoolKind2TabletOps) {
        for (const auto& value: kind.second) {
            auto& self = lhs.PoolKind2TabletOps[kind.first][value.first];
            self += value.second;
        }
    }

    return lhs;
}

TBlobLoadMetrics TakeDelta(
    const TBlobLoadMetrics& prevMetricsValue,
    const TBlobLoadMetrics& newMetricsValue)
{
    auto takeDelta = [](auto& newValue, const auto& prevValue)
    {
        if (newValue.ByteCount >= prevValue.ByteCount) {
            newValue.ByteCount -= prevValue.ByteCount;
        }

        if (newValue.Iops >= prevValue.Iops) {
            newValue.Iops -= prevValue.Iops;
        }
    };

    TBlobLoadMetrics result = newMetricsValue;
    for (auto& [newKind, newOps]: result.PoolKind2TabletOps) {
        if (const auto prevKind =
                prevMetricsValue.PoolKind2TabletOps.find(newKind);
            prevKind != prevMetricsValue.PoolKind2TabletOps.end())
        {
            for (auto& [newKey, newMetric]: newOps) {
                if (const auto prevOps = prevKind->second.find(newKey);
                    prevOps != prevKind->second.end())
                {
                    takeDelta(
                        newMetric.ReadOperations,
                        prevOps->second.ReadOperations);
                    takeDelta(
                        newMetric.WriteOperations,
                        prevOps->second.WriteOperations);
                }
            }
        }
    }
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage::NBlobMetrics
