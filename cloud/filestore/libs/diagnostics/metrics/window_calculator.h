#pragma once

#include "key.h"
#include "label.h"
#include "registry.h"

#include <util/system/spinlock.h>
#include <util/system/types.h>

#include <array>
#include <atomic>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DEFAULT_BUCKET_COUNT = 15;

////////////////////////////////////////////////////////////////////////////////

template <ui32 BucketCount>
class TWindowCalculator: private TMetricNextFreeKey
{
    using TMetricsRegistriesMap = THashMap<TMetricKey, IMetricsRegistryPtr>;

private:
    std::array<std::atomic<i64>, BucketCount> Buckets;

    std::atomic<ui64> CurrentBucket{0};

    TMetricsRegistriesMap Registries;
    TAdaptiveLock RegistriesLock;

public:
    explicit TWindowCalculator(i64 base = {})
    {
        for (auto& value: Buckets) {
            value.store(base, std::memory_order_relaxed);
        }
    }

    void Record(i64 value)
    {
        const ui64 currentBucket =
            CurrentBucket.fetch_add(1, std::memory_order_relaxed) % BucketCount;
        Buckets[currentBucket].store(value, std::memory_order_relaxed);
    }

    TMetricKey Register(
        IMetricsRegistryPtr registry,
        TLabels labels,
        EAggregationType aggrType = EAggregationType::AT_SUM)
    {
        IMetricsRegistryPtr scoped =
            CreateScopedMetricsRegistry(std::move(labels), std::move(registry));

        for (auto& bucket: Buckets) {
            scoped->Register({}, bucket, aggrType, EMetricType::MT_ABSOLUTE);
        }

        const auto _ = Guard(RegistriesLock);

        const TMetricKey key(this, GenerateNextFreeKey());

        const bool inserted =
            Registries.try_emplace(key, std::move(scoped)).second;
        Y_ABORT_UNLESS(inserted);

        return key;
    }

    void Unregister(const TMetricKey& key)
    {
        const auto _ = Guard(RegistriesLock);
        Registries.erase(key);
    }
};

using TDefaultWindowCalculator = TWindowCalculator<DEFAULT_BUCKET_COUNT>;

}   // namespace NCloud::NFileStore::NMetrics
