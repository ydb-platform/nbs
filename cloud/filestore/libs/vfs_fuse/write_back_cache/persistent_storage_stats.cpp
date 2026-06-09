#include "persistent_storage_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/vfs_fuse/counters/relaxed_counters.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPersistentStorageStats
    : public std::enable_shared_from_this<TPersistentStorageStats>
    , public IPersistentStorageStats
{
private:
    TRelaxedCounter RawCapacityBytesCounter;
    TRelaxedCombinedMaxCounter<> RawUsedBytesCounter;
    TRelaxedCombinedMaxCounter<> EntryCounter;
    TRelaxedCounter MaxObservedEntryByteCount;
    TRelaxedCounter VersionCounter;
    TRelaxedCounter CorruptedCounter;

public:
    void SetPersistentStorageCounters(
        const TPersistentStorageCounters& counters) override
    {
        RawCapacityBytesCounter.Set(
            static_cast<i64>(counters.RawCapacityBytesCount));
        RawUsedBytesCounter.Set(static_cast<i64>(counters.RawUsedBytesCount));
        EntryCounter.Set(static_cast<i64>(counters.EntryCount));
        MaxObservedEntryByteCount.Set(
            static_cast<i64>(counters.MaxObservedEntryByteCount));
        VersionCounter.Set(static_cast<i64>(counters.Version));
        CorruptedCounter.Set(counters.IsCorrupted ? 1 : 0);
    }

    TPersistentStorageMetrics CreateMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .Storage = {
                .RawCapacityByteCount = CreateMetric(
                    [self] { return self->RawCapacityBytesCounter.Get(); }),
                .RawUsedByteCount = CreateMetric(
                    [self] { return self->RawUsedBytesCounter.GetCurrent(); }),
                .RawUsedByteMaxCount = CreateMetric(
                    [self] { return self->RawUsedBytesCounter.GetMax(); }),
                .EntryCount = CreateMetric(
                    [self] { return self->EntryCounter.GetCurrent(); }),
                .EntryMaxCount = CreateMetric(
                    [self] { return self->EntryCounter.GetMax(); }),
                .MaxObservedEntryByteCount = CreateMetric(
                    [self] { return self->MaxObservedEntryByteCount.Get(); }),
                .Version =
                    CreateMetric([self] { return self->VersionCounter.Get(); }),
                .Corrupted = CreateMetric(
                    [self] { return self->CorruptedCounter.Get(); }),
            }};
    }

    void UpdateStats() override
    {
        RawUsedBytesCounter.Update();
        EntryCounter.Update();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPersistentStorageMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    localMetricsRegistry.Register(
        {CreateSensor("Storage_RawCapacityByteCount")},
        Storage.RawCapacityByteCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_RawUsedByteCount")},
        Storage.RawUsedByteCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_RawUsedByteMaxCount")},
        Storage.RawUsedByteMaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_EntryCount")},
        Storage.EntryCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_EntryMaxCount")},
        Storage.EntryMaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_MaxObservedEntryByteCount")},
        Storage.MaxObservedEntryByteCount,
        EAggregationType::AT_MAX,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_Version")},
        Storage.Version,
        EAggregationType::AT_MAX,
        EMetricType::MT_ABSOLUTE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("Storage_Corrupted")},
        Storage.Corrupted,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);
}

////////////////////////////////////////////////////////////////////////////////

IPersistentStorageStatsPtr CreatePersistentStorageStats()
{
    return std::make_shared<TPersistentStorageStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
