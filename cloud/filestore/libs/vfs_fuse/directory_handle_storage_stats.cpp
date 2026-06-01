#include "directory_handle_storage_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/vfs_fuse/counters/max_counter.h>
#include <cloud/filestore/libs/vfs_fuse/counters/relaxed_counters.h>

namespace NCloud::NFileStore::NFuse {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Storage counters are always updated while holding the storage's TableLock,
// so relaxed memory ordering is sufficient.
class TDirectoryHandleStorageStats final
    : public std::enable_shared_from_this<TDirectoryHandleStorageStats>
    , public IDirectoryHandleStorageStats
{
private:
    TRelaxedCombinedMaxCounter<DirectoryHandleMaxBucketCount>
        RawCapacityByteCounter;
    TRelaxedCombinedMaxCounter<DirectoryHandleMaxBucketCount>
        RawUsedByteCounter;
    TRelaxedCounter ShrinkCounter;
    TRelaxedCounter ExpansionCounter;
    TRelaxedCounter CompactionCounter;
    TRelaxedCounter MemoryLimiterRejectionCounter;

public:
    void SetCounters(TDynamicPersistentTableCounters counters) override
    {
        RawCapacityByteCounter.Set(
            static_cast<i64>(counters.RawCapacityByteCount));
        RawUsedByteCounter.Set(static_cast<i64>(counters.RawUsedByteCount));
        ShrinkCounter.Set(static_cast<i64>(counters.ShrinkCount));
        ExpansionCounter.Set(static_cast<i64>(counters.ExpansionCount));
        CompactionCounter.Set(static_cast<i64>(counters.CompactionCount));
        MemoryLimiterRejectionCounter.Set(
            static_cast<i64>(counters.MemoryLimiterRejectionCount));
    }

    TDirectoryHandleStorageMetrics CreateMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .RawCapacityByteMaxCount = CreateMetric(
                [self] { return self->RawCapacityByteCounter.GetMax(); }),
            .RawUsedByteMaxCount = CreateMetric(
                [self] { return self->RawUsedByteCounter.GetMax(); }),
            .ShrinkCount =
                CreateMetric([self] { return self->ShrinkCounter.Get(); }),
            .ExpansionCount =
                CreateMetric([self] { return self->ExpansionCounter.Get(); }),
            .CompactionCount =
                CreateMetric([self] { return self->CompactionCounter.Get(); }),
            .MemoryLimiterRejectionCount = CreateMetric(
                [self] { return self->MemoryLimiterRejectionCounter.Get(); }),
        };
    }

    void UpdateStats() override
    {
        RawCapacityByteCounter.Update();
        RawUsedByteCounter.Update();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDirectoryHandleStorageMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    Y_UNUSED(aggregatableMetricsRegistry);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_RawCapacityByteMaxCount")},
        RawCapacityByteMaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_RawUsedByteMaxCount")},
        RawUsedByteMaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_ShrinkCount")},
        ShrinkCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_ExpansionCount")},
        ExpansionCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_CompactionCount")},
        CompactionCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    localMetricsRegistry.Register(
        {CreateSensor("Storage_MemoryLimiterRejectionCount")},
        MemoryLimiterRejectionCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);
}

////////////////////////////////////////////////////////////////////////////////

IDirectoryHandleStorageStatsPtr CreateDirectoryHandleStorageStats()
{
    return std::make_shared<TDirectoryHandleStorageStats>();
}

}   // namespace NCloud::NFileStore::NFuse
