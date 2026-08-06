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

// All counters here are individually thread-safe (atomic CAS / atomic loads
// and stores), so SetCounters and UpdateStats may be called from any thread
// without external synchronization.
class TDirectoryHandleStorageStats final
    : public std::enable_shared_from_this<TDirectoryHandleStorageStats>
    , public IDirectoryHandleStorageStats
{
private:
    TMaxCounter<DirectoryHandleMaxBucketCount> RawCapacityByteCounter;
    TMaxCounter<DirectoryHandleMaxBucketCount> RawUsedByteCounter;
    TRelaxedCounter ShrinkCounter;
    TRelaxedCounter ExpansionCounter;
    TRelaxedCounter CompactionCounter;
    TRelaxedCounter MemoryLimiterRejectionCounter;
    TRelaxedCounter HandleSizeLimitRejectionCounter;

public:
    explicit TDirectoryHandleStorageStats(ITimerPtr timer)
        : RawCapacityByteCounter(timer)
        , RawUsedByteCounter(std::move(timer))
    {}

    void SetCounters(TDynamicPersistentTableCounters counters) override
    {
        RawCapacityByteCounter.Set(counters.RawCapacityByteCount);
        RawUsedByteCounter.Set(counters.RawUsedByteCount);
        ShrinkCounter.Set(counters.ShrinkCount);
        ExpansionCounter.Set(counters.ExpansionCount);
        CompactionCounter.Set(counters.CompactionCount);
        MemoryLimiterRejectionCounter.Set(
            counters.MemoryLimiterRejectionCount);
    }

    void IncrementHandleSizeLimitRejection() override
    {
        HandleSizeLimitRejectionCounter.Inc();
    }

    TDirectoryHandleStorageMetrics CreateMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .RawCapacityByteMaxCount = CreateMetric(
                [self] { return self->RawCapacityByteCounter.GetValue(); }),
            .RawUsedByteMaxCount = CreateMetric(
                [self] { return self->RawUsedByteCounter.GetValue(); }),
            .ShrinkCount =
                CreateMetric([self] { return self->ShrinkCounter.Get(); }),
            .ExpansionCount =
                CreateMetric([self] { return self->ExpansionCounter.Get(); }),
            .CompactionCount =
                CreateMetric([self] { return self->CompactionCounter.Get(); }),
            .MemoryLimiterRejectionCount = CreateMetric(
                [self] { return self->MemoryLimiterRejectionCounter.Get(); }),
            .HandleSizeLimitRejectionCount = CreateMetric(
                [self]
                { return self->HandleSizeLimitRejectionCounter.Get(); }),
        };
    }

    void UpdateStats() override
    {
        RawCapacityByteCounter.UpdateMax();
        RawUsedByteCounter.UpdateMax();
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

    localMetricsRegistry.Register(
        {CreateSensor("Storage_HandleSizeLimitRejectionCount")},
        HandleSizeLimitRejectionCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);
}

////////////////////////////////////////////////////////////////////////////////

IDirectoryHandleStorageStatsPtr CreateDirectoryHandleStorageStats(
    ITimerPtr timer)
{
    return std::make_shared<TDirectoryHandleStorageStats>(std::move(timer));
}

}   // namespace NCloud::NFileStore::NFuse
