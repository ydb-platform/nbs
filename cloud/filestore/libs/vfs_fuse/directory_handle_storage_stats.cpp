#include "directory_handle_storage_stats.h"

#include "directory_handle_max_metric.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

#include <atomic>
#include <utility>

namespace NCloud::NFileStore::NFuse {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandleStorageStats final: public IDirectoryHandleStorageStats
{
private:
    TMaxMetric<DirectoryHandleMaxBucketCount> FileMapSizeCounter;
    TMaxMetric<DirectoryHandleMaxBucketCount> UsedSpaceCounter;
    std::atomic<i64> ShrinkCounter = 0;
    std::atomic<i64> ExpansionCounter = 0;
    std::atomic<i64> CompactionCounter = 0;
    std::atomic<i64> MemoryControllerRejectCounter = 0;

public:
    explicit TDirectoryHandleStorageStats(ITimerPtr timer)
        : FileMapSizeCounter(timer)
        , UsedSpaceCounter(std::move(timer))
    {}

    void RegisterCounters(
        IMetricsRegistry& localMetricsRegistry,
        IMetricsRegistry& aggregatableMetricsRegistry) override
    {
        Y_UNUSED(aggregatableMetricsRegistry);

        FileMapSizeCounter.Register(
            localMetricsRegistry,
            "Storage_FileMapSizeMax");
        UsedSpaceCounter.Register(localMetricsRegistry, "Storage_UsedSpaceMax");

        localMetricsRegistry.Register(
            {CreateSensor("Storage_ShrinkCount")},
            ShrinkCounter,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        localMetricsRegistry.Register(
            {CreateSensor("Storage_ExpansionCount")},
            ExpansionCounter,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        localMetricsRegistry.Register(
            {CreateSensor("Storage_CompactionCount")},
            CompactionCounter,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        localMetricsRegistry.Register(
            {CreateSensor("Storage_MemoryControllerRejectCount")},
            MemoryControllerRejectCounter,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);
    }

    void SetCounters(TDirectoryHandleStorageCounters counters) override
    {
        FileMapSizeCounter.Set(counters.FileMapSize);
        UsedSpaceCounter.Set(counters.UsedSpace);
        ShrinkCounter.store(static_cast<i64>(counters.ShrinkCount));
        ExpansionCounter.store(static_cast<i64>(counters.ExpansionCount));
        CompactionCounter.store(static_cast<i64>(counters.CompactionCount));
    }

    void IncrementMemoryControllerRejectCount() override
    {
        MemoryControllerRejectCounter.fetch_add(1);
    }

    void UpdateStats() override
    {
        FileMapSizeCounter.UpdateMax();
        UsedSpaceCounter.UpdateMax();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDirectoryHandleStorageStatsPtr CreateDirectoryHandleStorageStats(
    ITimerPtr timer)
{
    return std::make_shared<TDirectoryHandleStorageStats>(std::move(timer));
}

}   // namespace NCloud::NFileStore::NFuse
