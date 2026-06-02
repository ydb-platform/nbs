#include "directory_handle_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleModuleStats::TDirectoryHandleModuleStats(
    ITimerPtr timer,
    IDirectoryHandleStorageStatsPtr storageStats)
    : CacheSize(timer)
    , ChunkCount(timer)
    , OpenHandleCount(timer)
    , StorageStats(std::move(storageStats))
{}

TStringBuf TDirectoryHandleModuleStats::GetName() const
{
    return "DirectoryHandles";
}

void TDirectoryHandleModuleStats::RegisterCounters(
    const NMetrics::IMetricsRegistryPtr& localMetricsRegistry,
    const NMetrics::IMetricsRegistryPtr& aggregatableMetricsRegistry)
{
    auto self = shared_from_this();

    localMetricsRegistry->Register(
        {NMetrics::CreateSensor("MaxCacheSize")},
        NMetrics::CreateMetric([self] { return self->CacheSize.GetValue(); }));
    localMetricsRegistry->Register(
        {NMetrics::CreateSensor("MaxChunkCount")},
        NMetrics::CreateMetric([self] { return self->ChunkCount.GetValue(); }));
    localMetricsRegistry->Register(
        {NMetrics::CreateSensor("MaxOpenHandleCount")},
        NMetrics::CreateMetric([self]
                               { return self->OpenHandleCount.GetValue(); }));
    localMetricsRegistry->Register(
        {NMetrics::CreateSensor("RewindCount")},
        NMetrics::CreateMetric([self] { return self->RewindCount.load(); }),
        NMetrics::EAggregationType::AT_SUM,
        NMetrics::EMetricType::MT_DERIVATIVE);

    if (StorageStats) {
        StorageStats->CreateMetrics().Register(
            *localMetricsRegistry,
            *aggregatableMetricsRegistry);
    }
}

void TDirectoryHandleModuleStats::ChangeCacheSize(i64 delta)
{
    CacheSize.Change(delta);
}

void TDirectoryHandleModuleStats::ChangeChunkCount(i64 delta)
{
    ChunkCount.Change(delta);
}

void TDirectoryHandleModuleStats::ChangeOpenHandleCount(i64 delta)
{
    OpenHandleCount.Change(delta);
}

void TDirectoryHandleModuleStats::IncrementRewindCount()
{
    RewindCount.fetch_add(1, std::memory_order_relaxed);
}

void TDirectoryHandleModuleStats::UpdateStats(TInstant now)
{
    Y_UNUSED(now);

    CacheSize.UpdateMax();
    ChunkCount.UpdateMax();
    OpenHandleCount.UpdateMax();
    if (StorageStats) {
        StorageStats->UpdateStats();
    }
}

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleModuleStatsPtr CreateDirectoryHandleStats(
    ITimerPtr timer,
    IDirectoryHandleStorageStatsPtr storageStats)
{
    return std::make_shared<TDirectoryHandleModuleStats>(
        std::move(timer),
        std::move(storageStats));
}

}   // namespace NCloud::NFileStore::NFuse
