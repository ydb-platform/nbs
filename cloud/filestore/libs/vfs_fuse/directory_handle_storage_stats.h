#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/file_backed_containers/dynamic_persistent_table_counters.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TDirectoryHandleStorageMetrics
{
    NMetrics::IMetricPtr RawCapacityByteMaxCount;
    NMetrics::IMetricPtr RawUsedByteMaxCount;
    // number of times the file map shrank
    NMetrics::IMetricPtr ShrinkCount;
    // number of times the file map expanded
    NMetrics::IMetricPtr ExpansionCount;
    NMetrics::IMetricPtr CompactionCount;
    NMetrics::IMetricPtr MemoryLimiterRejectionCount;

    void Register(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const;
};

////////////////////////////////////////////////////////////////////////////////

struct IDirectoryHandleStorageStats
{
    virtual ~IDirectoryHandleStorageStats() = default;

    virtual void SetCounters(TDynamicPersistentTableCounters counters) = 0;

    virtual TDirectoryHandleStorageMetrics CreateMetrics() const = 0;
    virtual void UpdateStats() = 0;
};

using IDirectoryHandleStorageStatsPtr =
    std::shared_ptr<IDirectoryHandleStorageStats>;

////////////////////////////////////////////////////////////////////////////////

IDirectoryHandleStorageStatsPtr CreateDirectoryHandleStorageStats(
    ITimerPtr timer);

}   // namespace NCloud::NFileStore::NFuse
