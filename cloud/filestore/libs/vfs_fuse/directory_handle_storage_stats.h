#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

#include <cloud/storage/core/libs/common/public.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TDirectoryHandleStorageCounters
{
    ui64 FileMapSize = 0;
    ui64 ShrinkCount = 0;
    ui64 ExpansionCount = 0;
    ui64 CompactionCount = 0;
    ui64 UsedSpace = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDirectoryHandleStorageStats
{
    virtual ~IDirectoryHandleStorageStats() = default;

    virtual void RegisterCounters(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) = 0;

    virtual void SetCounters(TDirectoryHandleStorageCounters counters) = 0;
    virtual void IncrementMemoryControllerRejectCount() = 0;

    virtual void UpdateStats() = 0;
};

using IDirectoryHandleStorageStatsPtr =
    std::shared_ptr<IDirectoryHandleStorageStats>;

////////////////////////////////////////////////////////////////////////////////

IDirectoryHandleStorageStatsPtr CreateDirectoryHandleStorageStats(
    ITimerPtr timer);

}   // namespace NCloud::NFileStore::NFuse
