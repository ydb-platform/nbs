#pragma once

#include "directory_handle_max_metric.h"
#include "directory_handle_storage_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/diagnostics/module_stats.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/yassert.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandleStats final: public IModuleStats
{
private:
    TMaxMetric<DirectoryHandleMaxBucketCount> CacheSize;
    TMaxMetric<DirectoryHandleMaxBucketCount> ChunkCount;
    TMaxMetric<DirectoryHandleMaxBucketCount> OpenHandleCount;
    IDirectoryHandleStorageStatsPtr StorageStats;

    void ChangeCacheSize(i64 delta);
    void ChangeChunkCount(i64 delta);

public:
    explicit TDirectoryHandleStats(ITimerPtr timer);

    TStringBuf GetName() const override;

    void RegisterCounters(
        const NMetrics::IMetricsRegistryPtr& localMetricsRegistry,
        const NMetrics::IMetricsRegistryPtr& aggregatableMetricsRegistry)
        override;

    void IncreaseCacheSize(size_t value);
    void DecreaseCacheSize(size_t value);
    void IncreaseChunkCount(size_t value);
    void DecreaseChunkCount(size_t value);
    void SetOpenHandleCount(size_t value);

    IDirectoryHandleStorageStatsPtr GetStorageStats();

    void UpdateStats(TInstant now) override;
};

using TDirectoryHandleStatsPtr = std::shared_ptr<TDirectoryHandleStats>;

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStatsPtr CreateDirectoryHandleStats(ITimerPtr timer);

}   // namespace NCloud::NFileStore::NFuse
