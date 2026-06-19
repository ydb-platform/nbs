#pragma once

#include "directory_handle_storage_stats.h"

#include <cloud/filestore/libs/diagnostics/module_stats.h>
#include <cloud/filestore/libs/vfs_fuse/counters/max_counter.h>
#include <cloud/filestore/libs/vfs_fuse/counters/relaxed_counters.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <util/system/types.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandleModuleStats final
    : public std::enable_shared_from_this<TDirectoryHandleModuleStats>
    , public IModuleStats
{
private:
    TMaxCounter<DirectoryHandleMaxBucketCount> CacheSize;
    TMaxCounter<DirectoryHandleMaxBucketCount> ChunkCount;
    TMaxCounter<DirectoryHandleMaxBucketCount> OpenHandleCount;
    TMaxCounter<DirectoryHandleMaxBucketCount> EntryVersionCacheEntryCount;
    TRelaxedCounter RewindCount;
    // Optional; when null no storage sensors are registered or updated.
    IDirectoryHandleStorageStatsPtr StorageStats;

public:
    TDirectoryHandleModuleStats(
        ITimerPtr timer,
        IDirectoryHandleStorageStatsPtr storageStats);

    TStringBuf GetName() const override;

    void RegisterCounters(
        const NMetrics::IMetricsRegistryPtr& localMetricsRegistry,
        const NMetrics::IMetricsRegistryPtr& aggregatableMetricsRegistry)
        override;

    void ChangeCacheSize(i64 delta);
    void ChangeChunkCount(i64 delta);
    void ChangeOpenHandleCount(i64 delta);
    void ChangeEntryVersionCacheEntryCount(i64 delta);
    void IncrementRewindCount();

    void UpdateStats(TInstant now) override;
};

using TDirectoryHandleModuleStatsPtr =
    std::shared_ptr<TDirectoryHandleModuleStats>;

////////////////////////////////////////////////////////////////////////////////

// storageStats may be null when the persistent directory-handle storage is
// disabled, the resulting module stats will not register or update the
// storage-level sensors.
TDirectoryHandleModuleStatsPtr CreateDirectoryHandleStats(
    ITimerPtr timer,
    IDirectoryHandleStorageStatsPtr storageStats);

}   // namespace NCloud::NFileStore::NFuse
