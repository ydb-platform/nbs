#include "directory_handle_stats.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStats::TDirectoryHandleStats(ITimerPtr timer)
    : CacheSize(timer)
    , ChunkCount(timer)
    , OpenHandleCount(timer)
    , StorageStats(CreateDirectoryHandleStorageStats(std::move(timer)))
{}

TStringBuf TDirectoryHandleStats::GetName() const
{
    return "DirectoryHandles";
}

void TDirectoryHandleStats::RegisterCounters(
    const NMetrics::IMetricsRegistryPtr& localMetricsRegistry,
    const NMetrics::IMetricsRegistryPtr& aggregatableMetricsRegistry)
{
    CacheSize.Register(*localMetricsRegistry, "MaxCacheSize");
    ChunkCount.Register(*localMetricsRegistry, "MaxChunkCount");
    OpenHandleCount.Register(*localMetricsRegistry, "MaxOpenHandleCount");
    StorageStats->RegisterCounters(
        *localMetricsRegistry,
        *aggregatableMetricsRegistry);
}

void TDirectoryHandleStats::ChangeCacheSize(i64 delta)
{
    CacheSize.Change(delta);
}

void TDirectoryHandleStats::ChangeChunkCount(i64 delta)
{
    ChunkCount.Change(delta);
}

void TDirectoryHandleStats::IncreaseCacheSize(size_t value)
{
    ChangeCacheSize(static_cast<i64>(value));
}

void TDirectoryHandleStats::DecreaseCacheSize(size_t value)
{
    ChangeCacheSize(-static_cast<i64>(value));
}

void TDirectoryHandleStats::IncreaseChunkCount(size_t value)
{
    ChangeChunkCount(static_cast<i64>(value));
}

void TDirectoryHandleStats::DecreaseChunkCount(size_t value)
{
    ChangeChunkCount(-static_cast<i64>(value));
}

void TDirectoryHandleStats::SetOpenHandleCount(size_t value)
{
    OpenHandleCount.Set(value);
}

IDirectoryHandleStorageStatsPtr TDirectoryHandleStats::GetStorageStats()
{
    return StorageStats;
}

void TDirectoryHandleStats::UpdateStats(TInstant now)
{
    Y_UNUSED(now);

    CacheSize.UpdateMax();
    ChunkCount.UpdateMax();
    OpenHandleCount.UpdateMax();
    StorageStats->UpdateStats();
}

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStatsPtr CreateDirectoryHandleStats(ITimerPtr timer)
{
    return std::make_shared<TDirectoryHandleStats>(std::move(timer));
}

}   // namespace NCloud::NFileStore::NFuse
