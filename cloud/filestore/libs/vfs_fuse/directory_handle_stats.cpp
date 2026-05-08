#include "directory_handle_stats.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStats::TDirectoryHandleStats(ITimerPtr timer)
    : CacheSize(timer)
    , ChunkCount(std::move(timer))
{}

TStringBuf TDirectoryHandleStats::GetName() const
{
    return "DirectoryHandles";
}

void TDirectoryHandleStats::RegisterCounters(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry)
{
    Y_UNUSED(aggregatableMetricsRegistry);

    CacheSize.Register(localMetricsRegistry, "MaxCacheSize");
    ChunkCount.Register(localMetricsRegistry, "MaxChunkCount");
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

void TDirectoryHandleStats::UpdateStats(TInstant now)
{
    Y_UNUSED(now);

    CacheSize.UpdateMax();
    ChunkCount.UpdateMax();
}

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStatsPtr CreateDirectoryHandleStats(ITimerPtr timer)
{
    return std::make_shared<TDirectoryHandleStats>(std::move(timer));
}

}   // namespace NCloud::NFileStore::NFuse
