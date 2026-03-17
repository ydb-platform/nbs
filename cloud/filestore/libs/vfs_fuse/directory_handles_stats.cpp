#include "directory_handles_stats.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStats::TDirectoryHandlesStats(ITimerPtr timer)
    : CacheSize(timer)
    , ChunkCount(std::move(timer))
{}

TStringBuf TDirectoryHandlesStats::GetName() const
{
    return "DirectoryHandles";
}

void TDirectoryHandlesStats::RegisterCounters(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry)
{
    Y_UNUSED(aggregatableMetricsRegistry);

    CacheSize.Register(localMetricsRegistry, "MaxCacheSize");
    ChunkCount.Register(localMetricsRegistry, "MaxChunkCount");
}

void TDirectoryHandlesStats::ChangeCacheSize(i64 delta)
{
    CacheSize.Change(delta);
}

void TDirectoryHandlesStats::ChangeChunkCount(i64 delta)
{
    ChunkCount.Change(delta);
}

void TDirectoryHandlesStats::IncreaseCacheSize(size_t value)
{
    ChangeCacheSize(static_cast<i64>(value));
}

void TDirectoryHandlesStats::DecreaseCacheSize(size_t value)
{
    ChangeCacheSize(-static_cast<i64>(value));
}

void TDirectoryHandlesStats::IncreaseChunkCount(size_t value)
{
    ChangeChunkCount(static_cast<i64>(value));
}

void TDirectoryHandlesStats::DecreaseChunkCount(size_t value)
{
    ChangeChunkCount(-static_cast<i64>(value));
}

void TDirectoryHandlesStats::UpdateStats(TInstant now)
{
    Y_UNUSED(now);

    CacheSize.UpdateMax();
    ChunkCount.UpdateMax();
}

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStatsPtr CreateDirectoryHandlesStats(
    IModuleStatsRegistryPtr registry,
    ITimerPtr timer,
    const TString& fileSystemId,
    const TString& clientId,
    const TString& cloudId,
    const TString& folderId)
{
    auto stats = std::make_shared<TDirectoryHandlesStats>(std::move(timer));
    registry->Register(fileSystemId, clientId, cloudId, folderId, stats);
    return stats;
}

}   // namespace NCloud::NFileStore::NFuse
