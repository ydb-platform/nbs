#include "directory_handles_stats.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStats::TDirectoryHandlesStats(
        ITimerPtr timer,
        NMonitoring::TDynamicCountersPtr counters)
    : CacheSize(timer, counters, "MaxCacheSize")
    , ChunkCount(timer, counters, "MaxChunkCount")
{}

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

void TDirectoryHandlesStats::UpdateStats()
{
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
    auto moduleCounters = registry->GetFileSystemModuleCounters(
        fileSystemId,
        clientId,
        cloudId,
        folderId,
        "DirectoryHandles");
    auto stats = std::make_shared<TDirectoryHandlesStats>(
        std::move(timer),
        std::move(moduleCounters));
    registry->Register(fileSystemId, clientId, stats);
    return stats;
}

}   // namespace NCloud::NFileStore::NFuse
