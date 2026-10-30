#include "node_ref_cache.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TNodeRefCacheShard::TNodeRefCacheShard(ui32 maxEntryCount)
    : Entries(maxEntryCount)
    , MaxTombstoneCount(maxEntryCount)
{}

void TNodeRefCacheShard::Add(
    const TNodeRefCacheKey& key,
    ui64 nodeId,
    ui64 version,
    TInstant deadline)
{
    auto g = Guard(Lock);

    if (version < Watermark) {
        return;
    }

    if (const auto* tombstone = Tombstones.FindPtr(key);
        tombstone && version < *tombstone)
    {
        return;
    }

    Entries.Update(key, TEntry{.NodeId = nodeId, .Deadline = deadline});
}

ui64 TNodeRefCacheShard::Get(
    const TNodeRefCacheKey& key,
    TInstant now)
{
    auto g = Guard(Lock);

    auto it = Entries.Find(key);
    if (it == Entries.End()) {
        return InvalidNodeId;
    }

    if (now >= it.Value().Deadline) {
        Entries.Erase(it);
        return InvalidNodeId;
    }

    return it.Value().NodeId;
}

ui64 TNodeRefCacheShard::Invalidate(
    const TNodeRefCacheKey& key,
    ui64 version)
{
    auto g = Guard(Lock);

    AddTombstone(key, version);

    auto it = Entries.Find(key);
    if (it == Entries.End()) {
        return InvalidNodeId;
    }

    const ui64 nodeId = it.Value().NodeId;
    Entries.Erase(it);
    return nodeId;
}

void TNodeRefCacheShard::InvalidateAll(ui64 version)
{
    auto g = Guard(Lock);

    Entries.Clear();
    Tombstones.clear();
    TombstoneQueue.clear();
    Watermark = Max(Watermark, version);
}

size_t TNodeRefCacheShard::GetEntryCount()
{
    auto g = Guard(Lock);

    return Entries.Size();
}

void TNodeRefCacheShard::AddTombstone(
    const TNodeRefCacheKey& key,
    ui64 version)
{
    auto& tombstoneVersion = Tombstones[key];
    tombstoneVersion = Max(tombstoneVersion, version);
    TombstoneQueue.push_back({.Key = key, .Version = version});

    //
    // The oldest tombstones are evicted first. A key may be present in the
    // queue several times - the map entry is erased only by its latest queue
    // entry. The evicted versions raise the watermark so that the requests
    // started before the evicted invalidations still don't cache anything.
    //

    while (TombstoneQueue.size() > MaxTombstoneCount) {
        const auto& oldest = TombstoneQueue.front();
        Watermark = Max(Watermark, oldest.Version);
        auto it = Tombstones.find(oldest.Key);
        if (it != Tombstones.end() && it->second == oldest.Version) {
            Tombstones.erase(it);
        }
        TombstoneQueue.pop_front();
    }
}

////////////////////////////////////////////////////////////////////////////////

TNodeRefCache::TNodeRefCache(ui32 maxEntryCount, ui32 shardCount)
{
    Y_ABORT_UNLESS(shardCount);
    const ui32 maxShardEntryCount = Max(maxEntryCount / shardCount, 1U);
    for (ui32 i = 0; i < shardCount; ++i) {
        Shards.push_back(
            std::make_unique<TNodeRefCacheShard>(maxShardEntryCount));
    }
}

void TNodeRefCache::Add(
    ui64 parentNodeId,
    const TString& name,
    ui64 nodeId,
    ui64 version,
    TInstant deadline)
{
    GetShard(parentNodeId).Add(
        TNodeRefCacheKey{.ParentNodeId = parentNodeId, .Name = name},
        nodeId,
        version,
        deadline);
}

ui64 TNodeRefCache::Get(
    ui64 parentNodeId,
    const TString& name,
    TInstant now)
{
    return GetShard(parentNodeId).Get(
        TNodeRefCacheKey{.ParentNodeId = parentNodeId, .Name = name},
        now);
}

ui64 TNodeRefCache::Invalidate(
    ui64 parentNodeId,
    const TString& name,
    ui64 version)
{
    return GetShard(parentNodeId).Invalidate(
        TNodeRefCacheKey{.ParentNodeId = parentNodeId, .Name = name},
        version);
}

void TNodeRefCache::InvalidateAll(ui64 version)
{
    for (auto& shard: Shards) {
        shard->InvalidateAll(version);
    }
}

size_t TNodeRefCache::GetEntryCount()
{
    size_t count = 0;
    for (auto& shard: Shards) {
        count += shard->GetEntryCount();
    }
    return count;
}

TNodeRefCacheShard& TNodeRefCache::GetShard(ui64 parentNodeId)
{
    return *Shards[parentNodeId % Shards.size()];
}

}   // namespace NCloud::NFileStore::NFuse
