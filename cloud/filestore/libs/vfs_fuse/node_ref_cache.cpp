#include "node_ref_cache.h"

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MinShardEntryCount = 1;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TNodeRefCache::TNodeRefCache(ui32 maxEntryCount, ui32 shardCount)
{
    Y_ABORT_UNLESS(shardCount);
    const ui32 maxShardEntryCount =
        Max(maxEntryCount / shardCount, MinShardEntryCount);
    for (ui32 i = 0; i < shardCount; ++i) {
        Shards.push_back(std::make_unique<TShard>(maxShardEntryCount));
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
    return GetShard(parentNodeId)
        .Get(TNodeRefCacheKey{.ParentNodeId = parentNodeId, .Name = name}, now)
        .GetOrElse(InvalidNodeId);
}

ui64 TNodeRefCache::Invalidate(
    ui64 parentNodeId,
    const TString& name,
    ui64 version)
{
    return GetShard(parentNodeId)
        .Invalidate(
            TNodeRefCacheKey{.ParentNodeId = parentNodeId, .Name = name},
            version)
        .GetOrElse(InvalidNodeId);
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

TNodeRefCache::TShard& TNodeRefCache::GetShard(ui64 parentNodeId)
{
    return *Shards[parentNodeId % Shards.size()];
}

}   // namespace NCloud::NFileStore::NFuse
