#include "node_attrs_cache.h"

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MinShardEntryCount = 1;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TNodeAttrsCache::TNodeAttrsCache(ui32 maxEntryCount, ui32 shardCount)
{
    Y_ABORT_UNLESS(shardCount);
    const ui32 maxShardEntryCount =
        Max(maxEntryCount / shardCount, MinShardEntryCount);
    for (ui32 i = 0; i < shardCount; ++i) {
        Shards.push_back(std::make_unique<TShard>(maxShardEntryCount));
    }
}

void TNodeAttrsCache::Add(
    const NProto::TNodeAttr& attrs,
    ui64 version,
    TInstant deadline)
{
    GetShard(attrs.GetId()).Add(attrs.GetId(), attrs, version, deadline);
}

TMaybe<NProto::TNodeAttr> TNodeAttrsCache::Get(ui64 nodeId, TInstant now)
{
    return GetShard(nodeId).Get(nodeId, now);
}

void TNodeAttrsCache::Invalidate(ui64 nodeId, ui64 version)
{
    //
    // The erased attrs are not needed by the callers.
    //

    Y_UNUSED(GetShard(nodeId).Invalidate(nodeId, version));
}

void TNodeAttrsCache::InvalidateAll(ui64 version)
{
    for (auto& shard: Shards) {
        shard->InvalidateAll(version);
    }
}

size_t TNodeAttrsCache::GetEntryCount()
{
    size_t count = 0;
    for (auto& shard: Shards) {
        count += shard->GetEntryCount();
    }
    return count;
}

TNodeAttrsCache::TShard& TNodeAttrsCache::GetShard(ui64 nodeId)
{
    return *Shards[nodeId % Shards.size()];
}

}   // namespace NCloud::NFileStore::NFuse
