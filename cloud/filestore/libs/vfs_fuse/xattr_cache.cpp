#include "xattr_cache.h"

#include <util/system/guard.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MinShardEntryCount = 1;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TXAttrCache::TXAttrCache(
        ITimerPtr timer,
        ui32 maxEntryCount,
        ui32 shardCount,
        TDuration timeout)
    : Timer(std::move(timer))
    , Timeout(timeout)
{
    Y_ABORT_UNLESS(shardCount);
    const ui32 maxShardEntryCount =
        Max(maxEntryCount / shardCount, MinShardEntryCount);
    for (ui32 i = 0; i < shardCount; ++i) {
        Shards.push_back(std::make_unique<TShard>(maxShardEntryCount));
    }
}

TXAttrCache::TShard& TXAttrCache::GetShard(ui64 ino)
{
    return *Shards[ino % Shards.size()];
}

TInstant TXAttrCache::GetDeadline(TInstant now, TDuration cacheTTL) const
{
    return now + (cacheTTL ? cacheTTL : Timeout);
}

bool TXAttrCache::IsCacheable(
    const TShard& shard,
    const TXAttrCacheKey& key,
    ui64 cacheVersion,
    ui64 sourceId)
{
    return shard.Cache.IsCacheable(key, cacheVersion) &&
           shard.Cache.IsSourceValid(sourceId, cacheVersion) &&
           shard.NodeInvalidations.IsCacheable(key.NodeId, cacheVersion);
}

const TXAttr* TXAttrCache::GetValid(
    TShard& shard,
    const TXAttrCacheKey& key,
    TInstant now)
{
    const auto* entry = shard.Cache.Get(key, now);
    if (!entry) {
        return nullptr;
    }

    //
    // The node-wide invalidations don't erase the entries - the entries
    // cached before such an invalidation are dropped here.
    //

    if (!shard.NodeInvalidations.IsCacheable(key.NodeId, entry->Version)) {
        shard.Cache.Erase(key);
        return nullptr;
    }

    return &entry->Value;
}

void TXAttrCache::AddValue(
    TShard& shard,
    const TXAttrCacheKey& key,
    const TString& value,
    ui64 version,
    ui64 cacheVersion,
    ui64 sourceId,
    TInstant now,
    TInstant deadline)
{
    if (!IsCacheable(shard, key, cacheVersion, sourceId)) {
        return;
    }

    //
    // The responses may arrive out of order - a newer value may have been
    // cached already.
    //

    const auto* current = GetValid(shard, key, now);
    if (current && current->Version >= version) {
        return;
    }

    shard.Cache.Add(
        key,
        TXAttr{.Value = value, .Version = version},
        cacheVersion,
        sourceId,
        deadline);
}

void TXAttrCache::AddAbsentValue(
    TShard& shard,
    const TXAttrCacheKey& key,
    ui64 cacheVersion,
    ui64 sourceId,
    TInstant deadline)
{
    if (!IsCacheable(shard, key, cacheVersion, sourceId)) {
        return;
    }

    shard.Cache.Add(
        key,
        TXAttr{.Value = Nothing(), .Version = 0},
        cacheVersion,
        sourceId,
        deadline);
}

void TXAttrCache::Add(
    ui64 ino,
    const TString& name,
    const TString& value,
    ui64 version,
    ui64 cacheVersion,
    ui64 sourceId,
    TDuration cacheTTL)
{
    const TXAttrCacheKey key{.NodeId = ino, .Name = name};
    const auto now = Timer->Now();
    const auto deadline = GetDeadline(now, cacheTTL);

    auto& shard = GetShard(ino);
    auto g = Guard(shard.Lock);

    AddValue(
        shard,
        key,
        value,
        version,
        cacheVersion,
        sourceId,
        now,
        deadline);
}

void TXAttrCache::AddAbsent(
    ui64 ino,
    const TString& name,
    ui64 cacheVersion,
    ui64 sourceId,
    TDuration cacheTTL)
{
    const TXAttrCacheKey key{.NodeId = ino, .Name = name};
    const auto deadline = GetDeadline(Timer->Now(), cacheTTL);

    auto& shard = GetShard(ino);
    auto g = Guard(shard.Lock);

    AddAbsentValue(shard, key, cacheVersion, sourceId, deadline);
}

void TXAttrCache::AddOwn(
    ui64 ino,
    const TString& name,
    const TMaybe<TString>& value,
    ui64 version,
    ui64 cacheVersion,
    ui64 invalidationVersion,
    ui64 sourceId,
    TDuration cacheTTL)
{
    const TXAttrCacheKey key{.NodeId = ino, .Name = name};
    const auto now = Timer->Now();
    const auto deadline = GetDeadline(now, cacheTTL);

    auto& shard = GetShard(ino);
    auto g = Guard(shard.Lock);

    if (!IsCacheable(shard, key, cacheVersion, sourceId)) {
        //
        // The erased value is not needed.
        //

        Y_UNUSED(shard.Cache.Invalidate(key, invalidationVersion));
        return;
    }

    if (value) {
        AddValue(
            shard,
            key,
            *value,
            version,
            cacheVersion,
            sourceId,
            now,
            deadline);
    } else {
        AddAbsentValue(shard, key, cacheVersion, sourceId, deadline);
    }

    //
    // The cached value is kept, the tombstone prevents only the caching of
    // the responses to the requests started before this point.
    //

    shard.Cache.AddTombstone(key, invalidationVersion);
}

TMaybe<TXAttr> TXAttrCache::Get(ui64 ino, const TString& name)
{
    const TXAttrCacheKey key{.NodeId = ino, .Name = name};
    const auto now = Timer->Now();

    auto& shard = GetShard(ino);
    auto g = Guard(shard.Lock);

    const auto* xattr = GetValid(shard, key, now);
    if (!xattr) {
        return Nothing();
    }

    return *xattr;
}

void TXAttrCache::Invalidate(ui64 ino, ui64 version)
{
    auto& shard = GetShard(ino);
    auto g = Guard(shard.Lock);

    shard.NodeInvalidations.Invalidate(ino, version);
}

void TXAttrCache::Invalidate(ui64 ino, const TString& name, ui64 version)
{
    const TXAttrCacheKey key{.NodeId = ino, .Name = name};

    auto& shard = GetShard(ino);
    auto g = Guard(shard.Lock);

    //
    // The erased value is not needed.
    //

    Y_UNUSED(shard.Cache.Invalidate(key, version));
}

void TXAttrCache::InvalidateSource(ui64 sourceId, ui64 version)
{
    for (auto& shard: Shards) {
        auto g = Guard(shard->Lock);

        shard->Cache.InvalidateSource(sourceId, version);
    }
}

size_t TXAttrCache::GetEntryCount()
{
    size_t count = 0;
    for (auto& shard: Shards) {
        auto g = Guard(shard->Lock);

        count += shard->Cache.GetEntryCount();
    }
    return count;
}

}   // namespace NCloud::NFileStore::NFuse
