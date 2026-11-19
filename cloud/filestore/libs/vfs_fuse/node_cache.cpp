#include "node_cache.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

bool TXAttrCache::IsCacheable(
    ui64 ino,
    const TString& name,
    ui64 cacheVersion,
    ui64 sourceId) const
{
    return Invalidations.IsCacheable(
               TXAttrCacheKey{.NodeId = ino, .Name = name},
               cacheVersion) &&
           Invalidations.IsSourceValid(sourceId, cacheVersion) &&
           NodeInvalidations.IsCacheable(ino, cacheVersion);
}

void TXAttrCache::Erase(ui64 ino, const TString& name)
{
    auto it = Cache.Find({ino, name});
    if (it != Cache.End()) {
        Cache.Erase(it);
    }
}

TInstant TXAttrCache::GetDeadline(TDuration cacheTTL) const
{
    return Timer->Now() + (cacheTTL ? cacheTTL : Timeout);
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
    if (!IsCacheable(ino, name, cacheVersion, sourceId)) {
        return;
    }

    const auto* current = Get(ino, name);
    if (!current || current->Version < version) {
        Cache.Update(
            TKey{ino, name},
            TXAttr{
                .Name = name,
                .Value = value,
                .Version = version,
                .CacheVersion = cacheVersion,
                .SourceId = sourceId,
                .Deadline = GetDeadline(cacheTTL)
            }
        );
    }
}

void TXAttrCache::AddAbsent(
    ui64 ino,
    const TString& name,
    ui64 cacheVersion,
    ui64 sourceId,
    TDuration cacheTTL)
{
    if (!IsCacheable(ino, name, cacheVersion, sourceId)) {
        return;
    }

    Cache.Update(
        TKey{ino, name},
        TXAttr{
            .Name = name,
            .Value = Nothing(),
            .Version = 0,
            .CacheVersion = cacheVersion,
            .SourceId = sourceId,
            .Deadline = GetDeadline(cacheTTL)
        }
    );
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
    if (!IsCacheable(ino, name, cacheVersion, sourceId)) {
        Invalidate(ino, name, invalidationVersion);
        return;
    }

    if (value) {
        Add(ino, name, *value, version, cacheVersion, sourceId, cacheTTL);
    } else {
        AddAbsent(ino, name, cacheVersion, sourceId, cacheTTL);
    }

    //
    // The cached value is kept, the tombstone prevents only the caching of
    // the responses to the requests started before this point.
    //

    Invalidations.Invalidate(
        TXAttrCacheKey{.NodeId = ino, .Name = name},
        invalidationVersion);
}

const TXAttr* TXAttrCache::Get(ui64 ino, const TString& name)
{
    auto it = Cache.Find({ino, name});
    if (it == Cache.End()) {
        return nullptr;
    }

    //
    // The node-wide and the source invalidations don't erase the entries -
    // the entries cached before such an invalidation are dropped here.
    //

    const auto& xattr = it.Value();
    if (Timer->Now() >= xattr.Deadline ||
        !NodeInvalidations.IsCacheable(ino, xattr.CacheVersion) ||
        !Invalidations.IsSourceValid(xattr.SourceId, xattr.CacheVersion))
    {
        Cache.Erase(it);
        return nullptr;
    }

    return &xattr;
}

void TXAttrCache::Invalidate(ui64 ino, ui64 version)
{
    NodeInvalidations.Invalidate(ino, version);
}

void TXAttrCache::Invalidate(ui64 ino, const TString& name, ui64 version)
{
    Invalidations.Invalidate(
        TXAttrCacheKey{.NodeId = ino, .Name = name},
        version);

    Erase(ino, name);
}

size_t TXAttrCache::GetEntryCount() const
{
    return Cache.Size();
}

void TXAttrCache::InvalidateSource(ui64 sourceId, ui64 version)
{
    Invalidations.InvalidateSource(sourceId, version);
}

void TXAttrCache::InvalidateAll(ui64 version)
{
    Invalidations.InvalidateAll(version);
    Cache.Clear();
}

}   // namespace NCloud::NFileStore::NFuse
