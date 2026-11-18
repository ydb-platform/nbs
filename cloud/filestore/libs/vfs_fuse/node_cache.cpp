#include "node_cache.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/request.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

bool TNodeCacheShard::UpdateNode(
    const NProto::TNodeAttr& attrs,
    ui64 version)
{
    auto g = Guard(Lock);

    auto [it, inserted] = Id2Node.emplace(attrs.GetId(), TNode(attrs));
    auto& node = it->second;
    bool updated = false;
    if (inserted) {
        node.LastUpdateVersion = version;
        updated = true;
    } else {
        if (version >= node.LastUpdateVersion) {
            node.UpdateAttrs(attrs, version);

            if (!node.IsValid()) {
                ReportNodeCacheInvalidNode(TStringBuilder()
                    << "fs: " << FileSystemId
                    << ", attrs: " << attrs.ShortUtf8DebugString()
                    << ", version: " << version);
            }

            updated = true;
        }

        node.Ref();
    }

    return updated;
}

void TNodeCacheShard::InvalidateNode(ui64 ino, ui64 version)
{
    auto g = Guard(Lock);

    NProto::TNodeAttr attrs;
    attrs.SetId(ino);
    auto [it, inserted] = Id2Node.emplace(ino, TNode(attrs));
    if (version >= it->second.LastUpdateVersion) {
        if (!inserted) {
            it->second.Attrs = std::move(attrs);
        }

        it->second.LastUpdateVersion = version;
    }
}

void TNodeCacheShard::ForgetNode(ui64 ino, size_t count)
{
    auto g = Guard(Lock);

    auto it = Id2Node.find(ino);
    if (it == Id2Node.end()) {
        // we lose our cache after restart, so we should expect forget requests
        // targeting nodes that are absent from our cache
        // see NBS-2102

        return;
    }

    count = it->second.UnRef(count);
    if (count == 0) {
        // do not pass element itself
        Id2Node.erase(it);
    }
}

ui64 TNodeCacheShard::GetNodeVersion(ui64 ino) const
{
    auto g = Guard(Lock);

    const auto* node = Id2Node.FindPtr(ino);
    return node ? node->LastUpdateVersion : 0;
}

////////////////////////////////////////////////////////////////////////////////

bool TXAttrCache::IsCacheable(
    ui64 ino,
    const TString& name,
    ui64 cacheVersion) const
{
    return Invalidations.IsCacheable(
               TXAttrCacheKey{.NodeId = ino, .Name = name},
               cacheVersion) &&
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
    TDuration cacheTTL)
{
    if (!IsCacheable(ino, name, cacheVersion)) {
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
                .Deadline = GetDeadline(cacheTTL)
            }
        );
    }
}

void TXAttrCache::AddAbsent(
    ui64 ino,
    const TString& name,
    ui64 cacheVersion,
    TDuration cacheTTL)
{
    if (!IsCacheable(ino, name, cacheVersion)) {
        return;
    }

    Cache.Update(
        TKey{ino, name},
        TXAttr{
            .Name = name,
            .Value = Nothing(),
            .Version = 0,
            .CacheVersion = cacheVersion,
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
    TDuration cacheTTL)
{
    if (!IsCacheable(ino, name, cacheVersion)) {
        Invalidate(ino, name, invalidationVersion);
        return;
    }

    if (value) {
        Add(ino, name, *value, version, cacheVersion, cacheTTL);
    } else {
        AddAbsent(ino, name, cacheVersion, cacheTTL);
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
    // The node-wide invalidations don't erase the entries of the node - the
    // entries cached before such an invalidation are dropped here.
    //

    const auto& xattr = it.Value();
    if (Timer->Now() >= xattr.Deadline ||
        !NodeInvalidations.IsCacheable(ino, xattr.CacheVersion))
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

void TXAttrCache::InvalidateAll(ui64 version)
{
    Invalidations.InvalidateAll(version);
    Cache.Clear();
}

}   // namespace NCloud::NFileStore::NFuse
