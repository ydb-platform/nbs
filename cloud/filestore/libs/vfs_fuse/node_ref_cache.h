#pragma once

#include "public.h"

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TNodeRefCacheKey
{
    ui64 ParentNodeId = 0;
    TString Name;

    bool operator==(const TNodeRefCacheKey& rhs) const = default;
};

}   // namespace NCloud::NFileStore::NFuse

template <>
struct THash<NCloud::NFileStore::NFuse::TNodeRefCacheKey>
{
    size_t operator()(
        const NCloud::NFileStore::NFuse::TNodeRefCacheKey& key) const
    {
        return MultiHash(key.ParentNodeId, key.Name);
    }
};

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

/*
 * Caches the NodeRefs (ParentNodeId, Name) -> NodeId returned by the backend
 * along with a TTL (NodeRefCacheTTL in the GetNodeAttr responses). The backend
 * sends an invalidation event upon a change of a NodeRef, the local changes
 * invalidate the NodeRefs directly.
 *
 * The NodeRefs fetched by the requests started before an invalidation of the
 * same NodeRef are not cached - the invalidations are remembered as
 * tombstones with the cache version of the invalidation. The number of the
 * tombstones is limited: upon eviction of a tombstone its version raises the
 * watermark below which nothing is cached.
 */
class TNodeRefCacheShard
{
private:
    struct TEntry
    {
        ui64 NodeId = 0;
        TInstant Deadline;
    };

    struct TTombstone
    {
        TNodeRefCacheKey Key;
        ui64 Version = 0;
    };

    TLRUCache<TNodeRefCacheKey, TEntry> Entries;
    THashMap<TNodeRefCacheKey, ui64> Tombstones;
    TDeque<TTombstone> TombstoneQueue;
    const ui32 MaxTombstoneCount;
    ui64 Watermark = 0;
    TAdaptiveLock Lock;

public:
    explicit TNodeRefCacheShard(ui32 maxEntryCount);

    void Add(
        const TNodeRefCacheKey& key,
        ui64 nodeId,
        ui64 version,
        TInstant deadline);
    TMaybe<ui64> Get(const TNodeRefCacheKey& key, TInstant now);
    TMaybe<ui64> Invalidate(const TNodeRefCacheKey& key, ui64 version);
    void InvalidateAll(ui64 version);
    size_t GetEntryCount();

private:
    void AddTombstone(const TNodeRefCacheKey& key, ui64 version);
};

////////////////////////////////////////////////////////////////////////////////

class TNodeRefCache
{
private:
    TVector<std::unique_ptr<TNodeRefCacheShard>> Shards;

public:
    /**
     * @param maxEntryCount - Max number of the cached NodeRefs (and of the
     *  remembered invalidations), split evenly between the shards.
     * @param shardCount - Number of shards.
     */
    TNodeRefCache(ui32 maxEntryCount, ui32 shardCount);

    /**
     * Caches a NodeRef until deadline unless the NodeRef has been invalidated
     * after version.
     *
     * @param parentNodeId - Parent node id.
     * @param name - Entry name.
     * @param nodeId - Target node id.
     * @param version - Cache version at which the request that has fetched
     *  the NodeRef has started.
     * @param deadline - Deadline after which the NodeRef is not used.
     */
    void Add(
        ui64 parentNodeId,
        const TString& name,
        ui64 nodeId,
        ui64 version,
        TInstant deadline);

    /**
     * Returns the target node id of a cached NodeRef. The NodeRef is erased
     * if its deadline has passed.
     *
     * @param parentNodeId - Parent node id.
     * @param name - Entry name.
     * @param now - Current time.
     * @return - Target node id or Nothing.
     */
    TMaybe<ui64> Get(ui64 parentNodeId, const TString& name, TInstant now);

    /**
     * Erases a NodeRef and prevents caching it by the requests started before
     * version.
     *
     * @param parentNodeId - Parent node id.
     * @param name - Entry name.
     * @param version - Cache version of the invalidation.
     * @return - Target node id of the erased NodeRef or Nothing.
     */
    TMaybe<ui64> Invalidate(
        ui64 parentNodeId,
        const TString& name,
        ui64 version);

    /**
     * Erases all the NodeRefs and prevents caching by the requests started
     * before version.
     *
     * @param version - Cache version of the invalidation.
     */
    void InvalidateAll(ui64 version);

    /**
     * @return - Number of the cached NodeRefs.
     */
    size_t GetEntryCount();

private:
    TNodeRefCacheShard& GetShard(ui64 parentNodeId);
};

}   // namespace NCloud::NFileStore::NFuse
