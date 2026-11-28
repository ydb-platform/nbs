#pragma once

#include "public.h"

#include "ttl_cache.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TXAttrCacheKey
{
    ui64 NodeId = 0;
    TString Name;

    bool operator==(const TXAttrCacheKey& rhs) const = default;
};

}   // namespace NCloud::NFileStore::NFuse

template <>
struct THash<NCloud::NFileStore::NFuse::TXAttrCacheKey>
{
    size_t operator()(
        const NCloud::NFileStore::NFuse::TXAttrCacheKey& key) const
    {
        return MultiHash(key.NodeId, key.Name);
    }
};

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TXAttr
{
    // Xattr value, Nothing() for the absence of the xattr.
    TMaybe<TString> Value;
    // Xattr version returned by the backend, zero for the absence.
    ui64 Version = 0;
};

/*
 * Caches the xattr values and absences for XAttrCacheTTL returned by the
 * backend or, if it is zero (FileSystemEvents are not supported), for
 * XAttrCacheTimeout, see TTTLCache. The xattrs are invalidated upon the local
 * changes and upon the FileSystemEvents, all the xattrs of a node are
 * invalidated upon the node id reuse. The values fetched by the requests
 * started before an invalidation are not cached, see TInvalidations.
 *
 * The xattrs are sharded by node id, each shard has its own lock.
 */
class TXAttrCache
{
private:
    struct TShard
    {
        TTTLCache<TXAttrCacheKey, TXAttr> Cache;
        // Invalidations of all the xattrs of a node, see
        // Invalidate(ino, version).
        TInvalidations<ui64> NodeInvalidations;
        TAdaptiveLock Lock;

        explicit TShard(ui32 maxEntryCount)
            : Cache(maxEntryCount)
            , NodeInvalidations(maxEntryCount)
        {}
    };

    const ITimerPtr Timer;
    const TDuration Timeout;
    TVector<std::unique_ptr<TShard>> Shards;

public:
    /**
     * @param timer - Timer.
     * @param maxEntryCount - Max number of the cached xattrs (and of the
     *  remembered invalidations), split evenly between the shards.
     * @param shardCount - Number of shards.
     * @param timeout - XAttrCacheTimeout, used if the backend returns zero
     *  XAttrCacheTTL.
     */
    TXAttrCache(
        ITimerPtr timer,
        ui32 maxEntryCount,
        ui32 shardCount,
        TDuration timeout);

    /**
     * Caches an xattr value unless the xattr has been invalidated after
     * cacheVersion or a newer value is cached.
     *
     * @param ino - Node id.
     * @param name - Xattr name.
     * @param value - Xattr value.
     * @param version - Xattr version returned by the backend.
     * @param cacheVersion - Cache version at which the request that has
     *  fetched the value has started.
     * @param sourceId - Source of the data (XAttrCacheSourceId).
     * @param cacheTTL - XAttrCacheTTL returned by the backend. Zero means
     *  that XAttrCacheTimeout is used.
     */
    void Add(
        ui64 ino,
        const TString& name,
        const TString& value,
        ui64 version,
        ui64 cacheVersion,
        ui64 sourceId,
        TDuration cacheTTL);

    /**
     * Caches the absence of an xattr unless the xattr has been invalidated
     * after cacheVersion.
     *
     * @param ino - Node id.
     * @param name - Xattr name.
     * @param cacheVersion - Cache version at which the request that has
     *  found out the absence has started.
     * @param sourceId - Source of the data (XAttrCacheSourceId).
     * @param cacheTTL - XAttrCacheTTL returned by the backend. Zero means
     *  that XAttrCacheTimeout is used.
     */
    void AddAbsent(
        ui64 ino,
        const TString& name,
        ui64 cacheVersion,
        ui64 sourceId,
        TDuration cacheTTL);

    /**
     * Caches the value set by the client itself or the absence found out by
     * the client's own SetNodeXAttr with F_REPLACE (if it may be cached, see
     * Add and AddAbsent) and prevents caching the responses to the requests
     * started before invalidationVersion: they may have been processed by
     * the backend before the SetNodeXAttr and contain an older value or
     * absence. The absences carry no version, so they can be ordered against
     * the values only this way.
     *
     * @param ino - Node id.
     * @param name - Xattr name.
     * @param value - Xattr value, Nothing() for the absence.
     * @param version - Xattr version returned by the backend, zero for the
     *  absence.
     * @param cacheVersion - Cache version at which the SetNodeXAttr has
     *  started.
     * @param invalidationVersion - Cache version generated upon the response
     *  arrival.
     * @param sourceId - Source of the data (XAttrCacheSourceId).
     * @param cacheTTL - XAttrCacheTTL returned by the backend. Zero means
     *  that XAttrCacheTimeout is used.
     */
    void AddOwn(
        ui64 ino,
        const TString& name,
        const TMaybe<TString>& value,
        ui64 version,
        ui64 cacheVersion,
        ui64 invalidationVersion,
        ui64 sourceId,
        TDuration cacheTTL);

    /**
     * Looks up a cached xattr value or absence.
     *
     * @param ino - Node id.
     * @param name - Xattr name.
     * @return - The cached xattr or Nothing() if there is no valid entry.
     */
    TMaybe<TXAttr> Get(ui64 ino, const TString& name);

    /**
     * Invalidates all the cached xattrs of a node (e.g. upon the node id
     * reuse) and prevents caching them by the requests started before
     * version. The entries are erased lazily, upon access or eviction.
     *
     * @param ino - Node id.
     * @param version - Cache version of the invalidation.
     */
    void Invalidate(ui64 ino, ui64 version);

    /**
     * Erases a cached xattr (value or absence) and prevents caching it by
     * the requests started before version.
     *
     * @param ino - Node id.
     * @param name - Xattr name.
     * @param version - Cache version of the invalidation.
     */
    void Invalidate(ui64 ino, const TString& name, ui64 version);

    /**
     * Invalidates all the xattrs received from a source (e.g. upon the loss
     * of the connection to it): the xattrs fetched from it by the requests
     * started before version are neither cached nor returned.
     *
     * @param sourceId - Source of the xattrs.
     * @param version - Cache version of the invalidation.
     */
    void InvalidateSource(ui64 sourceId, ui64 version);

    /**
     * @return - Number of the cached xattrs (values and absences).
     */
    size_t GetEntryCount();

private:
    TShard& GetShard(ui64 ino);
    TInstant GetDeadline(TInstant now, TDuration cacheTTL) const;

    static bool IsCacheable(
        const TShard& shard,
        const TXAttrCacheKey& key,
        ui64 cacheVersion,
        ui64 sourceId);
    static const TXAttr* GetValid(
        TShard& shard,
        const TXAttrCacheKey& key,
        TInstant now);
    static void AddValue(
        TShard& shard,
        const TXAttrCacheKey& key,
        const TString& value,
        ui64 version,
        ui64 cacheVersion,
        ui64 sourceId,
        TInstant now,
        TInstant deadline);
    static void AddAbsentValue(
        TShard& shard,
        const TXAttrCacheKey& key,
        ui64 cacheVersion,
        ui64 sourceId,
        TInstant deadline);
};

}   // namespace NCloud::NFileStore::NFuse
