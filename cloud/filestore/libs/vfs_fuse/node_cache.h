#pragma once

#include "public.h"

#include "ttl_cache.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/cache/cache.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

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
    TString Name;
    TMaybe<TString> Value;
    ui64 Version;
    // Cache version at which the request that has fetched the entry started.
    ui64 CacheVersion;
    ui64 SourceId;
    TInstant Deadline;
};

/*
 * Caches the xattr values and absences for XAttrCacheTTL returned by the
 * backend or, if it is zero (FileSystemEvents are not supported), for
 * XAttrCacheTimeout. The xattrs are invalidated upon the local changes and
 * upon the FileSystemEvents, all the xattrs of a node are invalidated upon
 * the node id reuse. The values fetched by the requests started before an
 * invalidation are not cached, see TInvalidations.
 *
 * Not thread-safe.
 */
class TXAttrCache
{
private:
    struct TWeighter {
        static TInstant Weight(const TXAttr& value)
        {
            return value.Deadline;
        }
    };
    using TKey = std::pair<ui64, TString>;
    using TCache = TLWCache<TKey, TXAttr, TInstant, TWeighter>;

private:
    ITimerPtr Timer;
    TCache Cache;
    TDuration Timeout;
    TInvalidations<TXAttrCacheKey> Invalidations;
    TInvalidations<ui64> NodeInvalidations;

public:
    TXAttrCache(
            ITimerPtr timer,
            ui32 maxSize,
            TDuration timeout)
        : Timer{std::move(timer)}
        , Cache{maxSize}
        , Timeout{timeout}
        , Invalidations{maxSize}
        , NodeInvalidations{maxSize}
    {}

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
     * @return - The cached entry or nullptr if there is no valid entry.
     */
    const TXAttr* Get(ui64 ino, const TString& name);

    /**
     * Invalidates all the cached xattrs of a node (e.g. upon the node id
     * reuse) and prevents caching them by the requests started before
     * version.
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
     * Erases all the cached xattrs and prevents caching by the requests
     * started before version.
     *
     * @param version - Cache version of the invalidation.
     */
    void InvalidateAll(ui64 version);

    /**
     * @return - Number of the cached xattrs (values and absences).
     */
    size_t GetEntryCount() const;

private:
    bool IsCacheable(
        ui64 ino,
        const TString& name,
        ui64 cacheVersion,
        ui64 sourceId) const;
    void Erase(ui64 ino, const TString& name);
    TInstant GetDeadline(TDuration cacheTTL) const;
};

}   // namespace NCloud::NFileStore::NFuse
