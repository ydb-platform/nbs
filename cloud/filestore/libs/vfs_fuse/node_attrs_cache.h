#pragma once

#include "public.h"

#include "ttl_cache.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

/*
 * Caches the node attrs returned by the backend along with a TTL
 * (NodeCacheTTL in the GetNodeAttr responses), see TTTLCacheShard. The
 * entries live until their deadline, invalidation or eviction.
 *
 * The invalidations of the node attrs are also used to decide whether the
 * guest may cache the node attrs returned to it, see IsCacheable. They are
 * tracked regardless of the TTLs, i.e. also when no node attrs are cached.
 */
class TNodeAttrsCache
{
private:
    using TShard = TTTLCacheShard<ui64, NProto::TNodeAttr>;

    TVector<std::unique_ptr<TShard>> Shards;

public:
    /**
     * @param maxEntryCount - Max number of the cached node attrs (and of the
     *  remembered invalidations), split evenly between the shards.
     * @param shardCount - Number of shards.
     */
    TNodeAttrsCache(ui32 maxEntryCount, ui32 shardCount);

    /**
     * Caches node attrs until deadline unless the node or the source has been
     * invalidated after version.
     *
     * @param attrs - Node attrs.
     * @param version - Cache version at which the request that has fetched
     *  the attrs has started.
     * @param sourceId - Source of the attrs (NodeCacheSourceId).
     * @param deadline - Deadline after which the attrs are not used.
     */
    void Add(
        const NProto::TNodeAttr& attrs,
        ui64 version,
        ui64 sourceId,
        TInstant deadline);

    /**
     * Checks whether the node attrs fetched by a request may be cached - by
     * this cache or by the guest.
     *
     * @param nodeId - Node id.
     * @param version - Cache version at which the request has started.
     * @return - False if the node has been invalidated after version.
     */
    bool IsCacheable(ui64 nodeId, ui64 version);

    /**
     * Returns the cached node attrs. The attrs are erased if their deadline
     * has passed.
     *
     * @param nodeId - Node id.
     * @param now - Current time.
     * @return - The attrs or Nothing.
     */
    TMaybe<NProto::TNodeAttr> Get(ui64 nodeId, TInstant now);

    /**
     * Erases the node attrs and prevents caching them by the requests started
     * before version.
     *
     * @param nodeId - Node id.
     * @param version - Cache version of the invalidation.
     */
    void Invalidate(ui64 nodeId, ui64 version);

    /**
     * Checks whether the node attrs fetched by a request may be cached (see
     * IsCacheable) and then invalidates them (see Invalidate), atomically.
     *
     * @param nodeId - Node id.
     * @param version - Cache version at which the request has started.
     * @param invalidationVersion - Cache version of the invalidation.
     * @return - False if the node has been invalidated after version.
     */
    bool CheckAndInvalidate(
        ui64 nodeId,
        ui64 version,
        ui64 invalidationVersion);

    /**
     * Invalidates all the node attrs received from a source (e.g. upon the
     * loss of the connection to it): the attrs fetched from it by the
     * requests started before version are neither cached nor returned.
     *
     * @param sourceId - Source of the attrs.
     * @param version - Cache version of the invalidation.
     */
    void InvalidateSource(ui64 sourceId, ui64 version);

    /**
     * Erases all the node attrs and prevents caching by the requests started
     * before version.
     *
     * @param version - Cache version of the invalidation.
     */
    void InvalidateAll(ui64 version);

    /**
     * @return - Number of the cached node attrs.
     */
    size_t GetEntryCount();

private:
    TShard& GetShard(ui64 nodeId);
};

}   // namespace NCloud::NFileStore::NFuse
