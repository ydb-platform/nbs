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
 * (NodeCacheTTL in the GetNodeAttr responses), see TTTLCacheShard. Unlike
 * TNodeCache, doesn't depend on the guest's lookup count: the entries live
 * until their deadline, invalidation or eviction.
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
     * Caches node attrs until deadline unless the node has been invalidated
     * after version.
     *
     * @param attrs - Node attrs.
     * @param version - Cache version at which the request that has fetched
     *  the attrs has started.
     * @param deadline - Deadline after which the attrs are not used.
     */
    void Add(const NProto::TNodeAttr& attrs, ui64 version, TInstant deadline);

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
