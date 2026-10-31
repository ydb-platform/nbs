#pragma once

#include "public.h"

#include "ttl_cache.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

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
 * along with a TTL (NodeRefCacheTTL in the GetNodeAttr responses), see
 * TTTLCacheShard.
 */
class TNodeRefCache
{
private:
    using TShard = TTTLCacheShard<TNodeRefCacheKey, ui64>;

    TVector<std::unique_ptr<TShard>> Shards;

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
     * @return - Target node id or InvalidNodeId.
     */
    ui64 Get(ui64 parentNodeId, const TString& name, TInstant now);

    /**
     * Erases a NodeRef and prevents caching it by the requests started before
     * version.
     *
     * @param parentNodeId - Parent node id.
     * @param name - Entry name.
     * @param version - Cache version of the invalidation.
     * @return - Target node id of the erased NodeRef or InvalidNodeId.
     */
    ui64 Invalidate(
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
    TShard& GetShard(ui64 parentNodeId);
};

}   // namespace NCloud::NFileStore::NFuse
