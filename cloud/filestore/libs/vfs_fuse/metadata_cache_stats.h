#pragma once

#include <cloud/filestore/libs/diagnostics/module_stats.h>
#include <cloud/filestore/libs/vfs_fuse/counters/relaxed_counters.h>

#include <util/system/spinlock.h>

#include <functional>
#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TMetadataCacheSizes
{
    ui64 NodeAttrsCacheSize = 0;
    ui64 NodeRefCacheSize = 0;
    ui64 XAttrCacheSize = 0;
};

using TMetadataCacheSizeProvider = std::function<TMetadataCacheSizes()>;

////////////////////////////////////////////////////////////////////////////////

//
// The counters of the metadata caches (node attrs, NodeRefs, xattrs): the
// cache hits and misses of the requests, the invalidation events received
// from the backend and the cache sizes.
//

class TMetadataCacheModuleStats final
    : public std::enable_shared_from_this<TMetadataCacheModuleStats>
    , public IModuleStats
{
private:
    TRelaxedCounter LookupCacheHitCount;
    TRelaxedCounter LookupCacheMissCount;
    TRelaxedCounter GetAttrCacheHitCount;
    TRelaxedCounter GetAttrCacheMissCount;
    TRelaxedCounter GetXAttrCacheHitCount;
    TRelaxedCounter GetXAttrCacheMissCount;

    TRelaxedCounter InvalidateNodeEventCount;
    TRelaxedCounter InvalidateNodeRefEventCount;
    TRelaxedCounter InvalidateNodeXAttrEventCount;
    TRelaxedCounter InvalidateAllCount;

    TRelaxedCounter NodeAttrsCacheSize;
    TRelaxedCounter NodeRefCacheSize;
    TRelaxedCounter XAttrCacheSize;

    TMetadataCacheSizeProvider SizeProvider;
    TAdaptiveLock SizeProviderLock;

public:
    TStringBuf GetName() const override;

    void RegisterCounters(
        const NMetrics::IMetricsRegistryPtr& localMetricsRegistry,
        const NMetrics::IMetricsRegistryPtr& aggregatableMetricsRegistry)
        override;

    void UpdateStats(TInstant now) override;

    /**
     * Sets the function that reports the cache sizes upon UpdateStats.
     *
     * @param sizeProvider - The function, called from UpdateStats.
     */
    void SetSizeProvider(TMetadataCacheSizeProvider sizeProvider);

    /**
     * Counts a Lookup request served from the caches or not.
     *
     * @param hit - True if the request has been served from the caches.
     */
    void CountLookup(bool hit);

    /**
     * Counts a GetAttr request served from the cache or not.
     *
     * @param hit - True if the request has been served from the cache.
     */
    void CountGetAttr(bool hit);

    /**
     * Counts a GetXAttr request served from the cache or not.
     *
     * @param hit - True if the request has been served from the cache.
     */
    void CountGetXAttr(bool hit);

    void IncrementInvalidateNodeEventCount();
    void IncrementInvalidateNodeRefEventCount();
    void IncrementInvalidateNodeXAttrEventCount();
    void IncrementInvalidateAllCount();
};

using TMetadataCacheModuleStatsPtr =
    std::shared_ptr<TMetadataCacheModuleStats>;

////////////////////////////////////////////////////////////////////////////////

TMetadataCacheModuleStatsPtr CreateMetadataCacheModuleStats();

}   // namespace NCloud::NFileStore::NFuse
