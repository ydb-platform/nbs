#include "metadata_cache_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TStringBuf TMetadataCacheModuleStats::GetName() const
{
    return "MetadataCache";
}

void TMetadataCacheModuleStats::RegisterCounters(
    const NMetrics::IMetricsRegistryPtr& localMetricsRegistry,
    const NMetrics::IMetricsRegistryPtr& aggregatableMetricsRegistry)
{
    Y_UNUSED(aggregatableMetricsRegistry);

    auto self = shared_from_this();
    auto registerCounter = [&] (
        const TString& name,
        const TRelaxedCounter& counter,
        NMetrics::EMetricType type)
    {
        localMetricsRegistry->Register(
            {NMetrics::CreateSensor(name)},
            NMetrics::CreateMetric([self, &counter] { return counter.Get(); }),
            NMetrics::EAggregationType::AT_SUM,
            type);
    };

    const auto derivative = NMetrics::EMetricType::MT_DERIVATIVE;
    registerCounter("LookupCacheHitCount", LookupCacheHitCount, derivative);
    registerCounter("LookupCacheMissCount", LookupCacheMissCount, derivative);
    registerCounter("GetAttrCacheHitCount", GetAttrCacheHitCount, derivative);
    registerCounter(
        "GetAttrCacheMissCount",
        GetAttrCacheMissCount,
        derivative);
    registerCounter(
        "GetXAttrCacheHitCount",
        GetXAttrCacheHitCount,
        derivative);
    registerCounter(
        "GetXAttrCacheMissCount",
        GetXAttrCacheMissCount,
        derivative);
    registerCounter(
        "InvalidateNodeEventCount",
        InvalidateNodeEventCount,
        derivative);
    registerCounter(
        "InvalidateNodeRefEventCount",
        InvalidateNodeRefEventCount,
        derivative);
    registerCounter(
        "InvalidateNodeXAttrEventCount",
        InvalidateNodeXAttrEventCount,
        derivative);
    registerCounter("InvalidateAllCount", InvalidateAllCount, derivative);

    const auto absolute = NMetrics::EMetricType::MT_ABSOLUTE;
    registerCounter("NodeAttrsCacheSize", NodeAttrsCacheSize, absolute);
    registerCounter("NodeRefCacheSize", NodeRefCacheSize, absolute);
    registerCounter("XAttrCacheSize", XAttrCacheSize, absolute);
}

void TMetadataCacheModuleStats::UpdateStats(TInstant now)
{
    Y_UNUSED(now);

    TMetadataCacheSizeProvider sizeProvider;
    with_lock (SizeProviderLock) {
        sizeProvider = SizeProvider;
    }

    if (!sizeProvider) {
        return;
    }

    const auto sizes = sizeProvider();
    NodeAttrsCacheSize.Set(sizes.NodeAttrsCacheSize);
    NodeRefCacheSize.Set(sizes.NodeRefCacheSize);
    XAttrCacheSize.Set(sizes.XAttrCacheSize);
}

void TMetadataCacheModuleStats::SetSizeProvider(
    TMetadataCacheSizeProvider sizeProvider)
{
    with_lock (SizeProviderLock) {
        SizeProvider = std::move(sizeProvider);
    }
}

void TMetadataCacheModuleStats::CountLookup(bool hit)
{
    (hit ? LookupCacheHitCount : LookupCacheMissCount).Inc();
}

void TMetadataCacheModuleStats::CountGetAttr(bool hit)
{
    (hit ? GetAttrCacheHitCount : GetAttrCacheMissCount).Inc();
}

void TMetadataCacheModuleStats::CountGetXAttr(bool hit)
{
    (hit ? GetXAttrCacheHitCount : GetXAttrCacheMissCount).Inc();
}

void TMetadataCacheModuleStats::IncrementInvalidateNodeEventCount()
{
    InvalidateNodeEventCount.Inc();
}

void TMetadataCacheModuleStats::IncrementInvalidateNodeRefEventCount()
{
    InvalidateNodeRefEventCount.Inc();
}

void TMetadataCacheModuleStats::IncrementInvalidateNodeXAttrEventCount()
{
    InvalidateNodeXAttrEventCount.Inc();
}

void TMetadataCacheModuleStats::IncrementInvalidateAllCount()
{
    InvalidateAllCount.Inc();
}

////////////////////////////////////////////////////////////////////////////////

TMetadataCacheModuleStatsPtr CreateMetadataCacheModuleStats()
{
    return std::make_shared<TMetadataCacheModuleStats>();
}

}   // namespace NCloud::NFileStore::NFuse
