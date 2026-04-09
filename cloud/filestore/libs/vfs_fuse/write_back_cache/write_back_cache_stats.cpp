#include "write_back_cache_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/diagnostics/module_stats.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheInternalStats
    : public std::enable_shared_from_this<TWriteBackCacheInternalStats>
    , public IWriteBackCacheInternalStats
{
private:
    TRelaxedCounter ReadDataCacheFullHit;
    TRelaxedCounter ReadDataCachePartialHit;
    TRelaxedCounter ReadDataCacheMiss;

public:
    void AddReadDataStats(EReadDataRequestCacheStatus status) override
    {
        switch (status) {
            case EReadDataRequestCacheStatus::Miss:
                ReadDataCacheMiss.Inc();
                break;
            case EReadDataRequestCacheStatus::PartialHit:
                ReadDataCachePartialHit.Inc();
                break;
            case EReadDataRequestCacheStatus::FullHit:
                ReadDataCacheFullHit.Inc();
                break;
        }
    }

    TWriteBackCacheInternalMetrics CreateMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .ReadData = {
                .CacheFullHitCount = CreateMetric(
                    [self] { return self->ReadDataCacheFullHit.Get(); }),
                .CachePartialHitCount = CreateMetric(
                    [self] { return self->ReadDataCachePartialHit.Get(); }),
                .CacheMissCount = CreateMetric(
                    [self] { return self->ReadDataCacheMiss.Get(); }),
            }};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheStats: public IWriteBackCacheStats
{
private:
    const IWriteBackCacheInternalStatsPtr WriteBackCacheInternalStats =
        std::make_shared<TWriteBackCacheInternalStats>();

    const IWriteBackCacheStateStatsPtr WriteBackCacheStateStats =
        CreateWriteBackCacheStateStats();

    const INodeStateHolderStatsPtr NodeStateHolderStats =
        CreateNodeStateHolderStats();

    const IWriteDataRequestManagerStatsPtr WriteDataRequestManagerStats =
        CreateWriteDataRequestManagerStats();

    const IPersistentStorageStatsPtr PersistentStorageStats =
        CreatePersistentStorageStats();

public:
    IWriteBackCacheInternalStatsPtr GetWriteBackCacheInternalStats() override
    {
        return WriteBackCacheInternalStats;
    }

    IWriteBackCacheStateStatsPtr GetWriteBackCacheStateStats() override
    {
        return WriteBackCacheStateStats;
    }

    INodeStateHolderStatsPtr GetNodeStateHolderStats() override
    {
        return NodeStateHolderStats;
    }

    IWriteDataRequestManagerStatsPtr GetWriteDataRequestManagerStats() override
    {
        return WriteDataRequestManagerStats;
    }

    IPersistentStorageStatsPtr GetPersistentStorageStats() override
    {
        return PersistentStorageStats;
    }

    TWriteBackCacheMetrics CreateMetrics() const override
    {
        return {
            WriteBackCacheInternalStats->CreateMetrics(),
            WriteBackCacheStateStats->CreateMetrics(),
            NodeStateHolderStats->CreateMetrics(),
            WriteDataRequestManagerStats->CreateMetrics(),
            PersistentStorageStats->CreateMetrics()};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheModuleStats: public IModuleStats
{
private:
    const IWriteBackCacheStatsPtr Stats;
    const std::function<void(TInstant now)> UpdateStatsFunc;

public:
    TWriteBackCacheModuleStats(
        IWriteBackCacheStatsPtr stats,
        std::function<void(TInstant now)> updateStatsFunc)
        : Stats(std::move(stats))
        , UpdateStatsFunc(std::move(updateStatsFunc))
    {}

    TStringBuf GetName() const override
    {
        return "WriteBackCache";
    }

    void RegisterCounters(
        IMetricsRegistry& localMetricsRegistry,
        IMetricsRegistry& aggregatableMetricsRegistry) override
    {
        // Local metrics can be aggregated when two WriteBackCache instances are
        // running for the same FileSystemId/ClientId pair (migration scenario)

        Stats->CreateMetrics().Register(
            localMetricsRegistry,
            aggregatableMetricsRegistry);
    }

    void UpdateStats(TInstant now) override
    {
        UpdateStatsFunc(now);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TWriteBackCacheInternalMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    Y_UNUSED(localMetricsRegistry);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("ReadData_CacheHit")},
        ReadData.CacheFullHitCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("ReadData_CachePartialHit")},
        ReadData.CachePartialHitCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("ReadData_CacheMiss")},
        ReadData.CacheMissCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteBackCacheMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    static_cast<const TWriteBackCacheInternalMetrics*>(this)->Register(
        localMetricsRegistry,
        aggregatableMetricsRegistry);

    static_cast<const TWriteBackCacheStateMetrics*>(this)->Register(
        localMetricsRegistry,
        aggregatableMetricsRegistry);

    static_cast<const TNodeStateHolderMetrics*>(this)->Register(
        localMetricsRegistry,
        aggregatableMetricsRegistry);

    static_cast<const TWriteDataRequestManagerMetrics*>(this)->Register(
        localMetricsRegistry,
        aggregatableMetricsRegistry);

    static_cast<const TPersistentStorageMetrics*>(this)->Register(
        localMetricsRegistry,
        aggregatableMetricsRegistry);
}

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateWriteBackCacheStats()
{
    return std::make_shared<TWriteBackCacheStats>();
}

IModuleStatsPtr CreateWriteBackCacheModuleStats(
    IWriteBackCacheStatsPtr stats,
    std::function<void(TInstant now)> updateStatsFunc)
{
    return std::make_shared<TWriteBackCacheModuleStats>(
        std::move(stats),
        std::move(updateStatsFunc));
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
