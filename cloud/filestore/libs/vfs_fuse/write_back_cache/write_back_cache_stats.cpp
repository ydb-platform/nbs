#include "write_back_cache_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/metric.h>

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateWriteBackCacheStats()
{
    return std::make_shared<TWriteBackCacheStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
