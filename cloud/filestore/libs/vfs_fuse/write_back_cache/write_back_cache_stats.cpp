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

    TWriteBackCacheInternalMetrics CreateInternalMetrics() const override
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

class TDummyWriteBackCacheStats
    : public std::enable_shared_from_this<TDummyWriteBackCacheStats>
    , public IWriteBackCacheStats
    , public IWriteBackCacheInternalStats
    , public IWriteBackCacheStateStats
    , public INodeStateHolderStats
    , public IWriteDataRequestManagerStats
    , public IPersistentStorageStats
{
public:
    void ResetNonDerivativeCounters() override
    {}

    void FlushStarted() override
    {}

    void FlushCompleted() override
    {}

    void FlushFailed() override
    {}

    void IncrementNodeCount() override
    {}

    void DecrementNodeCount() override
    {}

    void WriteDataRequestDropped() override
    {}

    void WriteDataRequestEnteredStatus(EWriteDataRequestStatus status) override
    {
        Y_UNUSED(status);
    }

    void WriteDataRequestExitedStatus(
        EWriteDataRequestStatus status,
        TDuration duration) override
    {
        Y_UNUSED(status);
        Y_UNUSED(duration);
    }

    void WriteDataRequestUpdateMinTime(
        EWriteDataRequestStatus status,
        TInstant minTime) override
    {
        Y_UNUSED(status);
        Y_UNUSED(minTime);
    }

    void AddReadDataStats(EReadDataRequestCacheStatus status) override
    {
        Y_UNUSED(status);
    }

    void UpdatePersistentStorageStats(
        const TPersistentStorageStats& stats) override
    {
        Y_UNUSED(stats);
    }

    IWriteBackCacheInternalStatsPtr GetWriteBackCacheInternalStats() override
    {
        return shared_from_this();
    }

    TWriteBackCacheInternalMetrics CreateInternalMetrics() const override
    {
        return {};
    }

    IWriteBackCacheStateStatsPtr GetWriteBackCacheStateStats() override
    {
        return shared_from_this();
    }

    INodeStateHolderStatsPtr GetNodeStateHolderStats() override
    {
        return shared_from_this();
    }

    IWriteDataRequestManagerStatsPtr GetWriteDataRequestManagerStats() override
    {
        return shared_from_this();
    }

    IPersistentStorageStatsPtr GetPersistentStorageStats() override
    {
        return shared_from_this();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateDummyWriteBackCacheStats()
{
    return std::make_shared<TDummyWriteBackCacheStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
