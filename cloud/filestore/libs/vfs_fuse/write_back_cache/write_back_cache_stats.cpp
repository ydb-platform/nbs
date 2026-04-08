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

    void AddedPendingRequest() override
    {}

    void RemovedPendingRequest(TDuration duration) override
    {
        Y_UNUSED(duration);
    }

    void AddedUnflushedRequest() override
    {}

    void RemovedUnflushedRequest(TDuration duration) override
    {
        Y_UNUSED(duration);
    }

    void AddedFlushedRequest() override
    {}

    void RemovedFlushedRequest() override
    {}

    void AddReadDataStats(EReadDataRequestCacheStatus status) override
    {
        Y_UNUSED(status);
    }

    void SetPersistentStorageCounters(
        ui64 rawCapacityBytesCount,
        ui64 rawUsedBytesCount,
        ui64 entryCount,
        bool isCorrupted) override
    {
        Y_UNUSED(rawCapacityBytesCount);
        Y_UNUSED(rawUsedBytesCount);
        Y_UNUSED(entryCount);
        Y_UNUSED(isCorrupted);
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

    TWriteBackCacheStateMetrics
    CreateWriteBackCacheStateMetrics() const override
    {
        return {};
    }

    void UpdateWriteBackCacheStateStats() override
    {}

    INodeStateHolderStatsPtr GetNodeStateHolderStats() override
    {
        return shared_from_this();
    }

    TNodeStateHolderMetrics CreateNodeStateHolderMetrics() const override
    {
        return {};
    }

    void UpdateNodeStateHolderStats() override
    {}

    IWriteDataRequestManagerStatsPtr GetWriteDataRequestManagerStats() override
    {
        return shared_from_this();
    }

    TWriteDataRequestManagerMetrics
    CreateWriteDataRequestManagerMetrics() const override
    {
        return {};
    }

    void UpdateWriteDataRequestManagerStats(
        TDuration maxPendingRequestDuration,
        TDuration maxUnflushedRequestDuration) override
    {
        Y_UNUSED(maxPendingRequestDuration);
        Y_UNUSED(maxUnflushedRequestDuration);
    }

    IPersistentStorageStatsPtr GetPersistentStorageStats() override
    {
        return shared_from_this();
    }

    TPersistentStorageMetrics CreatePersistentStorageMetrics() const override
    {
        return {};
    }

    void UpdatePersistentStorageStats() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateDummyWriteBackCacheStats()
{
    return std::make_shared<TDummyWriteBackCacheStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
