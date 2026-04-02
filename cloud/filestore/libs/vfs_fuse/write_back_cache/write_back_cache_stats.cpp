#include "write_back_cache_stats.h"

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
    std::atomic<i64> ReadDataCacheFullHit = 0;
    std::atomic<i64> ReadDataCachePartialHit = 0;
    std::atomic<i64> ReadDataCacheMiss = 0;

public:
    void AddReadDataStats(EReadDataRequestCacheStatus status) override
    {
        switch (status) {
            case EReadDataRequestCacheStatus::Miss:
                ReadDataCacheMiss.fetch_add(1, std::memory_order_release);
                break;
            case EReadDataRequestCacheStatus::PartialHit:
                ReadDataCachePartialHit.fetch_add(1, std::memory_order_release);
                break;
            case EReadDataRequestCacheStatus::FullHit:
                ReadDataCacheFullHit.fetch_add(1, std::memory_order_release);
                break;
        }
    }

    TWriteBackCacheInternalMetrics CreateInternalMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .ReadData = {
                .CacheFullHitCount = CreateMetric(
                    [self] { return self->ReadDataCacheFullHit.load(); }),
                .CachePartialHitCount = CreateMetric(
                    [self] { return self->ReadDataCachePartialHit.load(); }),
                .CacheMissCount = CreateMetric(
                    [self] { return self->ReadDataCacheMiss.load(); }),
            }};
    }

    void UpdateInternalStats() override
    {}
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

    virtual void Set(
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

    void UpdateInternalStats() override
    {}

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
