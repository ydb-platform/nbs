#include "write_back_cache_stats.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDummyWriteBackCacheStats
    : public IWriteBackCacheStats
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

    IWriteBackCacheInternalStatsPtr
    GetWriteBackCacheInternalStats() const override
    {
        return std::make_shared<TDummyWriteBackCacheStats>();
    }

    IWriteBackCacheStateStatsPtr GetWriteBackCacheStateStats() const override
    {
        return std::make_shared<TDummyWriteBackCacheStats>();
    }

    INodeStateHolderStatsPtr GetNodeStateHolderStats() const override
    {
        return std::make_shared<TDummyWriteBackCacheStats>();
    }

    IWriteDataRequestManagerStatsPtr
    GetWriteDataRequestManagerStats() const override
    {
        return std::make_shared<TDummyWriteBackCacheStats>();
    }

    IPersistentStorageStatsPtr GetPersistentStorageStats() const override
    {
        return std::make_shared<TDummyWriteBackCacheStats>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateDummyWriteBackCacheStats()
{
    return std::make_shared<TDummyWriteBackCacheStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
