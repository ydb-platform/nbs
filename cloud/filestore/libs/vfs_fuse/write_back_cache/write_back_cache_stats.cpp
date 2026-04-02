#include "write_back_cache_stats.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

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
