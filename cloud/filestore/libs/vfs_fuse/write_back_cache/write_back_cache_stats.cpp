#include "write_back_cache_stats.h"

#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <atomic>

namespace NCloud::NFileStore::NFuse {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TValue = TDynamicCounters::TCounterPtr::TValueType::TValueBase;
using TAtomicInstant = std::atomic<TInstant>;
using EWriteDataRequestStatus = NFuse::TWriteBackCache::EWriteDataRequestStatus;

////////////////////////////////////////////////////////////////////////////////

class TWriteDataRequestStats
{
private:
    const ITimerPtr Timer;

    TDynamicCounters::TCounterPtr InProgressCount;
    TDynamicCounters::TCounterPtr Count;
    // WriteData requests can take several minutes (~10^9 microseconds).
    // Summing durations in an ui64 counter may eventually overflow.
    // We use two counters to calculate seconds and microseconds separately.
    // In this case, overflow will happen after about ~10^13 requests.
    TDynamicCounters::TCounterPtr TimeSumSeconds;
    TDynamicCounters::TCounterPtr TimeSumUs;
    TDynamicCounters::TCounterPtr MaxTime;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxTimeCalc;
    TAtomicInstant MinInstant = TInstant::Zero();

    static TDynamicCounters::TCounterPtr GetCounter(
        TDynamicCounters& counters,
        const TString& group,
        const TString& name,
        bool derivative)
    {
        return counters.GetCounter(
            "WriteDataRequest_" + group + "_" + name,
            derivative);
    }

public:
    TWriteDataRequestStats(
            TDynamicCounters& counters,
            ITimerPtr timer,
            const TString& group)
        : Timer(std::move(timer))
        , MaxTimeCalc(Timer)
    {
        InProgressCount = GetCounter(counters, group, "InProgressCount", false);
        Count = GetCounter(counters, group, "Count", true);
        TimeSumSeconds = GetCounter(counters, group, "TimeSumSeconds", true);
        TimeSumUs = GetCounter(counters, group, "TimeSumUs", true);
        MaxTime = GetCounter(counters, group, "MaxTime", false);
    }

    void Reset()
    {
        InProgressCount->Set(0);
        MaxTime->Set(0);
        MinInstant.store(TInstant::Zero());
        MaxTimeCalc = {Timer};
    }

    void IncrementInProgressCount()
    {
        InProgressCount->Inc();
    }

    void DecrementInProgressCount()
    {
        InProgressCount->Dec();
    }

    void AddStats(TDuration duration)
    {
        Count->Inc();
        TimeSumUs->Add(static_cast<TValue>(duration.MicroSecondsOfSecond()));
        TimeSumSeconds->Add(static_cast<TValue>(duration.Seconds()));
        MaxTimeCalc.Add(duration.MicroSeconds());
    }

    void UpdateMinInstant(TInstant value)
    {
        MinInstant.store(value);
    }

    void UpdateStats()
    {
        auto maxTime = MaxTimeCalc.NextValue();
        auto minInstant = MinInstant.load();
        if (minInstant) {
            maxTime = Max(maxTime, (Timer->Now() - minInstant).MicroSeconds());
        }
        MaxTime->Set(static_cast<TValue>(maxTime));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadDataRequestStats
{
private:
    const ITimerPtr Timer;

    TDynamicCounters::TCounterPtr CacheMissCount;
    TDynamicCounters::TCounterPtr CachePartialHitCount;
    TDynamicCounters::TCounterPtr CacheFullHitCount;

    TDynamicCounters::TCounterPtr WaitCount;
    TDynamicCounters::TCounterPtr WaitTimeSumSeconds;
    TDynamicCounters::TCounterPtr WaitTimeSumUs;
    TDynamicCounters::TCounterPtr WaitMaxTime;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> WaitMaxTimeCalc;

    static TDynamicCounters::TCounterPtr GetCounter(
        TDynamicCounters& counters,
        const TString& name,
        bool derivative)
    {
        return counters.GetCounter("ReadDataRequest_" + name, derivative);
    }

public:
    TReadDataRequestStats(
            TDynamicCounters& counters,
            ITimerPtr timer)
        : Timer(std::move(timer))
        , WaitMaxTimeCalc(Timer)
    {
        CacheFullHitCount = GetCounter(counters, "CacheFullHitCount", true);
        CachePartialHitCount =
            GetCounter(counters, "CachePartialHitCount", true);
        CacheMissCount = GetCounter(counters, "CacheMissCount", true);

        WaitCount = GetCounter(counters, "Wait_Count", true);
        WaitTimeSumSeconds = GetCounter(counters, "Wait_TimeSumSeconds", true);
        WaitTimeSumUs = GetCounter(counters, "Wait_TimeSumUs", true);
        WaitMaxTime = GetCounter(counters, "Wait_MaxTime", false);
    }

    void Reset()
    {
        WaitMaxTime->Set(0);
        WaitMaxTimeCalc = {Timer};
    }

    void IncrementCacheMissCount()
    {
        CacheMissCount->Inc();
    }

    void IncrementCachePartialHitCount()
    {
        CachePartialHitCount->Inc();
    }

    void IncrementCacheFullHitCount()
    {
        CacheFullHitCount->Inc();
    }

    void AddWaitStats(TDuration duration)
    {
        WaitCount->Inc();
        WaitTimeSumUs->Add(
            static_cast<TValue>(duration.MicroSecondsOfSecond()));
        WaitTimeSumSeconds->Add(static_cast<TValue>(duration.Seconds()));
        WaitMaxTimeCalc.Add(duration.MicroSeconds());
    }

    void UpdateStats()
    {
        auto maxWaitTime = WaitMaxTimeCalc.NextValue();
        WaitMaxTime->Set(static_cast<TValue>(maxWaitTime));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheStats
    : public IWriteBackCacheStats
{
private:
    ITimerPtr Timer;

    TDynamicCounters::TCounterPtr InProgressFlushCount;
    TDynamicCounters::TCounterPtr CompletedFlushCount;
    TDynamicCounters::TCounterPtr FailedFlushCount;

    TDynamicCounters::TCounterPtr NodeCount;

    TDynamicCounters::TCounterPtr PersistentQueueRawCapacity;
    TDynamicCounters::TCounterPtr PersistentQueueRawUsedBytesCount;
    TDynamicCounters::TCounterPtr PersistentQueueMaxAllocationBytesCount;
    TDynamicCounters::TCounterPtr PersistentQueueIsCorrupted;

    TWriteDataRequestStats PendingWriteDataRequestStats;
    TWriteDataRequestStats CachedWriteDataRequestStats;
    TWriteDataRequestStats FlushRequestedWriteDataRequestStats;
    TWriteDataRequestStats FlushingWriteDataRequestStats;
    TWriteDataRequestStats FlushedWriteDataRequestStats;

    TReadDataRequestStats ReadDataRequestStats;

public:
    TWriteBackCacheStats(
            TDynamicCounters& counters,
            ITimerPtr timer)
        : Timer(std::move(timer))
        , PendingWriteDataRequestStats(counters, Timer, "Pending")
        , CachedWriteDataRequestStats(counters, Timer, "Cached")
        , FlushRequestedWriteDataRequestStats(counters, Timer, "FlushRequested")
        , FlushingWriteDataRequestStats(counters, Timer, "Flushing")
        , FlushedWriteDataRequestStats(counters, Timer, "Flushed")
        , ReadDataRequestStats(counters, Timer)
    {
        InProgressFlushCount = counters.GetCounter("InProgressFlushCount");
        CompletedFlushCount = counters.GetCounter("CompletedFlushCount", true);
        FailedFlushCount = counters.GetCounter("FailedFlushCount", true);

        NodeCount = counters.GetCounter("NodeCount");

        PersistentQueueRawCapacity =
            counters.GetCounter("PersistentQueue_RawCapacity");
        PersistentQueueRawUsedBytesCount =
            counters.GetCounter("PersistentQueue_RawUsedBytesCount");
        PersistentQueueMaxAllocationBytesCount =
            counters.GetCounter("PersistentQueue_MaxAllocationBytesCount");
        PersistentQueueIsCorrupted =
            counters.GetCounter("PersistentQueue_IsCorrupted");
    }

    void ResetNonDerivativeCounters() override
    {
        InProgressFlushCount->Set(0);
        NodeCount->Set(0);
        PendingWriteDataRequestStats.Reset();
        CachedWriteDataRequestStats.Reset();
        FlushingWriteDataRequestStats.Reset();
        FlushedWriteDataRequestStats.Reset();
        FlushRequestedWriteDataRequestStats.Reset();
        ReadDataRequestStats.Reset();
    }

    void FlushStarted() override
    {
        InProgressFlushCount->Inc();
    }

    void FlushCompleted() override
    {
        InProgressFlushCount->Dec();
        CompletedFlushCount->Inc();
    }

    void FlushFailed() override
    {
        FailedFlushCount->Inc();
    }

    void IncrementNodeCount() override
    {
        NodeCount->Inc();
    }

    void DecrementNodeCount() override
    {
        NodeCount->Dec();
    }

    void WriteDataRequestEnteredStatus(
        EWriteDataRequestStatus status) override
    {
        auto& stats = GetWriteDataRequestStats(status);
        stats.IncrementInProgressCount();
    }

    void WriteDataRequestExitedStatus(
        EWriteDataRequestStatus status,
        TDuration duration) override
    {
        auto& stats = GetWriteDataRequestStats(status);
        stats.DecrementInProgressCount();
        stats.AddStats(duration);
    }

    void WriteDataRequestUpdateMinTime(
        EWriteDataRequestStatus status,
        TInstant minTime) override
    {
        auto& stats = GetWriteDataRequestStats(status);
        stats.UpdateMinInstant(minTime);
    }

    void AddReadDataStats(
        EReadDataRequestCacheStatus status,
        TDuration waitDuration) override
    {
        switch (status) {
            case EReadDataRequestCacheStatus::Miss:
                ReadDataRequestStats.IncrementCacheMissCount();
                break;
            case EReadDataRequestCacheStatus::PartialHit:
                ReadDataRequestStats.IncrementCachePartialHitCount();
                break;
            case EReadDataRequestCacheStatus::FullHit:
                ReadDataRequestStats.IncrementCacheFullHitCount();
                break;
            default:
                Y_UNREACHABLE();
        }
        ReadDataRequestStats.AddWaitStats(waitDuration);
    }

    void UpdatePersistentQueueStats(
        const TPersistentQueueStats& stats) override
    {
        PersistentQueueRawCapacity->Set(static_cast<TValue>(stats.RawCapacity));
        PersistentQueueRawUsedBytesCount->Set(
            static_cast<TValue>(stats.RawUsedBytesCount));
        PersistentQueueMaxAllocationBytesCount->Set(
            static_cast<TValue>(stats.MaxAllocationBytesCount));
        PersistentQueueIsCorrupted->Set(static_cast<TValue>(stats.IsCorrupted));
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);

        PendingWriteDataRequestStats.UpdateStats();
        CachedWriteDataRequestStats.UpdateStats();
        FlushRequestedWriteDataRequestStats.UpdateStats();
        FlushingWriteDataRequestStats.UpdateStats();
        FlushedWriteDataRequestStats.UpdateStats();

        ReadDataRequestStats.UpdateStats();
    }

private:
    TWriteDataRequestStats& GetWriteDataRequestStats(
        EWriteDataRequestStatus status)
    {
        switch (status) {
            case EWriteDataRequestStatus::Pending:
                return PendingWriteDataRequestStats;
            case EWriteDataRequestStatus::Cached:
                return CachedWriteDataRequestStats;
            case EWriteDataRequestStatus::FlushRequested:
                return FlushRequestedWriteDataRequestStats;
            case EWriteDataRequestStatus::Flushing:
                return FlushingWriteDataRequestStats;
            case EWriteDataRequestStatus::Flushed:
                return FlushedWriteDataRequestStats;
            default:
                Y_UNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheStatsStub
    : public IWriteBackCacheStats
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

    void WriteDataRequestEnteredStatus(
        TWriteBackCache::EWriteDataRequestStatus status) override
    {
        Y_UNUSED(status);
    }

    void WriteDataRequestExitedStatus(
        TWriteBackCache::EWriteDataRequestStatus status,
        TDuration duration) override
    {
        Y_UNUSED(status);
        Y_UNUSED(duration);
    }

    void WriteDataRequestUpdateMinTime(
        TWriteBackCache::EWriteDataRequestStatus status,
        TInstant minTime) override
    {
        Y_UNUSED(status);
        Y_UNUSED(minTime);
    }

    void AddReadDataStats(
        IWriteBackCacheStats::EReadDataRequestCacheStatus status,
        TDuration duraton) override
    {
        Y_UNUSED(status);
        Y_UNUSED(duraton);
    }

    void UpdatePersistentQueueStats(
        const TPersistentQueueStats& stats) override
    {
        Y_UNUSED(stats);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateWriteBackCacheStats(
    NMonitoring::TDynamicCounters& counters,
    ITimerPtr timer)
{
    return std::make_shared<TWriteBackCacheStats>(
        counters,
        std::move(timer));
}

IWriteBackCacheStatsPtr CreateWriteBackCacheStatsStub()
{
    return std::make_shared<TWriteBackCacheStatsStub>();
}

}   // namespace NCloud::NFileStore::NFuse
