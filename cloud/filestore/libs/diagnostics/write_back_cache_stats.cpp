#include "write_back_cache_stats.h"

#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <atomic>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TValue = NMonitoring::TDeprecatedCounter::TValueBase;
using TAtomicInstant = std::atomic<TInstant>;

////////////////////////////////////////////////////////////////////////////////

class TWriteDataRequestStats
{
private:
    const ITimerPtr Timer;

    TDynamicCounters::TCounterPtr InProgressCount;
    TDynamicCounters::TCounterPtr Count;
    // WriteData requests can take several minutes (~10^9 microseconds).
    // Summing durations in an ui64 counter may eventually overflow.
    // We use two counters for calculate seconds and microseconds separately.
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
        CachePartialHitCount = GetCounter(counters, "CachePartialHitCount", true);
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
        WaitTimeSumUs->Add(static_cast<TValue>(duration.MicroSecondsOfSecond()));
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

class TWriteBackCacheStats: public IWriteBackCacheStats
{
private:
    ITimerPtr Timer;

    TDynamicCounters::TCounterPtr InProgressFlushCount;
    TDynamicCounters::TCounterPtr CompletedFlushCount;
    TDynamicCounters::TCounterPtr FailedFlushCount;

    TDynamicCounters::TCounterPtr NodeCount;

    TDynamicCounters::TCounterPtr PersistentQueueRawCapacity;
    TDynamicCounters::TCounterPtr PersistentQueueRawUsedBytesCount;
    TDynamicCounters::TCounterPtr PersistentQueueMaxAllocationSize;
    TDynamicCounters::TCounterPtr PersistentQueueCorrupted;

    TWriteDataRequestStats PendingWriteDataRequestStats;
    TWriteDataRequestStats WaitWriteDataRequestStats;
    TWriteDataRequestStats FlushWriteDataRequestStats;
    TWriteDataRequestStats EvictWriteDataRequestStats;
    TWriteDataRequestStats CachedWriteDataRequestStats;

    TReadDataRequestStats ReadDataRequestStats;

public:
    TWriteBackCacheStats(
            TDynamicCounters& counters,
            ITimerPtr timer)
        : Timer(std::move(timer))
        , PendingWriteDataRequestStats(counters, Timer, "Pending")
        , WaitWriteDataRequestStats(counters, Timer, "Wait")
        , FlushWriteDataRequestStats(counters, Timer, "Flush")
        , EvictWriteDataRequestStats(counters, Timer, "Evict")
        , CachedWriteDataRequestStats(counters, Timer, "Cached")
        , ReadDataRequestStats(counters, Timer)
    {
        InProgressFlushCount = counters.GetCounter("InProgressFlushCount");
        CompletedFlushCount = counters.GetCounter("CompletedFlushCount", true);
        FailedFlushCount = counters.GetCounter("FailedFlushCount", true);

        NodeCount = counters.GetCounter("NodeCount");

        PersistentQueueRawCapacity =
            counters.GetCounter("PersistentQueueRawCapacity");
        PersistentQueueRawUsedBytesCount =
            counters.GetCounter("PersistentQueueRawUsedBytesCount");
        PersistentQueueMaxAllocationSize =
            counters.GetCounter("PersistentQueueMaxAllocationSize");
        PersistentQueueCorrupted =
            counters.GetCounter("PersistentQueueCorrupted");
    }

    void Reset() override
    {
        InProgressFlushCount->Set(0);
        NodeCount->Set(0);
        PendingWriteDataRequestStats.Reset();
        WaitWriteDataRequestStats.Reset();
        FlushWriteDataRequestStats.Reset();
        EvictWriteDataRequestStats.Reset();
        CachedWriteDataRequestStats.Reset();
        ReadDataRequestStats.Reset();
    }

    void IncrementInProgressFlushCount() override
    {
        InProgressFlushCount->Inc();
    }

    void DecrementInProgressFlushCount() override
    {
        InProgressFlushCount->Dec();
    }

    void IncrementCompletedFlushCount() override
    {
        CompletedFlushCount->Inc();
    }

    void IncrementFailedFlushCount() override
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

    void SetWriteDataRequestMinInstant(
        EWriteDataRequestState state,
        TInstant value) override
    {
        GetWriteDataRequestStats(state).UpdateMinInstant(value);
    }

    void IncrementInProgressWriteDataRequestCount(
        EWriteDataRequestState state) override
    {
        GetWriteDataRequestStats(state).IncrementInProgressCount();
    }

    void DecrementInProgressWriteDataRequestCount(
        EWriteDataRequestState state) override
    {
        GetWriteDataRequestStats(state).DecrementInProgressCount();
    }

    void AddWriteDataRequestStats(
        EWriteDataRequestState state,
        TDuration duration) override
    {
        GetWriteDataRequestStats(state).AddStats(duration);
    }

    void AddReadDataStats(
        EReadDataRequestCacheState state,
        TDuration waitDuration) override
    {
        switch (state) {
            case EReadDataRequestCacheState::Miss:
                ReadDataRequestStats.IncrementCacheMissCount();
                break;
            case EReadDataRequestCacheState::PartialHit:
                ReadDataRequestStats.IncrementCachePartialHitCount();
                break;
            case EReadDataRequestCacheState::FullHit:
                ReadDataRequestStats.IncrementCacheFullHitCount();
                break;
            default:
                Y_ABORT("Invalid EReadDataRequestCacheState value");
        }
        ReadDataRequestStats.AddWaitStats(waitDuration);
    }

    void SetPersistentQueueStats(
        const NFuse::TWriteBackCache::TPersistentQueueStats& stats) override
    {
        PersistentQueueRawCapacity->Set(static_cast<TValue>(stats.RawCapacity));
        PersistentQueueRawUsedBytesCount->Set(
            static_cast<TValue>(stats.RawUsedBytesCount));
        PersistentQueueMaxAllocationSize->Set(
            static_cast<TValue>(stats.MaxAllocationSize));
        PersistentQueueCorrupted->Set(stats.IsCorrupted ? 1 : 0);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);

        PendingWriteDataRequestStats.UpdateStats();
        WaitWriteDataRequestStats.UpdateStats();
        FlushWriteDataRequestStats.UpdateStats();
        EvictWriteDataRequestStats.UpdateStats();
        CachedWriteDataRequestStats.UpdateStats();
        ReadDataRequestStats.UpdateStats();
    }

private:
    TWriteDataRequestStats& GetWriteDataRequestStats(
        EWriteDataRequestState state)
    {
        switch (state) {
            case EWriteDataRequestState::Pending:
                return PendingWriteDataRequestStats;
            case EWriteDataRequestState::Wait:
                return WaitWriteDataRequestStats;
            case EWriteDataRequestState::Flush:
                return FlushWriteDataRequestStats;
            case EWriteDataRequestState::Evict:
                return EvictWriteDataRequestStats;
            case EWriteDataRequestState::Cached:
                return CachedWriteDataRequestStats;
            default:
                Y_ABORT("Invalid EWriteDataRequestState value");
        }
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

}   // namespace NCloud::NFileStore
