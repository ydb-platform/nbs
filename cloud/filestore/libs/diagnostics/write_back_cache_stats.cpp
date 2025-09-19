#include "write_back_cache_stats.h"

#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TValue = NMonitoring::TDeprecatedCounter::TValueBase;

TValue ToMicroSeconds(const TDuration& duration)
{
    return static_cast<TValue>(duration.MicroSeconds());
}

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheStats: public IWriteBackCacheStats
{
private:
    TDynamicCounters::TCounterPtr CompletedFlush;
    TDynamicCounters::TCounterPtr FailedFlush;
    TDynamicCounters::TCounterPtr NodeCount;
    TDynamicCounters::TCounterPtr CachedWriteRequestCount;
    TDynamicCounters::TCounterPtr PendingWriteRequestCount;
    TDynamicCounters::TCounterPtr FlushedWriteRequestCount;

    TDynamicCounters::TCounterPtr PersistentQueueCapacity;
    TDynamicCounters::TCounterPtr PersistentQueueUsedBytesCount;
    TDynamicCounters::TCounterPtr PersistentQueueMaxAllocationSize;
    TDynamicCounters::TCounterPtr PersistentQueueCorrupted;

    TDynamicCounters::TCounterPtr PendingTime;
    TDynamicCounters::TCounterPtr WaitingTime;
    TDynamicCounters::TCounterPtr FlushTime;

    TDynamicCounters::TCounterPtr MaxPendingTime;
    TDynamicCounters::TCounterPtr MaxWaitingTime;
    TDynamicCounters::TCounterPtr MaxFlushTime;

    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxPendingTimeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxWaitingTimeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxFlushTimeCalc;

public:
    TWriteBackCacheStats(
            TDynamicCounters& counters,
            ITimerPtr timer)
        : MaxPendingTimeCalc(timer)
        , MaxWaitingTimeCalc(timer)
        , MaxFlushTimeCalc(timer)
    {
        CompletedFlush = counters.GetCounter("CompletedFlush", true);
        FailedFlush = counters.GetCounter("FailedFlush", true);
        NodeCount = counters.GetCounter("NodeCount");
        CachedWriteRequestCount =
            counters.GetCounter("CachedWriteRequestCount");
        PendingWriteRequestCount =
            counters.GetCounter("PendingWriteRequestCount");
        FlushedWriteRequestCount =
            counters.GetCounter("FlushedWriteRequestCount", true);

        PersistentQueueCapacity =
            counters.GetCounter("PersistentQueueCapacity");
        PersistentQueueUsedBytesCount =
            counters.GetCounter("PersistentQueueUsedBytesCount");
        PersistentQueueMaxAllocationSize =
            counters.GetCounter("PersistentQueueMaxAllocationSize");
        PersistentQueueCorrupted =
            counters.GetCounter("PersistentQueueCorrupted");

        PendingTime = counters.GetCounter("PendingTime", true);
        WaitingTime = counters.GetCounter("WaitingTime", true);
        FlushTime = counters.GetCounter("FlushTime", true);

        MaxPendingTime = counters.GetCounter("MaxPendingTime");
        MaxWaitingTime = counters.GetCounter("MaxWaitingTime");
        MaxFlushTime = counters.GetCounter("MaxFlushTime");

        Y_UNUSED(timer);
    }

    void IncrementCompletedFlushCount() override
    {
        CompletedFlush->Inc();
    }

    void IncrementFailedFlushCount() override
    {
        FailedFlush->Inc();
    }

    void SetNodeCount(ui64 value) override
    {
        NodeCount->Set(static_cast<TValue>(value));
    }

    void SetCachedWriteRequestCount(ui64 value) override
    {
        CachedWriteRequestCount->Set(static_cast<TValue>(value));
    }

    void SetPendingWriteRequestCount(ui64 value) override
    {
        PendingWriteRequestCount->Set(static_cast<TValue>(value));
    }

    void SetPersistentQueueStats(
        const NFuse::TWriteBackCache::TPersistentQueueStats& stats) override
    {
        PersistentQueueCapacity->Set(static_cast<TValue>(stats.Capacity));
        PersistentQueueUsedBytesCount->Set(
            static_cast<TValue>(stats.UsedBytesCount));
        PersistentQueueMaxAllocationSize->Set(
            static_cast<TValue>(stats.MaxAllocationSize));
        PersistentQueueCorrupted->Set(stats.IsCorrupted ? 1 : 0);
    }

    void PostWriteRequestStats(
        const NFuse::TWriteBackCache::TWriteDataStats& stats) override
    {
        FlushedWriteRequestCount->Inc();

        PendingTime->Add(ToMicroSeconds(stats.PendingDuration));
        WaitingTime->Add(ToMicroSeconds(stats.WaitingDuration));
        FlushTime->Add(ToMicroSeconds(stats.FlushDuration));

        MaxPendingTimeCalc.Add(ToMicroSeconds(stats.PendingDuration));
        MaxWaitingTimeCalc.Add(ToMicroSeconds(stats.WaitingDuration));
        MaxFlushTimeCalc.Add(ToMicroSeconds(stats.FlushDuration));
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);

        MaxPendingTime->Set(
            static_cast<TValue>(MaxPendingTimeCalc.NextValue()));
        MaxWaitingTime->Set(
            static_cast<TValue>(MaxWaitingTimeCalc.NextValue()));
        MaxFlushTime->Set(static_cast<TValue>(MaxFlushTimeCalc.NextValue()));
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
