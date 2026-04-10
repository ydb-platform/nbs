#pragma once

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/system/types.h>

#include <array>
#include <atomic>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// Max values are calculated over a sliding window - each Update() call shifts
// the window and resets the current bucket.
// Usually, Update() is called periodically every 1 second and the metrics are
// collected every 15 seconds.
constexpr size_t DefaultMaxCalculatorBucketSize = 15;

////////////////////////////////////////////////////////////////////////////////

/**
 * Most of the counters in WriteBackCache are modified inside lock sections.
 * There is no need to use strict memory ordering for these counters.
 * Need only to ensure that IMetricsRegistry can read it atomically.
 *
 * Rule of thumb:
 * - All Get* calls can be called concurrently;
 * - All modify calls (including Update*) should be synchronized.
 */

// Simple wrapper over an atomic variable
class TRelaxedCounter
{
private:
    std::atomic<i64> Value = 0;

public:
    // Can be called concurrently
    i64 Get() const
    {
        return Value.load(std::memory_order_relaxed);
    }

    void Set(i64 value)
    {
        Value.store(value, std::memory_order_relaxed);
    }

    void Inc()
    {
        Value.fetch_add(1, std::memory_order_relaxed);
    }

    void Add(i64 value)
    {
        Value.fetch_add(value, std::memory_order_relaxed);
    }

    void Dec()
    {
        Value.fetch_sub(1, std::memory_order_relaxed);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Report Max value over sliding window
template <size_t BucketCount = DefaultMaxCalculatorBucketSize>
class TRelaxedMaxCounter
{
    static_assert(BucketCount > 0, "BucketCount must be positive");

private:
    // Access to Bucket array is single-threaded and should be synchronized
    std::array<i64, BucketCount> Buckets{};
    size_t CurrentBucketIndex = 0;
    // This value can be read outside lock
    std::atomic<i64> MaxValue = 0;

public:
    // Can be called concurrently
    i64 Get() const
    {
        return MaxValue.load(std::memory_order_relaxed);
    }

    void Put(i64 value)
    {
        auto& bucketValue = Buckets[CurrentBucketIndex];
        bucketValue = Max(bucketValue, value);

        MaxValue.store(
            Max(MaxValue.load(std::memory_order_relaxed), value),
            std::memory_order_relaxed);
    }

    // Recalculate MaxValue and go to the next bucket
    void Update()
    {
        MaxValue.store(
            *MaxElement(Buckets.begin(), Buckets.end()),
            std::memory_order_relaxed);

        CurrentBucketIndex = (CurrentBucketIndex + 1) % BucketCount;
        Buckets[CurrentBucketIndex] = 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

// Combines TRelaxedCounter and TRelaxedMaxCounter
template <size_t BucketCount = DefaultMaxCalculatorBucketSize>
class TRelaxedCombinedMaxCounter
{
private:
    TRelaxedCounter CurrentCount;
    TRelaxedMaxCounter<BucketCount> MaxCount;

public:
    // Can be called concurrently
    i64 GetCurrent() const
    {
        return CurrentCount.Get();
    }

    // Can be called concurrently
    i64 GetMax() const
    {
        return MaxCount.Get();
    }

    void Set(i64 value)
    {
        CurrentCount.Set(value);
        MaxCount.Put(value);
    }

    void Inc()
    {
        CurrentCount.Inc();
        MaxCount.Put(CurrentCount.Get());
    }

    void Dec()
    {
        CurrentCount.Dec();
    }

    void Update()
    {
        MaxCount.Update();
        MaxCount.Put(CurrentCount.Get());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <size_t BucketCount = DefaultMaxCalculatorBucketSize>
class TRelaxedEventCounter
{
private:
    TRelaxedCombinedMaxCounter<BucketCount> ActiveCount;
    TRelaxedCounter CompletedCount;

public:
    // Can be called concurrently
    i64 GetActiveCount() const
    {
        return ActiveCount.GetCurrent();
    }

    // Can be called concurrently
    i64 GetActiveMaxCount() const
    {
        return ActiveCount.GetMax();
    }

    // Can be called concurrently
    i64 GetCompletedCount() const
    {
        return CompletedCount.Get();
    }

    void Started()
    {
        ActiveCount.Inc();
    }

    void Completed()
    {
        CompletedCount.Inc();
        ActiveCount.Dec();
    }

    void Reset()
    {
        ActiveCount.Set(0);
    }

    void Update()
    {
        ActiveCount.Update();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <size_t BucketCount = DefaultMaxCalculatorBucketSize>
class TRelaxedEventCounterWithTimeStats
{
private:
    TRelaxedEventCounter<BucketCount> EventCount;
    TRelaxedCounter CompletedTime;
    TRelaxedMaxCounter<BucketCount> MaxTime;

public:
    // Can be called concurrently
    i64 GetActiveCount() const
    {
        return EventCount.GetActiveCount();
    }

    // Can be called concurrently
    i64 GetActiveMaxCount() const
    {
        return EventCount.GetActiveMaxCount();
    }

    // Can be called concurrently
    i64 GetCompletedCount() const
    {
        return EventCount.GetCompletedCount();
    }

    // Can be called concurrently
    i64 GetCompletedTime() const
    {
        return CompletedTime.Get();
    }

    // Can be called concurrently
    i64 GetMaxTime() const
    {
        return MaxTime.Get();
    }

    void Started()
    {
        EventCount.Started();
    }

    void Completed(TDuration duration)
    {
        const i64 us = static_cast<i64>(duration.MicroSeconds());

        EventCount.Completed();
        CompletedTime.Add(us);
        MaxTime.Put(us);
    }

    void Reset()
    {
        EventCount.Reset();
    }

    void Update(TDuration maxActiveDuration)
    {
        const i64 us = static_cast<i64>(maxActiveDuration.MicroSeconds());
        MaxTime.Put(us);
        MaxTime.Update();
        EventCount.Update();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <size_t BucketCount = DefaultMaxCalculatorBucketSize>
class TRelaxedExtendedEventCounterWithTimeStats
    : public TRelaxedEventCounterWithTimeStats<BucketCount>
{
private:
    TRelaxedCounter FailedCount;
    TRelaxedCounter CompletedImmediatelyCount;

public:
    // Can be called concurrently
    i64 GetFailedCount() const
    {
        return FailedCount.Get();
    }

    // Can be called concurrently
    i64 GetCompletedImmediately() const
    {
        return CompletedImmediatelyCount.Get();
    }

    void Failed(TDuration duration)
    {
        FailedCount.Inc();
        this->Completed(duration);
    }

    void CompletedImmediately()
    {
        CompletedImmediatelyCount.Inc();
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
