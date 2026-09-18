#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/system/compiler.h>

#include <array>
#include <atomic>
#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Fixed buckets, following TStorageIoStats. Boundaries are converted to cycles
// once: recording does not convert units or access monitoring counters.
struct TLatencyStats
{
    static constexpr std::array<ui64, 25> LimitsUs = {
        1,    2,     3,     4,      5,       10,       15,      20,   25,
        30,   35,    40,    50,     100,     200,      500,     1000, 2000,
        5000, 10000, 35000, 100000, 1000000, 10000000, 60000000};
    static constexpr size_t BucketCount = LimitsUs.size() + 1;

    const std::array<ui64, LimitsUs.size()> LimitsCycles;
    std::array<std::atomic<ui64>, BucketCount> Buckets = {};
    std::atomic<ui64> TotalCycles = 0;

    TLatencyStats();

    void Record(ui64 cycles)
    {
        Buckets[FindBucket(cycles)].fetch_add(1, std::memory_order_relaxed);
        TotalCycles.fetch_add(cycles, std::memory_order_relaxed);
    }

    // The writer owns these values, as with an SPDK per-channel histogram.
    // Atomic loads/stores let the publisher read without a data race, without
    // locked read-modify-write instructions on the IO thread.
    void RecordSingleWriter(ui64 cycles)
    {
        auto& count = Buckets[FindBucket(cycles)];
        count.store(
            count.load(std::memory_order_relaxed) + 1,
            std::memory_order_relaxed);
        TotalCycles.store(
            TotalCycles.load(std::memory_order_relaxed) + cycles,
            std::memory_order_relaxed);
    }

private:
    Y_FORCE_INLINE size_t FindBucket(ui64 cycles) const
    {
        // Completion callbacks and empty waits often take less than one us.
        if (cycles <= LimitsCycles.front()) {
            return 0;
        }
        size_t bucket = 1;
        while (bucket < LimitsCycles.size() && cycles > LimitsCycles[bucket]) {
            ++bucket;
        }
        return bucket;
    }
};

// Counters require a non-null counter group and explicit concurrency and
// publication modes. Periodic counters share one publisher; only the fixed
// atomic stats above are touched on IO paths.
class TLatencyCounter
{
private:
    struct TPublishedStats;
    class TPublisher;

    std::shared_ptr<TPublishedStats> Published;
    TLatencyStats* Stats;
    bool SingleWriter;

public:
    enum class EPublishing
    {
        Periodic,
        Manual,
    };

    enum class EConcurrency
    {
        MultipleWriters,
        SingleWriter,
    };

    TLatencyCounter(
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        const TString& name, EConcurrency concurrency, EPublishing publishing);

    ~TLatencyCounter();

    TLatencyCounter(const TLatencyCounter&) = delete;
    TLatencyCounter& operator=(const TLatencyCounter&) = delete;

    // Flushes pending samples, also used after IO threads have stopped.
    void Publish() const;

    ui64 Start() const
    {
        return GetCycleCount();
    }

    void Record(ui64 started) const
    {
        RecordCycles(GetCycleCount() - started);
    }

    void RecordCycles(ui64 cycles) const
    {
        if (SingleWriter) {
            Stats->RecordSingleWriter(cycles);
        } else {
            Stats->Record(cycles);
        }
    }
};

class TLatencyScope
{
private:
    const TLatencyCounter& Counter;
    const ui64 Started;

public:
    explicit TLatencyScope(const TLatencyCounter& counter)
        : Counter(counter)
        , Started(counter.Start())
    {}

    TLatencyScope(const TLatencyScope&) = delete;
    TLatencyScope& operator=(const TLatencyScope&) = delete;

    ~TLatencyScope()
    {
        Counter.Record(Started);
    }
};

}   // namespace NCloud
