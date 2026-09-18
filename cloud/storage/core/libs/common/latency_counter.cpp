#include "latency_counter.h"

#include "thread.h"

#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TLatencyStats::TLatencyStats()
    : LimitsCycles(
          []
          {
              std::array<ui64, LimitsUs.size()> limits;
              for (size_t i = 0; i < limits.size(); ++i) {
                  limits[i] = DurationToCyclesSafe(
                      TDuration::MicroSeconds(LimitsUs[i]));
              }
              return limits;
          }())
{}

struct TLatencyCounter::TPublishedStats
{
    TLatencyStats Stats;
    NMonitoring::TDynamicCounters::TCounterPtr Count;
    NMonitoring::TDynamicCounters::TCounterPtr Time;
    NMonitoring::THistogramPtr Histogram;
    std::array<NMonitoring::TDynamicCounters::TCounterPtr, 5> Percentiles;

    std::mutex PublishMutex;
    std::array<ui64, TLatencyStats::BucketCount> PublishedBuckets = {};
    ui64 PublishedTimeUs = 0;

    TPublishedStats(
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        const TString& name)
        : Count(counters->GetCounter(name + "Count", true))
        , Time(counters->GetCounter(name + "TimeUs", true))
        , Histogram(counters->GetHistogram(
              name + "LatencyUs",
              NMonitoring::ExplicitHistogram(
                  {TLatencyStats::LimitsUs.begin(),
                   TLatencyStats::LimitsUs.end()}), true))
    {
        auto group = counters->GetSubgroup("percentiles", name)
                         ->GetSubgroup("units", "usec");
        constexpr std::array<const char*, 5> names =
            {"50", "90", "99", "99.9", "100"};
        for (size_t i = 0; i < names.size(); ++i) {
            Percentiles[i] = group->GetCounter(names[i]);
        }
    }

    void Publish()
    {
        std::lock_guard guard(PublishMutex);
        std::array<ui64, TLatencyStats::BucketCount> counts;
        ui64 total = 0;
        for (size_t i = 0; i < counts.size(); ++i) {
            const ui64 count = Stats.Buckets[i].load(std::memory_order_relaxed);
            counts[i] = count - PublishedBuckets[i];
            PublishedBuckets[i] = count;
            total += counts[i];
            if (counts[i]) {
                Histogram->Collect(
                    i < TLatencyStats::LimitsUs.size()
                        ? double(TLatencyStats::LimitsUs[i])
                        : std::numeric_limits<double>::max(), counts[i]);
            }
        }
        Count->Add(total);
        const ui64 timeUs = CyclesToDurationSafe(Stats.TotalCycles.load(
                                                     std::memory_order_relaxed))
                                .MicroSeconds();
        Time->Add(timeUs - PublishedTimeUs);
        PublishedTimeUs = timeUs;

        // Percentiles describe this publication interval, not process lifetime.
        // Interpolate within finite buckets, as disk-registry device stats do.
        constexpr std::array<double, 5> ranks = {0.5, 0.9, 0.99, 0.999, 1.0};
        for (size_t p = 0; p < ranks.size(); ++p) {
            double result = 0;
            ui64 preceding = 0;
            ui64 lower = 0;
            if (total) {
                for (size_t i = 0; i < counts.size(); ++i) {
                    const ui64 upper = TLatencyStats::LimitsUs[std::min(
                        i, TLatencyStats::LimitsUs.size() - 1)];
                    if (counts[i] && preceding + counts[i] >= ranks[p] * total)
                    {
                        result = lower + (upper - lower) *
                                             ((ranks[p] * total - preceding) /
                                              counts[i]);
                        break;
                    }
                    preceding += counts[i];
                    lower = upper;
                }
            }
            Percentiles[p]->Set(std::llround(result));
        }
    }
};

// Like the local-NVMe UpdateCountersLoop, publication is periodic and
// independent of IO progress. One sleeping thread serves all enabled counters
// in the process, including services whose submission or completion thread is
// blocked.
class TLatencyCounter::TPublisher
{
private:
    std::mutex Mutex;
    std::condition_variable Wakeup;
    bool Stopped = false;
    std::vector<std::weak_ptr<TPublishedStats>> Counters;
    std::thread Thread;

public:
    TPublisher()
        : Thread([this] { Run(); })
    {}

    ~TPublisher()
    {
        {
            std::lock_guard guard(Mutex);
            Stopped = true;
        }
        Wakeup.notify_one();
        Thread.join();
    }

    static TPublisher& Instance()
    {
        static TPublisher publisher;
        return publisher;
    }

    void Register(const std::shared_ptr<TPublishedStats>& stats)
    {
        std::lock_guard guard(Mutex);
        Counters.push_back(stats);
    }

private:
    void Run()
    {
        SetCurrentThreadName("IO.Stats");
        std::unique_lock lock(Mutex);
        while (!Wakeup.wait_for(
            lock, std::chrono::seconds(1), [this] { return Stopped; }))
        {
            std::vector<std::shared_ptr<TPublishedStats>> live;
            std::erase_if(
                Counters,
                [&live](const auto& weak)
                {
                    if (auto stats = weak.lock()) {
                        live.push_back(std::move(stats));
                        return false;
                    }
                    return true;
                });
            lock.unlock();
            for (auto& stats: live) {
                stats->Publish();
            }
            // Drop the last possible reference outside the registry lock.
            live.clear();
            lock.lock();
        }
    }
};

TLatencyCounter::TLatencyCounter(
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
    const TString& name, EConcurrency concurrency, EPublishing publishing)
    : Published(std::make_shared<TPublishedStats>(std::move(counters), name))
    , Stats(&Published->Stats)
    , SingleWriter(concurrency == EConcurrency::SingleWriter)
{
    if (publishing == EPublishing::Periodic) {
        TPublisher::Instance().Register(Published);
    }
}

TLatencyCounter::~TLatencyCounter()
{
    Publish();
}

void TLatencyCounter::Publish() const
{
    Published->Publish();
}

}   // namespace NCloud
