#include "latency_counter.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/thread.h>

#include <thread>
#include <type_traits>
#include <vector>

namespace NCloud {

namespace {

using EConcurrency = TLatencyCounter::EConcurrency;
using EPublishing = TLatencyCounter::EPublishing;

static_assert(!std::is_default_constructible_v<TLatencyCounter>);
static_assert(!std::is_move_constructible_v<TLatencyCounter>);
static_assert(!std::is_move_assignable_v<TLatencyCounter>);
static_assert(!std::is_constructible_v<
              TLatencyCounter,
              TIntrusivePtr<NMonitoring::TDynamicCounters>, TString>);
static_assert(
    !std::is_constructible_v<
        TLatencyCounter,
        TIntrusivePtr<NMonitoring::TDynamicCounters>, TString, EConcurrency>);

ui64 GetSampleCount(const NMonitoring::THistogramPtr& histogram)
{
    auto snapshot = histogram->Snapshot();
    ui64 total = 0;
    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        total += snapshot->Value(i);
    }
    return total;
}

void CheckConcurrentPublication(EConcurrency concurrency, size_t writers)
{
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TLatencyCounter latency(
        counters, "Submit", concurrency, EPublishing::Manual);
    const ui64 cycles = DurationToCyclesSafe(TDuration::MicroSeconds(10));
    constexpr size_t samples = 10000;
    std::atomic<bool> finished = false;
    std::thread publisher(
        [&]
        {
            while (!finished.load()) {
                latency.Publish();
            }
        });
    std::vector<std::thread> threads;
    for (size_t i = 0; i < writers; ++i) {
        threads.emplace_back(
            [&]
            {
                for (size_t j = 0; j < samples; ++j) {
                    latency.RecordCycles(cycles);
                }
            });
    }
    for (auto& thread: threads) {
        thread.join();
    }
    finished.store(true);
    publisher.join();
    latency.Publish();

    UNIT_ASSERT_VALUES_EQUAL(
        counters->GetCounter("SubmitCount")->Val(), writers * samples);
    UNIT_ASSERT_VALUES_EQUAL(
        GetSampleCount(counters->FindHistogram("SubmitLatencyUs")),
        writers * samples);
    UNIT_ASSERT_VALUES_EQUAL(
        counters->GetCounter("SubmitTimeUs")->Val(),
        CyclesToDurationSafe(cycles * writers * samples).MicroSeconds());
}

}   // namespace

Y_UNIT_TEST_SUITE(TLatencyCounterTest)
{
    Y_UNIT_TEST(ShouldRecordBoundariesAndOverflowWithoutPublishingOnIOPath)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TLatencyCounter latency(
            counters,
            "Submit", EConcurrency::SingleWriter, EPublishing::Manual);
        TLatencyStats bounds;
        for (ui64 cycles: bounds.LimitsCycles) {
            latency.RecordCycles(cycles);
        }
        latency.RecordCycles(bounds.LimitsCycles.back() + 1);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("SubmitCount")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            GetSampleCount(counters->FindHistogram("SubmitLatencyUs")), 0);

        latency.Publish();
        auto histogram = counters->FindHistogram("SubmitLatencyUs")->Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(
            histogram->Count(), TLatencyStats::BucketCount);
        for (ui32 i = 0; i < histogram->Count(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(histogram->Value(i), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            counters->GetCounter("SubmitCount")->Val(),
            TLatencyStats::BucketCount);
        latency.Publish();
        UNIT_ASSERT_VALUES_EQUAL(
            counters->GetCounter("SubmitCount")->Val(),
            TLatencyStats::BucketCount);
    }

    Y_UNIT_TEST(ShouldPublishIntervalPercentiles)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto percentiles = counters->GetSubgroup("percentiles", "Submit")
                               ->GetSubgroup("units", "usec");
        TLatencyCounter latency(
            counters,
            "Submit", EConcurrency::SingleWriter, EPublishing::Manual);
        for (size_t i = 0; i < 100; ++i) {
            latency.RecordCycles(
                DurationToCyclesSafe(TDuration::MicroSeconds(1000)));
        }
        latency.Publish();
        UNIT_ASSERT_VALUES_EQUAL(percentiles->GetCounter("50")->Val(), 750);
        UNIT_ASSERT_VALUES_EQUAL(percentiles->GetCounter("100")->Val(), 1000);

        latency.RecordCycles(DurationToCyclesSafe(TDuration::MicroSeconds(10)));
        latency.Publish();
        UNIT_ASSERT_VALUES_EQUAL(percentiles->GetCounter("100")->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(
            counters->GetCounter("SubmitCount")->Val(), 101);
        latency.Publish();
        UNIT_ASSERT_VALUES_EQUAL(percentiles->GetCounter("99.9")->Val(), 0);
    }

    Y_UNIT_TEST(ShouldPublishSingleWriterWithoutLosingSamples)
    {
        CheckConcurrentPublication(EConcurrency::SingleWriter, 1);
    }

    Y_UNIT_TEST(ShouldPublishMultipleWritersWithoutLosingSamples)
    {
        CheckConcurrentPublication(EConcurrency::MultipleWriters, 4);
    }

    Y_UNIT_TEST(ShouldPublishWithoutFurtherIO)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TLatencyCounter latency(
            counters,
            "Submit", EConcurrency::SingleWriter, EPublishing::Periodic);
        latency.RecordCycles(100);
        const auto deadline = TDuration::Seconds(5).ToDeadLine();
        auto count = counters->GetCounter("SubmitCount");
        while (!count->Val() && Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(count->Val(), 1);
    }

    Y_UNIT_TEST(ShouldFlushOnDestruction)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        {
            TLatencyCounter latency(
                counters,
                "Submit", EConcurrency::SingleWriter, EPublishing::Manual);
            latency.RecordCycles(1);
            latency.RecordCycles(1);
            UNIT_ASSERT_VALUES_EQUAL(
                counters->GetCounter("SubmitCount")->Val(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("SubmitCount")->Val(), 2);
    }
}

}   // namespace NCloud
