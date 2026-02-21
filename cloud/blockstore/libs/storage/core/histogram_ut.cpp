#include "histogram.h"

#include <cloud/storage/core/libs/diagnostics/counters_printer.h>
#include <cloud/storage/core/libs/diagnostics/histogram_types.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using TDynamicCounterPtr = NMonitoring::TDynamicCounterPtr;
using TDynamicCounters = NMonitoring::TDynamicCounters;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto CreateTestHistogram1(
    const TDynamicCounterPtr& group,
    EHistogramCounterOptions histCounterOptions)
{
    THistogram<TKbSizeBuckets> histogram(histCounterOptions);
    histogram.Register(group, true);

    histogram.Increment(1);
    histogram.Increment(2, 1);
    histogram.Increment(4, 3);   // 4KB: 5
    histogram.Increment(6, 1);
    histogram.Increment(7, 3);   // 8KB: 4
    histogram.Increment(10, 1);
    histogram.Increment(16, 2);   // 16KB: 3
    histogram.Increment(20, 1);
    histogram.Increment(30, 1);   // 32KB: 2
    histogram.Increment(40, 1);   // 64KB: 1
    return histogram;
}

auto CreateTestHistogram2(
    const TDynamicCounterPtr& group,
    EHistogramCounterOptions histCounterOptions)
{
    THistogram<TKbSizeBuckets> histogram(histCounterOptions);
    histogram.Register(group, true);

    histogram.Increment(1024, 1);
    histogram.Increment(2000, 2);
    histogram.Increment(3000, 2);
    histogram.Increment(4096, 1);
    histogram.Increment(4097, 1);
    histogram.Increment(10000, 1);
    histogram.Increment(100500, 1);
    histogram.Increment(1000000000, 1);
    return histogram;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THistogramTest)
{
    Y_UNIT_TEST(TestGetSolomonHistogram)
    {
        TDynamicCounterPtr group{new TDynamicCounters()};

        {
            const auto histogram = CreateTestHistogram1(
                group,
                EHistogramCounterOption::ReportMultipleCounters);
            const auto buckets = histogram.GetSolomonHistogram();

            UNIT_ASSERT_VALUES_EQUAL(buckets.size(), 12);

            const TVector<ui64> expected = {5, 4, 3, 2, 1, 0, 0, 0, 0, 0, 0, 0};
            for (size_t i = 0; i < buckets.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    buckets[i],
                    expected[i],
                    "index=" + ToString(i));
            }
        }

        {
            const auto histogram = CreateTestHistogram2(
                group,
                EHistogramCounterOption::ReportMultipleCounters);
            const auto buckets = histogram.GetSolomonHistogram();

            const TVector<ui64> expected = {0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
            for (size_t i = 0; i < buckets.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    buckets[i],
                    expected[i],
                    "index=" + ToString(i));
            }
        }
    }

    Y_UNIT_TEST(TestGetPercentileBuckets)
    {
        TDynamicCounterPtr group{new TDynamicCounters()};

        {
            const auto histogram = CreateTestHistogram1(
                group,
                EHistogramCounterOption::ReportMultipleCounters);
            const auto buckets = histogram.GetPercentileBuckets();

            UNIT_ASSERT_VALUES_EQUAL(
                buckets.size(),
                TKbSizeBuckets::BUCKETS_COUNT);
            UNIT_ASSERT_DOUBLES_EQUAL(buckets[0].first, 4, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(buckets[1].first, 8, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(buckets[2].first, 16, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(buckets[3].first, 32, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(buckets[4].first, 64, Min<double>());
            UNIT_ASSERT_VALUES_EQUAL(buckets[0].second, 5);
            UNIT_ASSERT_VALUES_EQUAL(buckets[1].second, 4);
            UNIT_ASSERT_VALUES_EQUAL(buckets[2].second, 3);
            UNIT_ASSERT_VALUES_EQUAL(buckets[3].second, 2);
            UNIT_ASSERT_VALUES_EQUAL(buckets[4].second, 1);
        }

        {
            const auto histogram = CreateTestHistogram2(
                group,
                EHistogramCounterOption::ReportMultipleCounters);
            const auto buckets = histogram.GetPercentileBuckets();

            UNIT_ASSERT_VALUES_EQUAL(
                buckets.size(),
                TKbSizeBuckets::BUCKETS_COUNT);
            UNIT_ASSERT_DOUBLES_EQUAL(buckets[8].first, 1024, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(buckets[9].first, 2048, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(buckets[10].first, 4096, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(
                buckets[11].first,
                Max<double>(),
                Min<double>());
            UNIT_ASSERT_VALUES_EQUAL(buckets[8].second, 1);
            UNIT_ASSERT_VALUES_EQUAL(buckets[9].second, 2);
            UNIT_ASSERT_VALUES_EQUAL(buckets[10].second, 3);
            UNIT_ASSERT_VALUES_EQUAL(buckets[11].second, 4);
        }
    }

    Y_UNIT_TEST(TestCalculatePercentiles)
    {
        TDynamicCounterPtr group{new TDynamicCounters()};
        auto histogram = CreateTestHistogram1(
            group,
            EHistogramCounterOption::ReportMultipleCounters);

        TVector<double> percentiles = histogram.CalculatePercentiles();
        UNIT_ASSERT_VALUES_EQUAL(percentiles.size(), 5);
        UNIT_ASSERT_DOUBLES_EQUAL(percentiles[0], 6.5, 0.1);   // p50
        UNIT_ASSERT_VALUES_EQUAL(percentiles[4], 64);          // p100
    }

    Y_UNIT_TEST(TestReset)
    {
        TDynamicCounterPtr group{new TDynamicCounters()};
        auto histogram = CreateTestHistogram1(
            group,
            EHistogramCounterOption::ReportMultipleCounters);

        histogram.Reset();

        const auto buckets = histogram.GetSolomonHistogram();
        for (auto value: buckets) {
            UNIT_ASSERT_VALUES_EQUAL(value, 0);
        }
    }

    Y_UNIT_TEST(TestAdd)
    {
        TDynamicCounterPtr group{new TDynamicCounters()};
        auto histogram1 = CreateTestHistogram1(
            group,
            EHistogramCounterOption::ReportMultipleCounters);
        auto histogram2 = CreateTestHistogram2(
            group,
            EHistogramCounterOption::ReportMultipleCounters);
        auto histogram3 = CreateTestHistogram1(
            group,
            EHistogramCounterOption::ReportMultipleCounters);

        {
            const TVector<ui64> expected = {5, 4, 3, 2, 1, 0, 0, 0, 0, 0, 0, 0};
            const auto buckets = histogram1.GetSolomonHistogram();
            for (size_t i = 0; i < buckets.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    buckets[i],
                    expected[i],
                    "index=" + ToString(i));
            }
        }

        histogram1.Add(histogram2);
        {
            const TVector<ui64> expected = {5, 4, 3, 2, 1, 0, 0, 0, 1, 2, 3, 4};
            const auto buckets = histogram1.GetSolomonHistogram();
            for (size_t i = 0; i < buckets.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    buckets[i],
                    expected[i],
                    "index=" + ToString(i));
            }
        }

        histogram1.Add(histogram3);
        {
            const TVector<ui64> expected =
                {10, 8, 6, 4, 2, 0, 0, 0, 1, 2, 3, 4};
            const auto buckets = histogram1.GetSolomonHistogram();
            for (size_t i = 0; i < buckets.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    buckets[i],
                    expected[i],
                    "index=" + ToString(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldReportHistogramSingleCounter)
    {
        TDynamicCounterPtr group(new TDynamicCounters());
        auto histogram1 = CreateTestHistogram1(
            group,
            EHistogramCounterOption::ReportSingleCounter);
        TStringStream ss;
        TCountersPrinter printer(&ss);
        group->Accept("root", "counters", printer);

        TString expected = R"(root:counters {
  histogram:Time {
    sensor:Time = {4: 5, 8: 4, 16: 3, 32: 2, 64: 1, 128: 0, 256: 0, 512: 0, 1024: 0, 2048: 0, 4096: 0, inf: 0}
  }
}
)";
        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), expected);
    }

    Y_UNIT_TEST(ShouldReportHistogramMultipleCounters)
    {
        TDynamicCounterPtr group(new TDynamicCounters());
        auto histogram1 = CreateTestHistogram1(
            group,
            EHistogramCounterOption::ReportMultipleCounters);
        TStringStream ss;
        TCountersPrinter printer(&ss);
        group->Accept("root", "counters", printer);

        TString expected = R"(root:counters {
  histogram:Time {
    sensor:1024KB = 0
    sensor:128KB = 0
    sensor:16KB = 3
    sensor:2048KB = 0
    sensor:256KB = 0
    sensor:32KB = 2
    sensor:4096KB = 0
    sensor:4KB = 5
    sensor:512KB = 0
    sensor:64KB = 1
    sensor:8KB = 4
    sensor:Inf = 0
  }
}
)";
        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), expected);
    }

    Y_UNIT_TEST(ShouldReportHistogramBothSingleAndMultipleCounters)
    {
        TDynamicCounterPtr group(new TDynamicCounters());
        auto histogram1 = CreateTestHistogram1(
            group,
            EHistogramCounterOption::ReportSingleCounter |
                EHistogramCounterOption::ReportMultipleCounters);
        TStringStream ss;
        TCountersPrinter printer(&ss);
        group->Accept("root", "counters", printer);

        TString expected = R"(root:counters {
  histogram:Time {
    sensor:1024KB = 0
    sensor:128KB = 0
    sensor:16KB = 3
    sensor:2048KB = 0
    sensor:256KB = 0
    sensor:32KB = 2
    sensor:4096KB = 0
    sensor:4KB = 5
    sensor:512KB = 0
    sensor:64KB = 1
    sensor:8KB = 4
    sensor:Inf = 0
    sensor:Time = {4: 5, 8: 4, 16: 3, 32: 2, 64: 1, 128: 0, 256: 0, 512: 0, 1024: 0, 2048: 0, 4096: 0, inf: 0}
  }
}
)";
        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), expected);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
