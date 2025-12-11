#include "aggregator.h"

#include "metric.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMetricImplTest)
{
    Y_UNIT_TEST(ShouldSumAbsAggregateMetrics)
    {
        std::atomic<i64> first(42);
        auto firstMetric = CreateMetric(first);

        i64 second = 322;
        auto secondMetric = CreateMetric([&] { return second; });

        NImpl::TAggregator aggregator(
            EAggregationType::AT_SUM,
            EMetricType::MT_ABSOLUTE);
        UNIT_ASSERT_VALUES_EQUAL(
            EAggregationType::AT_SUM,
            aggregator.GetAggregationType());
        UNIT_ASSERT_VALUES_EQUAL(
            EMetricType::MT_ABSOLUTE,
            aggregator.GetMetricType());

        const auto firstKey = aggregator.Register(firstMetric);
        const auto secondKey = aggregator.Register(secondMetric);

        UNIT_ASSERT_VALUES_EQUAL(364, aggregator.Aggregate(TInstant::Now()));

        first = -11;

        UNIT_ASSERT_VALUES_EQUAL(311, aggregator.Aggregate(TInstant::Now()));

        second = 5;

        UNIT_ASSERT_VALUES_EQUAL(-6, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(firstKey);

        UNIT_ASSERT_VALUES_EQUAL(5, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(secondKey);

        UNIT_ASSERT_VALUES_EQUAL(0, aggregator.Aggregate(TInstant::Now()));
    }

    Y_UNIT_TEST(ShouldAvgAbsAggregateMetrics)
    {
        std::atomic<i64> first(42);
        auto firstMetric = CreateMetric(first);

        i64 second = 322;
        auto secondMetric = CreateMetric([&] { return second; });

        NImpl::TAggregator aggregator(
            EAggregationType::AT_AVG,
            EMetricType::MT_ABSOLUTE);
        UNIT_ASSERT_VALUES_EQUAL(
            EAggregationType::AT_AVG,
            aggregator.GetAggregationType());
        UNIT_ASSERT_VALUES_EQUAL(
            EMetricType::MT_ABSOLUTE,
            aggregator.GetMetricType());

        const auto firstKey = aggregator.Register(firstMetric);
        const auto secondKey = aggregator.Register(secondMetric);

        UNIT_ASSERT_VALUES_EQUAL(182, aggregator.Aggregate(TInstant::Now()));

        first = -11;

        UNIT_ASSERT_VALUES_EQUAL(155, aggregator.Aggregate(TInstant::Now()));

        second = 5;

        UNIT_ASSERT_VALUES_EQUAL(-3, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(firstKey);

        UNIT_ASSERT_VALUES_EQUAL(5, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(secondKey);

        UNIT_ASSERT_VALUES_EQUAL(0, aggregator.Aggregate(TInstant::Now()));
    }

    Y_UNIT_TEST(ShouldMinAbsAggregateMetrics)
    {
        std::atomic<i64> first(42);
        auto firstMetric = CreateMetric(first);

        i64 second = 322;
        auto secondMetric = CreateMetric([&] { return second; });

        NImpl::TAggregator aggregator(
            EAggregationType::AT_MIN,
            EMetricType::MT_ABSOLUTE);
        UNIT_ASSERT_VALUES_EQUAL(
            EAggregationType::AT_MIN,
            aggregator.GetAggregationType());
        UNIT_ASSERT_VALUES_EQUAL(
            EMetricType::MT_ABSOLUTE,
            aggregator.GetMetricType());

        const auto firstKey = aggregator.Register(firstMetric);
        const auto secondKey = aggregator.Register(secondMetric);

        UNIT_ASSERT_VALUES_EQUAL(42, aggregator.Aggregate(TInstant::Now()));

        first = -11;

        UNIT_ASSERT_VALUES_EQUAL(-11, aggregator.Aggregate(TInstant::Now()));

        second = 5;

        UNIT_ASSERT_VALUES_EQUAL(-11, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(firstKey);

        UNIT_ASSERT_VALUES_EQUAL(5, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(secondKey);

        UNIT_ASSERT_VALUES_EQUAL(
            Max<i64>(),
            aggregator.Aggregate(TInstant::Now()));
    }

    Y_UNIT_TEST(ShouldMaxAbsAggregateMetrics)
    {
        std::atomic<i64> first(42);
        auto firstMetric = CreateMetric(first);

        i64 second = 322;
        auto secondMetric = CreateMetric([&] { return second; });

        NImpl::TAggregator aggregator(
            EAggregationType::AT_MAX,
            EMetricType::MT_ABSOLUTE);
        UNIT_ASSERT_VALUES_EQUAL(
            EAggregationType::AT_MAX,
            aggregator.GetAggregationType());
        UNIT_ASSERT_VALUES_EQUAL(
            EMetricType::MT_ABSOLUTE,
            aggregator.GetMetricType());

        const auto firstKey = aggregator.Register(firstMetric);
        const auto secondKey = aggregator.Register(secondMetric);

        UNIT_ASSERT_VALUES_EQUAL(322, aggregator.Aggregate(TInstant::Now()));

        first = -11;

        UNIT_ASSERT_VALUES_EQUAL(322, aggregator.Aggregate(TInstant::Now()));

        second = 5;

        UNIT_ASSERT_VALUES_EQUAL(5, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(secondKey);

        UNIT_ASSERT_VALUES_EQUAL(-11, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(firstKey);

        UNIT_ASSERT_VALUES_EQUAL(
            Min<i64>(),
            aggregator.Aggregate(TInstant::Now()));
    }

    Y_UNIT_TEST(ShouldSumDerAggregateMetrics)
    {
        std::atomic<i64> first(42);
        auto firstMetric = CreateMetric(first);

        i64 second = 322;
        auto secondMetric = CreateMetric([&] { return second; });

        NImpl::TAggregator aggregator(
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);
        UNIT_ASSERT_VALUES_EQUAL(
            EAggregationType::AT_SUM,
            aggregator.GetAggregationType());
        UNIT_ASSERT_VALUES_EQUAL(
            EMetricType::MT_DERIVATIVE,
            aggregator.GetMetricType());

        const auto firstKey = aggregator.Register(firstMetric);
        const auto secondKey = aggregator.Register(secondMetric);

        UNIT_ASSERT_VALUES_EQUAL(0, aggregator.Aggregate(TInstant::Now()));

        first += 11;

        UNIT_ASSERT_VALUES_EQUAL(11, aggregator.Aggregate(TInstant::Now()));

        second += 5;

        UNIT_ASSERT_VALUES_EQUAL(16, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(firstKey);

        UNIT_ASSERT_VALUES_EQUAL(16, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(secondKey);

        UNIT_ASSERT_VALUES_EQUAL(16, aggregator.Aggregate(TInstant::Now()));
    }

    Y_UNIT_TEST(ShouldAvgDerAggregateMetrics)
    {
        std::atomic<i64> first(42);
        auto firstMetric = CreateMetric(first);

        i64 second = 322;
        auto secondMetric = CreateMetric([&] { return second; });

        NImpl::TAggregator aggregator(
            EAggregationType::AT_AVG,
            EMetricType::MT_DERIVATIVE);
        UNIT_ASSERT_VALUES_EQUAL(
            EAggregationType::AT_AVG,
            aggregator.GetAggregationType());
        UNIT_ASSERT_VALUES_EQUAL(
            EMetricType::MT_DERIVATIVE,
            aggregator.GetMetricType());

        const auto firstKey = aggregator.Register(firstMetric);
        const auto secondKey = aggregator.Register(secondMetric);

        UNIT_ASSERT_VALUES_EQUAL(0, aggregator.Aggregate(TInstant::Now()));

        first += 11;

        UNIT_ASSERT_VALUES_EQUAL(5, aggregator.Aggregate(TInstant::Now()));

        second += 5;

        UNIT_ASSERT_VALUES_EQUAL(8, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(firstKey);

        UNIT_ASSERT_VALUES_EQUAL(16, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(secondKey);

        UNIT_ASSERT_VALUES_EQUAL(16, aggregator.Aggregate(TInstant::Now()));
    }

    Y_UNIT_TEST(ShouldMinDerAggregateMetrics)
    {
        std::atomic<i64> first(42);
        auto firstMetric = CreateMetric(first);

        i64 second = 322;
        auto secondMetric = CreateMetric([&] { return second; });

        NImpl::TAggregator aggregator(
            EAggregationType::AT_MIN,
            EMetricType::MT_DERIVATIVE);
        UNIT_ASSERT_VALUES_EQUAL(
            EAggregationType::AT_MIN,
            aggregator.GetAggregationType());
        UNIT_ASSERT_VALUES_EQUAL(
            EMetricType::MT_DERIVATIVE,
            aggregator.GetMetricType());

        const auto firstKey = aggregator.Register(firstMetric);
        const auto secondKey = aggregator.Register(secondMetric);

        UNIT_ASSERT_VALUES_EQUAL(42, aggregator.Aggregate(TInstant::Now()));

        first = -11;

        UNIT_ASSERT_VALUES_EQUAL(-11, aggregator.Aggregate(TInstant::Now()));

        second = 5;

        UNIT_ASSERT_VALUES_EQUAL(-11, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(firstKey);

        UNIT_ASSERT_VALUES_EQUAL(5, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(secondKey);

        UNIT_ASSERT_VALUES_EQUAL(
            Max<i64>(),
            aggregator.Aggregate(TInstant::Now()));
    }

    Y_UNIT_TEST(ShouldMaxDerAggregateMetrics)
    {
        std::atomic<i64> first(42);
        auto firstMetric = CreateMetric(first);

        i64 second = 322;
        auto secondMetric = CreateMetric([&] { return second; });

        NImpl::TAggregator aggregator(
            EAggregationType::AT_MAX,
            EMetricType::MT_DERIVATIVE);
        UNIT_ASSERT_VALUES_EQUAL(
            EAggregationType::AT_MAX,
            aggregator.GetAggregationType());
        UNIT_ASSERT_VALUES_EQUAL(
            EMetricType::MT_DERIVATIVE,
            aggregator.GetMetricType());

        const auto firstKey = aggregator.Register(firstMetric);
        const auto secondKey = aggregator.Register(secondMetric);

        UNIT_ASSERT_VALUES_EQUAL(322, aggregator.Aggregate(TInstant::Now()));

        first = -11;

        UNIT_ASSERT_VALUES_EQUAL(322, aggregator.Aggregate(TInstant::Now()));

        second = 5;

        UNIT_ASSERT_VALUES_EQUAL(5, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(secondKey);

        UNIT_ASSERT_VALUES_EQUAL(-11, aggregator.Aggregate(TInstant::Now()));

        aggregator.Unregister(firstKey);

        UNIT_ASSERT_VALUES_EQUAL(
            Min<i64>(),
            aggregator.Aggregate(TInstant::Now()));
    }
}

}   // namespace NCloud::NFileStore::NMetrics
