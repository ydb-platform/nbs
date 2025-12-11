#include "visitor.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>

namespace NCloud::NFileStore::NMetrics {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv: public NUnitTest::TBaseFixture
{
    TStringStream Data;
    IRegistryVisitorPtr Visitor;

    TEnv()
        : Visitor(CreateRegistryVisitor(Data))
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVisitorTest)
{
    Y_UNIT_TEST_F(ShouldPrintStream, TEnv)
    {
        Visitor->OnStreamBegin();
        UNIT_ASSERT_VALUES_EQUAL("metrics:[", Data.Str());

        Visitor->OnStreamEnd();
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintSumAbsMetric, TEnv)
    {
        Visitor->OnMetricBegin(
            TInstant::MicroSeconds(1ULL << 56),
            EAggregationType::AT_SUM,
            EMetricType::MT_ABSOLUTE);
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',",
            Data.Str());

        Visitor->OnMetricEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "},",
            Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintAvgAbsMetric, TEnv)
    {
        Visitor->OnMetricBegin(
            TInstant::MicroSeconds(1ULL << 56),
            EAggregationType::AT_AVG,
            EMetricType::MT_ABSOLUTE);
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',",
            Data.Str());

        Visitor->OnMetricEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "},",
            Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintMinAbsMetric, TEnv)
    {
        Visitor->OnMetricBegin(
            TInstant::MicroSeconds(1ULL << 56),
            EAggregationType::AT_MIN,
            EMetricType::MT_ABSOLUTE);
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',",
            Data.Str());

        Visitor->OnMetricEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "},",
            Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintMaxAbsMetric, TEnv)
    {
        Visitor->OnMetricBegin(
            TInstant::MicroSeconds(1ULL << 56),
            EAggregationType::AT_MAX,
            EMetricType::MT_ABSOLUTE);
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',",
            Data.Str());

        Visitor->OnMetricEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "},",
            Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintSumDerMetric, TEnv)
    {
        Visitor->OnMetricBegin(
            TInstant::MicroSeconds(1ULL << 56),
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'sum',"
            "metric_type:'derivative',",
            Data.Str());

        Visitor->OnMetricEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'sum',"
            "metric_type:'derivative',"
            "},",
            Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintAvgDerMetric, TEnv)
    {
        Visitor->OnMetricBegin(
            TInstant::MicroSeconds(1ULL << 56),
            EAggregationType::AT_AVG,
            EMetricType::MT_DERIVATIVE);
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'average',"
            "metric_type:'derivative',",
            Data.Str());

        Visitor->OnMetricEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'average',"
            "metric_type:'derivative',"
            "},",
            Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintMinDerMetric, TEnv)
    {
        Visitor->OnMetricBegin(
            TInstant::MicroSeconds(1ULL << 56),
            EAggregationType::AT_MIN,
            EMetricType::MT_DERIVATIVE);
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'min',"
            "metric_type:'derivative',",
            Data.Str());

        Visitor->OnMetricEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'min',"
            "metric_type:'derivative',"
            "},",
            Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintMaxDerMetric, TEnv)
    {
        Visitor->OnMetricBegin(
            TInstant::MicroSeconds(1ULL << 56),
            EAggregationType::AT_MAX,
            EMetricType::MT_DERIVATIVE);
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'max',"
            "metric_type:'derivative',",
            Data.Str());

        Visitor->OnMetricEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'max',"
            "metric_type:'derivative',"
            "},",
            Data.Str());
    }

    // TODO: Other types

    Y_UNIT_TEST_F(ShouldPrintLabels, TEnv)
    {
        Visitor->OnLabelsBegin();
        UNIT_ASSERT_VALUES_EQUAL("labels:[", Data.Str());

        Visitor->OnLabel("service", "test_service");
        UNIT_ASSERT_VALUES_EQUAL(
            "labels:["
            "{name:'service',value:'test_service'},",
            Data.Str());

        Visitor->OnLabel("volume", "test_fs");
        UNIT_ASSERT_VALUES_EQUAL(
            "labels:["
            "{name:'service',value:'test_service'},"
            "{name:'volume',value:'test_fs'},",
            Data.Str());

        Visitor->OnLabel("sensor", "test_sensor");
        UNIT_ASSERT_VALUES_EQUAL(
            "labels:["
            "{name:'service',value:'test_service'},"
            "{name:'volume',value:'test_fs'},"
            "{name:'sensor',value:'test_sensor'},",
            Data.Str());

        Visitor->OnLabelsEnd();
        UNIT_ASSERT_VALUES_EQUAL(
            "labels:["
            "{name:'service',value:'test_service'},"
            "{name:'volume',value:'test_fs'},"
            "{name:'sensor',value:'test_sensor'},"
            "],",
            Data.Str());
    }

    Y_UNIT_TEST_F(ShouldPrintI64Value, TEnv)
    {
        Visitor->OnValue(42);
        UNIT_ASSERT_VALUES_EQUAL("value:'42',", Data.Str());
    }

    // TODO: Other metric values

    Y_UNIT_TEST_F(ShouldPrintCorrectly, TEnv)
    {
        Visitor->OnStreamBegin();
        {
            // First metric
            Visitor->OnMetricBegin(
                TInstant::MicroSeconds(1ULL << 55),
                EAggregationType::AT_SUM,
                EMetricType::MT_ABSOLUTE);
            {
                Visitor->OnLabelsBegin();
                {
                    Visitor->OnLabel("service", "first_service");
                    Visitor->OnLabel("volume", "first_fs");
                    Visitor->OnLabel("sensor", "first_sensor");
                }
                Visitor->OnLabelsEnd();

                Visitor->OnValue(42);
            }
            Visitor->OnMetricEnd();

            // Second metric
            Visitor->OnMetricBegin(
                TInstant::MicroSeconds(1ULL << 56),
                EAggregationType::AT_AVG,
                EMetricType::MT_DERIVATIVE);
            {
                Visitor->OnLabelsBegin();
                {
                    Visitor->OnLabel("service", "second_service");
                    Visitor->OnLabel("host", "second_host");
                    Visitor->OnLabel("volume_service", "second_fs");
                    Visitor->OnLabel("sensor", "second_sensor");
                }
                Visitor->OnLabelsEnd();

                Visitor->OnValue(322);
            }
            Visitor->OnMetricEnd();
        }
        Visitor->OnStreamEnd();

        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'3111-09-16T23:10:18Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'service',value:'first_service'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'sensor',value:'first_sensor'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'4253-05-31T22:20:37Z',"
            "aggregation_type:'average',"
            "metric_type:'derivative',"
            "labels:["
            "{name:'service',value:'second_service'},"
            "{name:'host',value:'second_host'},"
            "{name:'volume_service',value:'second_fs'},"
            "{name:'sensor',value:'second_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
    }
}

}   // namespace NCloud::NFileStore::NMetrics
