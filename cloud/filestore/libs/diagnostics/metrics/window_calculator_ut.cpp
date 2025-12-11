#include "window_calculator.h"

#include "registry.h"
#include "visitor.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NMetrics {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv: public NUnitTest::TBaseFixture
{
    TStringStream Data;
    NMonitoring::TDynamicCountersPtr Counters;

    IRegistryVisitorPtr Visitor;
    IMainMetricsRegistryPtr Registry;

    TEnv()
        : Counters(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , Visitor(CreateRegistryVisitor(Data))
    {
        SetupRegistry();
    }

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

    void SetupRegistry(TLabels commonLabels = {})
    {
        Registry = CreateMetricsRegistry(std::move(commonLabels), Counters);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCalculatorTest)
{
    Y_UNIT_TEST_F(ShouldRegisterAndUnregister, TEnv)
    {
        TDefaultWindowCalculator calc;
        const auto key = calc.Register(
            Registry,
            {CreateLabel("filesystem", "test_fs"), CreateSensor("MaxQuota")});

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Unregister(TMetricKey(*key));

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Unregister(key);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldRegisterWithBaseValue, TEnv)
    {
        TDefaultWindowCalculator calc(3);
        calc.Register(
            Registry,
            {CreateLabel("filesystem", "test_fs"), CreateSensor("MaxQuota")});

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'45',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterOnDestroy, TEnv)
    {
        {
            TDefaultWindowCalculator calc;
            calc.Register(
                Registry,
                {CreateLabel("filesystem", "test_fs"),
                 CreateSensor("MaxQuota")});

            Registry->Visit(TInstant::Zero(), *Visitor);
            UNIT_ASSERT_VALUES_EQUAL(
                "metrics:["
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'filesystem',value:'test_fs'},"
                "{name:'sensor',value:'MaxQuota'},"
                "],"
                "value:'0',"
                "},"
                "]",
                Data.Str());
            Data.Clear();
        }

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldCorrectlyCalculateSum, TEnv)
    {
        TDefaultWindowCalculator calc;
        calc.Register(
            Registry,
            {CreateLabel("filesystem", "test_fs"), CreateSensor("MaxQuota")});

        calc.Record(15);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'15',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(14);
        calc.Record(13);
        calc.Record(12);
        calc.Record(11);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'65',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(10);
        calc.Record(9);
        calc.Record(8);
        calc.Record(7);
        calc.Record(6);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'105',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(5);
        calc.Record(4);
        calc.Record(3);
        calc.Record(2);
        calc.Record(1);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'120',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(0);

        // First value replaced.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'105',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        for (size_t i = 0; i < 14; ++i) {
            calc.Record(0);
        }

        // All values replaced.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(15);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'15',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldCorrectlyCalculateAvg, TEnv)
    {
        TDefaultWindowCalculator calc;
        calc.Register(
            Registry,
            {CreateLabel("filesystem", "test_fs"), CreateSensor("MaxQuota")},
            EAggregationType::AT_AVG);

        calc.Record(15);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'1',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(14);
        calc.Record(13);
        calc.Record(12);
        calc.Record(11);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'4',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(10);
        calc.Record(9);
        calc.Record(8);
        calc.Record(7);
        calc.Record(6);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'7',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(5);
        calc.Record(4);
        calc.Record(3);
        calc.Record(2);
        calc.Record(1);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'8',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(0);

        // First value replaced.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'7',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        for (size_t i = 0; i < 14; ++i) {
            calc.Record(0);
        }

        // All values replaced.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(15);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'1',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldCorrectlyCalculateMin, TEnv)
    {
        TDefaultWindowCalculator calc(30);
        calc.Register(
            Registry,
            {CreateLabel("filesystem", "test_fs"), CreateSensor("MaxQuota")},
            EAggregationType::AT_MIN);

        calc.Record(15);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'15',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(14);
        calc.Record(13);
        calc.Record(12);
        calc.Record(11);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'11',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(10);
        calc.Record(9);
        calc.Record(8);
        calc.Record(7);
        calc.Record(6);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'6',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(5);
        calc.Record(4);
        calc.Record(3);
        calc.Record(2);
        calc.Record(1);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'1',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(0);

        // First value replaced.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        for (size_t i = 0; i < 14; ++i) {
            calc.Record(0);
        }

        // All values replaced.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(15);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldCorrectlyCalculateMax, TEnv)
    {
        TDefaultWindowCalculator calc(-30);
        calc.Register(
            Registry,
            {CreateLabel("filesystem", "test_fs"), CreateSensor("MaxQuota")},
            EAggregationType::AT_MAX);

        calc.Record(15);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'15',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(14);
        calc.Record(13);
        calc.Record(12);
        calc.Record(11);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'15',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(10);
        calc.Record(9);
        calc.Record(8);
        calc.Record(7);
        calc.Record(6);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'15',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(5);
        calc.Record(4);
        calc.Record(3);
        calc.Record(2);
        calc.Record(1);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'15',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(0);

        // First value replaced.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'14',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        for (size_t i = 0; i < 14; ++i) {
            calc.Record(0);
        }

        // All values replaced.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        calc.Record(15);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'filesystem',value:'test_fs'},"
            "{name:'sensor',value:'MaxQuota'},"
            "],"
            "value:'15',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }
}

}   // namespace NCloud::NFileStore::NMetrics
