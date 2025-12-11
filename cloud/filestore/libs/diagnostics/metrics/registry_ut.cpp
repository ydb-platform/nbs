#include "registry.h"

#include "key.h"
#include "label.h"
#include "metric.h"
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
        , Registry(CreateMetricsRegistry({}, Counters))
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

    void SetupRegistry(TLabels commonLabels = {})
    {
        Registry = CreateMetricsRegistry(std::move(commonLabels), Counters);
    }

    void DestroyRegistry()
    {
        Registry.reset();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMetricRegistryTest)
{
    ////////////////////////////////////////////////////////////////////////////////
    //// Registry
    ////////////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST_F(ShouldRegisterAbs, TEnv)
    {
        std::atomic<i64> value(42);
        Registry->Register({CreateSensor("test_sensor")}, value);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldRegisterDer, TEnv)
    {
        std::atomic<i64> value(42);
        Registry->Register(
            {CreateSensor("test_sensor")},
            value,
            EAggregationType::AT_AVG,
            EMetricType::MT_DERIVATIVE);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'derivative',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        DestroyRegistry();
    }

    Y_UNIT_TEST_F(ShouldRegisterAndAggregateAbs, TEnv)
    {
        std::atomic<i64> value(42);
        Registry->Register({CreateSensor("test_sensor")}, value);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        value = 17;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'17',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldRegisterAndAggregateDer, TEnv)
    {
        std::atomic<i64> value(42);
        Registry->Register(
            {CreateSensor("test_sensor")},
            value,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'derivative',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        value += 17;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'derivative',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'17',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        DestroyRegistry();
    }

    Y_UNIT_TEST_F(ShouldUnregisterAbs, TEnv)
    {
        std::atomic<i64> value(42);
        const auto key =
            Registry->Register({CreateSensor("test_sensor")}, value);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(key);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterDer, TEnv)
    {
        std::atomic<i64> value(42);
        const auto key = Registry->Register(
            {CreateSensor("test_sensor")},
            value,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'derivative',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(key);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterAndAggregateAbs, TEnv)
    {
        std::atomic<i64> value(42);
        const auto key =
            Registry->Register({CreateSensor("test_sensor")}, value);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(key);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterAndAggregateDer, TEnv)
    {
        std::atomic<i64> value(42);
        const auto key = Registry->Register(
            {CreateSensor("test_sensor")},
            value,
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'derivative',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        value += 123;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'derivative',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'123',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(key);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldMultipleRegisterUnregister, TEnv)
    {
        std::atomic<i64> firstValue(42);
        const auto firstKey =
            Registry->Register({CreateSensor("first_sensor")}, firstValue);

        std::atomic<i64> secondValue(322);
        const auto secondKey = Registry->Register(
            {CreateLabel("test_name", "test_value"),
             CreateSensor("second_sensor")},
            secondValue);

        // Keys for every counter must be unique.
        UNIT_ASSERT(firstKey != secondKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'first_sensor'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_name',value:'test_value'},"
            "{name:'sensor',value:'second_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstValue = 17;

        // First metric must be changed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'first_sensor'},"
            "],"
            "value:'17',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_name',value:'test_value'},"
            "{name:'sensor',value:'second_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        secondValue -= 22;

        // Second metric must be changed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'first_sensor'},"
            "],"
            "value:'17',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_name',value:'test_value'},"
            "{name:'sensor',value:'second_sensor'},"
            "],"
            "value:'300',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(firstKey);

        // First metric must be removed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_name',value:'test_value'},"
            "{name:'sensor',value:'second_sensor'},"
            "],"
            "value:'300',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(secondKey);

        // Second metric must be removed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldMultipleRegisterUnregisterWithCommonLabels, TEnv)
    {
        SetupRegistry({
            CreateLabel("test_project", "filestore"),
            CreateLabel("test_component", "test_fs"),
        });

        std::atomic<i64> firstValue(42);
        const auto firstKey =
            Registry->Register({CreateSensor("first_sensor")}, firstValue);

        std::atomic<i64> secondValue(322);
        const auto secondKey = Registry->Register(
            {CreateLabel("test_name", "test_value"),
             CreateSensor("second_sensor")},
            secondValue);

        // Keys for every counter must be unique.
        UNIT_ASSERT(firstKey != secondKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'test_fs'},"
            "{name:'sensor',value:'first_sensor'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'test_fs'},"
            "{name:'test_name',value:'test_value'},"
            "{name:'sensor',value:'second_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        secondValue -= 22;

        // Second metric must be changed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'test_fs'},"
            "{name:'sensor',value:'first_sensor'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'test_fs'},"
            "{name:'test_name',value:'test_value'},"
            "{name:'sensor',value:'second_sensor'},"
            "],"
            "value:'300',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstValue = 17;

        // First metric must be changed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'test_fs'},"
            "{name:'sensor',value:'first_sensor'},"
            "],"
            "value:'17',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'test_fs'},"
            "{name:'test_name',value:'test_value'},"
            "{name:'sensor',value:'second_sensor'},"
            "],"
            "value:'300',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(secondKey);

        // Second metric must be removed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'test_fs'},"
            "{name:'sensor',value:'first_sensor'},"
            "],"
            "value:'17',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(firstKey);

        // First metric must be removed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldRegisterAggregatable, TEnv)
    {
        SetupRegistry({
            CreateLabel("test_project", "filestore"),
            CreateLabel("test_component", "service"),
        });

        std::atomic<i64> firstValue(42);
        const auto firstKey =
            Registry->Register({CreateSensor("test_sensor")}, firstValue);

        std::atomic<i64> secondValue(322);
        const auto secondKey =
            Registry->Register({CreateSensor("test_sensor")}, secondValue);

        // Keys for every counter must be unique.
        UNIT_ASSERT(firstKey != secondKey);

        // value == 364, because 322 + 42 = 364.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'service'},"
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'364',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        secondValue -= 22;

        // value == 342, because (322 - 22) + 42 = 342.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'service'},"
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'342',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstValue = 17;

        // value == 317, because (322 - 22) + 17 = 317.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'service'},"
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'317',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterAggregatable, TEnv)
    {
        SetupRegistry({
            CreateLabel("test_project", "filestore"),
            CreateLabel("test_component", "service"),
        });

        std::atomic<i64> firstValue(42);
        const auto firstKey =
            Registry->Register({CreateSensor("test_sensor")}, firstValue);

        std::atomic<i64> secondValue(322);
        const auto secondKey =
            Registry->Register({CreateSensor("test_sensor")}, secondValue);

        // Keys for every counter must be unique.
        UNIT_ASSERT(firstKey != secondKey);

        // value == 364, because 322 + 42 = 364.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'service'},"
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'364',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(firstKey);

        // value == 322, because first counter was unregistered.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'service'},"
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        secondValue = 123;

        // value == 123, because second counter was changed.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'test_component',value:'service'},"
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'123',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(secondKey);

        // Aggregator was erased, because the last counter had been erased.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldMainRegistryRemoveSubgroups, TEnv)
    {
        SetupRegistry({CreateLabel("test_project", "filestore")});

        std::atomic<i64> firstFsUsedBytes(0);
        Registry->Register(
            {CreateSensor("used_bytes_first")},
            firstFsUsedBytes);
        Registry->Register(
            {CreateLabel("service", "common"), CreateSensor("used_bytes")},
            firstFsUsedBytes);

        std::atomic<i64> secondFsUsedBytes(0);
        Registry->Register(
            {CreateSensor("used_bytes_second")},
            secondFsUsedBytes);
        Registry->Register(
            {CreateLabel("service", "common"), CreateSensor("used_bytes")},
            secondFsUsedBytes);

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "    sensor=used_bytes_first: 0\n"
            "    sensor=used_bytes_second: 0\n"
            "\n"
            "    service=common:\n"
            "        sensor=used_bytes: 0\n",
            Data.Str());
        Data.Clear();

        firstFsUsedBytes = 1024;
        secondFsUsedBytes = 4096;

        // Counters don't have values because update was not called.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "    sensor=used_bytes_first: 0\n"
            "    sensor=used_bytes_second: 0\n"
            "\n"
            "    service=common:\n"
            "        sensor=used_bytes: 0\n",
            Data.Str());
        Data.Clear();

        Registry->Update(TInstant::Now());

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "    sensor=used_bytes_first: 1024\n"
            "    sensor=used_bytes_second: 4096\n"
            "\n"
            "    service=common:\n"
            "        sensor=used_bytes: 5120\n",
            Data.Str());
        Data.Clear();

        DestroyRegistry();

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL("", Data.Str());
        Data.Clear();
    }

    ////////////////////////////////////////////////////////////////////////////////
    //// Scoped Registry
    ////////////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST_F(ShouldRegisterScopedRegistry, TEnv)
    {
        std::atomic<i64> value(42);
        const auto key =
            Registry->Register({CreateSensor("test_sensor")}, value);

        auto scopedRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("test_common_label", "test_common_value")},
            Registry);
        std::atomic<i64> subValue(322);
        const auto subKey = scopedRegistry->Register(
            {CreateSensor("test_sub_sensor")},
            subValue);

        UNIT_ASSERT(key != subKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        auto output = Data.Str();
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_common_label',value:'test_common_value'},"
            "{name:'sensor',value:'test_sub_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        value = 17;
        subValue = 27;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'17',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_common_label',value:'test_common_value'},"
            "{name:'sensor',value:'test_sub_sensor'},"
            "],"
            "value:'27',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterScopedRegistry, TEnv)
    {
        std::atomic<i64> value(42);
        const auto key =
            Registry->Register({CreateSensor("test_sensor")}, value);

        auto scopedRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("test_common_label", "test_common_value")},
            Registry);
        std::atomic<i64> subValue(322);
        const auto subKey = scopedRegistry->Register(
            {CreateSensor("test_sub_sensor")},
            subValue);

        UNIT_ASSERT(key != subKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_common_label',value:'test_common_value'},"
            "{name:'sensor',value:'test_sub_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        Registry->Unregister(key);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_common_label',value:'test_common_value'},"
            "{name:'sensor',value:'test_sub_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        scopedRegistry->Unregister(subKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterOnlyOwnedScopedRegistry, TEnv)
    {
        std::atomic<i64> value(42);
        const auto key =
            Registry->Register({CreateSensor("test_sensor")}, value);

        auto scopedRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("test_common_label", "test_common_value")},
            Registry);
        std::atomic<i64> subValue(322);
        const auto subKey = scopedRegistry->Register(
            {CreateSensor("test_sub_sensor")},
            subValue);

        UNIT_ASSERT(key != subKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_common_label',value:'test_common_value'},"
            "{name:'sensor',value:'test_sub_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        scopedRegistry->Unregister(key);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_common_label',value:'test_common_value'},"
            "{name:'sensor',value:'test_sub_sensor'},"
            "],"
            "value:'322',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        scopedRegistry->Unregister(subKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterScopedRegistryOnDestroy, TEnv)
    {
        std::atomic<i64> value(42);
        const auto key =
            Registry->Register({CreateSensor("test_sensor")}, value);

        {
            auto scopedRegistry = CreateScopedMetricsRegistry(
                {CreateLabel("test_common_label", "test_common_value")},
                Registry);
            std::atomic<i64> subValue(322);
            const auto subKey = scopedRegistry->Register(
                {CreateSensor("test_sub_sensor")},
                subValue);

            UNIT_ASSERT(key != subKey);

            Registry->Visit(TInstant::Zero(), *Visitor);
            UNIT_ASSERT_VALUES_EQUAL(
                "metrics:["
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'sensor',value:'test_sensor'},"
                "],"
                "value:'42',"
                "},"
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'test_common_label',value:'test_common_value'},"
                "{name:'sensor',value:'test_sub_sensor'},"
                "],"
                "value:'322',"
                "},"
                "]",
                Data.Str());
            Data.Clear();
        }

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor'},"
            "],"
            "value:'42',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldNotFailOnBaseRegistryDestruction, TEnv)
    {
        std::atomic<i64> first(42);
        Registry->Register({CreateSensor("test_sensor_1")}, first);

        auto scopedBaseRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("test_base_label", "label")},
            Registry);

        std::atomic<i64> second(42);
        scopedBaseRegistry->Register({CreateSensor("test_sensor_2")}, second);

        auto scopedRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("test_label", "label")},
            scopedBaseRegistry);

        std::atomic<i64> third(322);
        scopedRegistry->Register({CreateSensor("test_sensor_3")}, third);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_base_label',value:'label'},"
            "{name:'test_label',value:'label'},"
            "{name:'sensor',value:'test_sensor_3'},"
            "],"
            "value:'322',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_base_label',value:'label'},"
            "{name:'sensor',value:'test_sensor_2'},"
            "],"
            "value:'42',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor_1'},"
            "],"
            "value:'42',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        scopedBaseRegistry.reset();
        UNIT_ASSERT(!scopedBaseRegistry);

        first = 1;
        second = 2;
        third = 3;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_base_label',value:'label'},"
            "{name:'test_label',value:'label'},"
            "{name:'sensor',value:'test_sensor_3'},"
            "],"
            "value:'3',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_base_label',value:'label'},"
            "{name:'sensor',value:'test_sensor_2'},"
            "],"
            "value:'2',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'sensor',value:'test_sensor_1'},"
            "],"
            "value:'1',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldAggregateScopedRegistry, TEnv)
    {
        SetupRegistry({CreateLabel("test_project", "filestore")});

        auto serviceRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service")},
            Registry);

        auto serviceVolumeRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service_volume")},
            Registry);

        auto firstFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "first_fs")},
            serviceVolumeRegistry);
        auto secondFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "second_fs")},
            serviceVolumeRegistry);

        std::atomic<i64> firstFsUsedBytes(0);
        const auto firstFsUsedBytesKey = firstFsRegistry->Register(
            {CreateSensor("used_bytes")},
            firstFsUsedBytes);

        std::atomic<i64> secondFsUsedBytes(0);
        const auto secondFsUsedBytesKey = secondFsRegistry->Register(
            {CreateSensor("used_bytes")},
            secondFsUsedBytes);

        const auto firstFsServiceUsedBytesKey = serviceRegistry->Register(
            {CreateSensor("used_bytes")},
            firstFsUsedBytes);
        const auto secondFsServiceUsedBytesKey = serviceRegistry->Register(
            {CreateSensor("used_bytes")},
            secondFsUsedBytes);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsUsedBytes = 1024;
        secondFsUsedBytes = 4096;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'4096',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'1024',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'5120',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsRegistry->Unregister(firstFsUsedBytesKey);

        // Unregistered only first fs counter, but service aggregation was not
        // touched.
        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'4096',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'5120',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        serviceRegistry->Unregister(firstFsServiceUsedBytesKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'4096',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'4096',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        secondFsUsedBytes -= 1024;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'3072',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'3072',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        serviceRegistry->Unregister(secondFsServiceUsedBytesKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'3072',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        secondFsRegistry->Unregister(secondFsUsedBytesKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldScopedRegistryRemoveSubgroups, TEnv)
    {
        SetupRegistry({CreateLabel("test_project", "filestore")});

        auto serviceRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service")},
            Registry);

        auto serviceVolumeRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service_volume")},
            Registry);

        auto firstFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "first_fs")},
            serviceVolumeRegistry);
        auto secondFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "second_fs")},
            serviceVolumeRegistry);

        std::atomic<i64> firstFsUsedBytes(0);
        const auto firstFsUsedBytesKey = firstFsRegistry->Register(
            {CreateSensor("used_bytes")},
            firstFsUsedBytes);

        std::atomic<i64> secondFsUsedBytes(0);
        const auto secondFsUsedBytesKey = secondFsRegistry->Register(
            {CreateSensor("used_bytes")},
            secondFsUsedBytes);

        const auto firstFsServiceUsedBytesKey = serviceRegistry->Register(
            {CreateSensor("used_bytes")},
            firstFsUsedBytes);
        const auto secondFsServiceUsedBytesKey = serviceRegistry->Register(
            {CreateSensor("used_bytes")},
            secondFsUsedBytes);

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "\n"
            "    service=service:\n"
            "        sensor=used_bytes: 0\n"
            "\n"
            "    service=service_volume:\n"
            "\n"
            "        volume=first_fs:\n"
            "            sensor=used_bytes: 0\n"
            "\n"
            "        volume=second_fs:\n"
            "            sensor=used_bytes: 0\n",
            Data.Str());
        Data.Clear();

        firstFsUsedBytes = 1024;
        secondFsUsedBytes = 4096;

        // Counters don't have values because update was not called.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "\n"
            "    service=service:\n"
            "        sensor=used_bytes: 0\n"
            "\n"
            "    service=service_volume:\n"
            "\n"
            "        volume=first_fs:\n"
            "            sensor=used_bytes: 0\n"
            "\n"
            "        volume=second_fs:\n"
            "            sensor=used_bytes: 0\n",
            Data.Str());
        Data.Clear();

        Registry->Update(TInstant::Now());

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "\n"
            "    service=service:\n"
            "        sensor=used_bytes: 5120\n"
            "\n"
            "    service=service_volume:\n"
            "\n"
            "        volume=first_fs:\n"
            "            sensor=used_bytes: 1024\n"
            "\n"
            "        volume=second_fs:\n"
            "            sensor=used_bytes: 4096\n",
            Data.Str());
        Data.Clear();

        firstFsRegistry->Unregister(firstFsUsedBytesKey);
        Registry->Update(TInstant::Now());

        // Unregistered only first fs counter, but service aggregation was not
        // touched.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "\n"
            "    service=service:\n"
            "        sensor=used_bytes: 5120\n"
            "\n"
            "    service=service_volume:\n"
            "\n"
            "        volume=second_fs:\n"
            "            sensor=used_bytes: 4096\n",
            Data.Str());
        Data.Clear();

        serviceRegistry->Unregister(firstFsServiceUsedBytesKey);
        Registry->Update(TInstant::Now());

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "\n"
            "    service=service:\n"
            "        sensor=used_bytes: 4096\n"
            "\n"
            "    service=service_volume:\n"
            "\n"
            "        volume=second_fs:\n"
            "            sensor=used_bytes: 4096\n",
            Data.Str());
        Data.Clear();

        secondFsUsedBytes -= 1024;
        Registry->Update(TInstant::Now());

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "\n"
            "    service=service:\n"
            "        sensor=used_bytes: 3072\n"
            "\n"
            "    service=service_volume:\n"
            "\n"
            "        volume=second_fs:\n"
            "            sensor=used_bytes: 3072\n",
            Data.Str());
        Data.Clear();

        serviceRegistry->Unregister(secondFsServiceUsedBytesKey);

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "test_project=filestore:\n"
            "\n"
            "    service=service_volume:\n"
            "\n"
            "        volume=second_fs:\n"
            "            sensor=used_bytes: 3072\n",
            Data.Str());
        Data.Clear();

        secondFsRegistry->Unregister(secondFsUsedBytesKey);

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL("", Data.Str());
        Data.Clear();
    }

    ////////////////////////////////////////////////////////////////////////////////
    //// Aggregatable Registry
    ////////////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST_F(ShouldAggregateAggregatableRegistry, TEnv)
    {
        SetupRegistry({CreateLabel("test_project", "filestore")});

        auto serviceRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service")},
            Registry);

        auto serviceVolumeRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service_volume")},
            Registry);

        auto firstFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "first_fs")},
            serviceVolumeRegistry);
        auto secondFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "second_fs")},
            serviceVolumeRegistry);

        auto firstFsAggregatableRegistry =
            CreateScopedMetricsRegistry({}, {serviceRegistry, firstFsRegistry});
        auto secondFsAggregatableRegistry = CreateScopedMetricsRegistry(
            {},
            {serviceRegistry, secondFsRegistry});

        std::atomic<i64> firstFsUsedBytes(0);
        std::atomic<i64> secondFsUsedBytes(0);

        const auto firstKey = firstFsAggregatableRegistry->Register(
            {CreateSensor("used_bytes")},
            firstFsUsedBytes);
        const auto secondKey = secondFsAggregatableRegistry->Register(
            {CreateSensor("used_bytes")},
            secondFsUsedBytes);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsUsedBytes = 1024;
        secondFsUsedBytes = 4096;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'4096',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'1024',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'5120',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsAggregatableRegistry->Unregister(firstKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'4096',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'4096',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        secondFsUsedBytes -= 1024;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'3072',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'3072',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        secondFsAggregatableRegistry->Unregister(secondKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldAggregateAllTypesAggregatableRegistry, TEnv)
    {
        SetupRegistry({CreateLabel("test_project", "filestore")});

        auto serviceRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service")},
            Registry);

        auto serviceVolumeRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service_volume")},
            Registry);

        auto firstFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "first_fs")},
            serviceVolumeRegistry);
        auto secondFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "second_fs")},
            serviceVolumeRegistry);

        auto firstFsAggregatableRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("request", "inflight")},
            {serviceRegistry, firstFsRegistry});
        auto secondFsAggregatableRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("request", "inflight")},
            {serviceRegistry, secondFsRegistry});

        std::array<std::atomic<i64>, 3> firstFsRequests{0};
        std::array<std::atomic<i64>, 3> secondFsRequests{0};

        std::array<std::array<TMetricKey, 4>, 3> firstFsKeys;
        std::array<std::array<TMetricKey, 4>, 2> secondFsKeys;

        {
            firstFsKeys[0][static_cast<size_t>(EAggregationType::AT_SUM)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "sum")},
                    firstFsRequests[0],
                    EAggregationType::AT_SUM);
            firstFsKeys[1][static_cast<size_t>(EAggregationType::AT_SUM)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "sum")},
                    firstFsRequests[1],
                    EAggregationType::AT_SUM);
            firstFsKeys[2][static_cast<size_t>(EAggregationType::AT_SUM)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "sum")},
                    firstFsRequests[2],
                    EAggregationType::AT_SUM);

            secondFsKeys[0][static_cast<size_t>(EAggregationType::AT_SUM)] =
                secondFsAggregatableRegistry->Register(
                    {CreateLabel("size", "sum")},
                    secondFsRequests[0],
                    EAggregationType::AT_SUM);
            secondFsKeys[1][static_cast<size_t>(EAggregationType::AT_SUM)] =
                secondFsAggregatableRegistry->Register(
                    {CreateLabel("size", "sum")},
                    secondFsRequests[1],
                    EAggregationType::AT_SUM);
        }

        {
            firstFsKeys[0][static_cast<size_t>(EAggregationType::AT_AVG)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "avg")},
                    firstFsRequests[0],
                    EAggregationType::AT_AVG);
            firstFsKeys[1][static_cast<size_t>(EAggregationType::AT_AVG)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "avg")},
                    firstFsRequests[1],
                    EAggregationType::AT_AVG);
            firstFsKeys[2][static_cast<size_t>(EAggregationType::AT_AVG)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "avg")},
                    firstFsRequests[2],
                    EAggregationType::AT_AVG);

            secondFsKeys[0][static_cast<size_t>(EAggregationType::AT_AVG)] =
                secondFsAggregatableRegistry->Register(
                    {CreateLabel("size", "avg")},
                    secondFsRequests[0],
                    EAggregationType::AT_AVG);
            secondFsKeys[1][static_cast<size_t>(EAggregationType::AT_AVG)] =
                secondFsAggregatableRegistry->Register(
                    {CreateLabel("size", "avg")},
                    secondFsRequests[1],
                    EAggregationType::AT_AVG);
        }

        {
            firstFsKeys[0][static_cast<size_t>(EAggregationType::AT_MIN)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "min")},
                    firstFsRequests[0],
                    EAggregationType::AT_MIN);
            firstFsKeys[1][static_cast<size_t>(EAggregationType::AT_MIN)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "min")},
                    firstFsRequests[1],
                    EAggregationType::AT_MIN);
            firstFsKeys[2][static_cast<size_t>(EAggregationType::AT_MIN)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "min")},
                    firstFsRequests[2],
                    EAggregationType::AT_MIN);

            secondFsKeys[0][static_cast<size_t>(EAggregationType::AT_MIN)] =
                secondFsAggregatableRegistry->Register(
                    {CreateLabel("size", "min")},
                    secondFsRequests[0],
                    EAggregationType::AT_MIN);
            secondFsKeys[1][static_cast<size_t>(EAggregationType::AT_MIN)] =
                secondFsAggregatableRegistry->Register(
                    {CreateLabel("size", "min")},
                    secondFsRequests[1],
                    EAggregationType::AT_MIN);
        }

        {
            firstFsKeys[0][static_cast<size_t>(EAggregationType::AT_MAX)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "max")},
                    firstFsRequests[0],
                    EAggregationType::AT_MAX);
            firstFsKeys[1][static_cast<size_t>(EAggregationType::AT_MAX)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "max")},
                    firstFsRequests[1],
                    EAggregationType::AT_MAX);
            firstFsKeys[2][static_cast<size_t>(EAggregationType::AT_MAX)] =
                firstFsAggregatableRegistry->Register(
                    {CreateLabel("size", "max")},
                    firstFsRequests[2],
                    EAggregationType::AT_MAX);

            secondFsKeys[0][static_cast<size_t>(EAggregationType::AT_MAX)] =
                secondFsAggregatableRegistry->Register(
                    {CreateLabel("size", "max")},
                    secondFsRequests[0],
                    EAggregationType::AT_MAX);
            secondFsKeys[1][static_cast<size_t>(EAggregationType::AT_MAX)] =
                secondFsAggregatableRegistry->Register(
                    {CreateLabel("size", "max")},
                    secondFsRequests[1],
                    EAggregationType::AT_MAX);
        }

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'0',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'0',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsRequests[0] = 1024;
        firstFsRequests[1] = 4096;
        firstFsRequests[2] = 65536;

        secondFsRequests[0] = 3072;
        secondFsRequests[1] = 618;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'618',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'14869',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'65536',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'3690',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'618',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'65536',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'70656',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'1845',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'3072',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'1024',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'74346',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'23552',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsAggregatableRegistry->Unregister(
            firstFsKeys[1][static_cast<size_t>(EAggregationType::AT_SUM)]);
        firstFsAggregatableRegistry->Unregister(
            firstFsKeys[1][static_cast<size_t>(EAggregationType::AT_AVG)]);
        firstFsAggregatableRegistry->Unregister(
            firstFsKeys[1][static_cast<size_t>(EAggregationType::AT_MIN)]);
        firstFsAggregatableRegistry->Unregister(
            firstFsKeys[1][static_cast<size_t>(EAggregationType::AT_MAX)]);

        firstFsRequests[0] = 2048;
        firstFsRequests[2] = 512;

        secondFsRequests[0] = 32768;
        secondFsRequests[1] = 18547;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'512',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'13468',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'2048',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'51315',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'18547',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'32768',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'2560',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'25657',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'second_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'32768',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'512',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'53875',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'1280',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsRequests[0] = 123;

        secondFsAggregatableRegistry.reset();

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'123',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'317',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'512',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'max',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'max'},"
            "],"
            "value:'512',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'635',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'min',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'min'},"
            "],"
            "value:'123',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'sum'},"
            "],"
            "value:'635',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'average',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'request',value:'inflight'},"
            "{name:'size',value:'avg'},"
            "],"
            "value:'317',"
            "},"
            "]",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterAggregatableRegistryOnDestroy, TEnv)
    {
        SetupRegistry({CreateLabel("test_project", "filestore")});

        auto serviceRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service")},
            Registry);

        auto serviceVolumeRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service_volume")},
            Registry);

        auto firstFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "first_fs")},
            serviceVolumeRegistry);
        auto secondFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "second_fs")},
            serviceVolumeRegistry);

        auto firstFsAggregatableRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("label", "common")},
            {serviceRegistry, firstFsRegistry});

        std::atomic<i64> firstFsUsedBytes(0);
        const auto firstKey = firstFsAggregatableRegistry->Register(
            {CreateSensor("used_bytes")},
            firstFsUsedBytes);

        {
            std::atomic<i64> secondFsUsedBytes(0);
            auto secondFsAggregatableRegistry = CreateScopedMetricsRegistry(
                {CreateLabel("label", "common")},
                {serviceRegistry, secondFsRegistry});

            secondFsAggregatableRegistry->Register(
                {CreateSensor("used_bytes")},
                secondFsUsedBytes);

            Registry->Visit(TInstant::Zero(), *Visitor);
            UNIT_ASSERT_VALUES_EQUAL(
                "metrics:["
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'test_project',value:'filestore'},"
                "{name:'service',value:'service_volume'},"
                "{name:'volume',value:'second_fs'},"
                "{name:'label',value:'common'},"
                "{name:'sensor',value:'used_bytes'},"
                "],"
                "value:'0',"
                "},"
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'test_project',value:'filestore'},"
                "{name:'service',value:'service_volume'},"
                "{name:'volume',value:'first_fs'},"
                "{name:'label',value:'common'},"
                "{name:'sensor',value:'used_bytes'},"
                "],"
                "value:'0',"
                "},"
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'test_project',value:'filestore'},"
                "{name:'service',value:'service'},"
                "{name:'label',value:'common'},"
                "{name:'sensor',value:'used_bytes'},"
                "],"
                "value:'0',"
                "},"
                "]",
                Data.Str());
            Data.Clear();

            firstFsUsedBytes = 1024;
            secondFsUsedBytes = 4096;

            Registry->Visit(TInstant::Zero(), *Visitor);
            UNIT_ASSERT_VALUES_EQUAL(
                "metrics:["
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'test_project',value:'filestore'},"
                "{name:'service',value:'service_volume'},"
                "{name:'volume',value:'second_fs'},"
                "{name:'label',value:'common'},"
                "{name:'sensor',value:'used_bytes'},"
                "],"
                "value:'4096',"
                "},"
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'test_project',value:'filestore'},"
                "{name:'service',value:'service_volume'},"
                "{name:'volume',value:'first_fs'},"
                "{name:'label',value:'common'},"
                "{name:'sensor',value:'used_bytes'},"
                "],"
                "value:'1024',"
                "},"
                "{"
                "time:'1970-01-01T00:00:00Z',"
                "aggregation_type:'sum',"
                "metric_type:'absolute',"
                "labels:["
                "{name:'test_project',value:'filestore'},"
                "{name:'service',value:'service'},"
                "{name:'label',value:'common'},"
                "{name:'sensor',value:'used_bytes'},"
                "],"
                "value:'5120',"
                "},"
                "]",
                Data.Str());
            Data.Clear();
        }

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'label',value:'common'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'1024',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'label',value:'common'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'1024',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsUsedBytes += 1024;

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(
            "metrics:["
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service_volume'},"
            "{name:'volume',value:'first_fs'},"
            "{name:'label',value:'common'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'2048',"
            "},"
            "{"
            "time:'1970-01-01T00:00:00Z',"
            "aggregation_type:'sum',"
            "metric_type:'absolute',"
            "labels:["
            "{name:'test_project',value:'filestore'},"
            "{name:'service',value:'service'},"
            "{name:'label',value:'common'},"
            "{name:'sensor',value:'used_bytes'},"
            "],"
            "value:'2048',"
            "},"
            "]",
            Data.Str());
        Data.Clear();

        firstFsAggregatableRegistry->Unregister(firstKey);

        Registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL("metrics:[]", Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUnregisterSubgroupsAggregatableRegistryOnDestroy, TEnv)
    {
        SetupRegistry({CreateLabel("test_project", "filestore")});

        auto serviceRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service")},
            Registry);

        auto serviceVolumeRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service_volume")},
            Registry);

        auto firstFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "first_fs")},
            serviceVolumeRegistry);
        auto secondFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "second_fs")},
            serviceVolumeRegistry);

        TInstant start = TInstant::Now();

        {
            std::atomic<i64> firstFsUsedBytes(123);
            auto firstFsAggregatableRegistry = CreateScopedMetricsRegistry(
                {CreateLabel("label", "common")},
                {serviceRegistry, firstFsRegistry});

            firstFsAggregatableRegistry->Register(
                {CreateSensor("used_bytes")},
                firstFsUsedBytes,
                EAggregationType::AT_AVG,
                EMetricType::MT_DERIVATIVE);

            {
                std::atomic<i64> secondFsUsedBytes(456);
                auto secondFsAggregatableRegistry = CreateScopedMetricsRegistry(
                    {CreateLabel("label", "common")},
                    {serviceRegistry, secondFsRegistry});

                secondFsAggregatableRegistry->Register(
                    {CreateSensor("used_bytes")},
                    secondFsUsedBytes,
                    EAggregationType::AT_AVG,
                    EMetricType::MT_DERIVATIVE);

                Registry->Update(start);

                Counters->OutputPlainText(Data);
                UNIT_ASSERT_VALUES_EQUAL(
                    "\n"
                    "test_project=filestore:\n"
                    "\n"
                    "    service=service:\n"
                    "\n"
                    "        label=common:\n"
                    "            sensor=used_bytes: 0\n"
                    "\n"
                    "    service=service_volume:\n"
                    "\n"
                    "        volume=first_fs:\n"
                    "\n"
                    "            label=common:\n"
                    "                sensor=used_bytes: 0\n"
                    "\n"
                    "        volume=second_fs:\n"
                    "\n"
                    "            label=common:\n"
                    "                sensor=used_bytes: 0\n",
                    Data.Str());
                Data.Clear();

                firstFsUsedBytes = 1024;
                secondFsUsedBytes = 4096;
                Registry->Update(start + TDuration::Seconds(15));

                Counters->OutputPlainText(Data);
                UNIT_ASSERT_VALUES_EQUAL(
                    "\n"
                    "test_project=filestore:\n"
                    "\n"
                    "    service=service:\n"
                    "\n"
                    "        label=common:\n"
                    "            sensor=used_bytes: 2270\n"
                    "\n"
                    "    service=service_volume:\n"
                    "\n"
                    "        volume=first_fs:\n"
                    "\n"
                    "            label=common:\n"
                    "                sensor=used_bytes: 901\n"
                    "\n"
                    "        volume=second_fs:\n"
                    "\n"
                    "            label=common:\n"
                    "                sensor=used_bytes: 3640\n",
                    Data.Str());
                Data.Clear();
            }

            Registry->Update(start + TDuration::Seconds(30));

            Counters->OutputPlainText(Data);
            UNIT_ASSERT_VALUES_EQUAL(
                "\n"
                "test_project=filestore:\n"
                "\n"
                "    service=service:\n"
                "\n"
                "        label=common:\n"
                "            sensor=used_bytes: 4541\n"
                "\n"
                "    service=service_volume:\n"
                "\n"
                "        volume=first_fs:\n"
                "\n"
                "            label=common:\n"
                "                sensor=used_bytes: 901\n",
                Data.Str());
            Data.Clear();

            firstFsUsedBytes += 1024;
            Registry->Update(start + TDuration::Seconds(45));

            Counters->OutputPlainText(Data);
            UNIT_ASSERT_VALUES_EQUAL(
                "\n"
                "test_project=filestore:\n"
                "\n"
                "    service=service:\n"
                "\n"
                "        label=common:\n"
                "            sensor=used_bytes: 5565\n"
                "\n"
                "    service=service_volume:\n"
                "\n"
                "        volume=first_fs:\n"
                "\n"
                "            label=common:\n"
                "                sensor=used_bytes: 1925\n",
                Data.Str());
            Data.Clear();
        }

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL("", Data.Str());
        Data.Clear();
    }

    // TODO: Other types (single and multiple)
    // TODO: Other registry
}

}   // namespace NCloud::NFileStore::NMetrics
