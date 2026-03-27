#include "module_stats.h"

#include "filesystem_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;
using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t TestMaxBucketCount = 60;

////////////////////////////////////////////////////////////////////////////////

class TTestModuleStats final: public IModuleStats
{
private:
    TString Name;
    std::unique_ptr<TMaxCalculator<TestMaxBucketCount>> MaxCalc;
    std::atomic<i64> MaxValue{0};
    std::atomic<i64> SumValue{0};

public:
    TTestModuleStats(TString name, ITimerPtr timer)
        : Name(std::move(name))
        , MaxCalc(
              std::make_unique<TMaxCalculator<TestMaxBucketCount>>(
                  std::move(timer)))
    {}

    TStringBuf GetName() const override
    {
        return Name;
    }

    void RegisterCounters(
        IMetricsRegistry& localMetricsRegistry,
        IMetricsRegistry& aggregatableMetricsRegistry) override
    {
        localMetricsRegistry.Register({CreateSensor("MaxValue")}, MaxValue);
        aggregatableMetricsRegistry.Register(
            {CreateSensor("SumValue")},
            SumValue,
            EAggregationType::AT_SUM,
            EMetricType::MT_ABSOLUTE);
    }

    void Add(ui64 value)
    {
        MaxCalc->Add(value);
        SumValue.fetch_add(static_cast<i64>(value));
    }

    void UpdateStats(TInstant now) override
    {
        Y_UNUSED(now);

        MaxValue.store(static_cast<i64>(MaxCalc->NextValue()));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    const TString Component = "test";
    const TString FileSystemId = "test_fs";
    const TString ClientId = "test_client";
    const TString CloudId = "test_cloud";
    const TString FolderId = "test_folder";

    ITimerPtr Timer = CreateWallClockTimer();
    TDynamicCountersPtr Counters = MakeIntrusive<TDynamicCounters>();
    IFsCountersProviderPtr FsCountersProvider =
        CreateFsCountersProvider(Component, Counters);
    IModuleStatsRegistryPtr Registry = CreateModuleStatsRegistry(
        Timer,
        FsCountersProvider,
        Counters->GetSubgroup("component", Component));

    std::shared_ptr<TTestModuleStats> CreateAndRegisterStats(
        const TString& moduleName,
        const TString& fsId,
        const TString& clientId)
    {
        auto stats = std::make_shared<TTestModuleStats>(moduleName, Timer);
        Registry->Register(fsId, clientId, CloudId, FolderId, stats);
        return stats;
    }

    std::shared_ptr<TTestModuleStats> CreateAndRegisterStats(
        const TString& moduleName)
    {
        return CreateAndRegisterStats(moduleName, FileSystemId, ClientId);
    }

    TDynamicCountersPtr GetModuleCounters(
        const TString& moduleName,
        const TString& fsId,
        const TString& clientId) const
    {
        return Counters->GetSubgroup("component", Component + "_fs")
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("filesystem", fsId)
            ->GetSubgroup("client", clientId)
            ->GetSubgroup("cloud", CloudId)
            ->GetSubgroup("folder", FolderId)
            ->GetSubgroup("module", moduleName);
    }

    TDynamicCountersPtr GetModuleCounters(const TString& moduleName) const
    {
        return GetModuleCounters(moduleName, FileSystemId, ClientId);
    }

    TDynamicCountersPtr GetAggregateModuleCounters(
        const TString& moduleName) const
    {
        return Counters->GetSubgroup("component", Component)
            ->GetSubgroup("module", moduleName);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TModuleStatsRegistryTest)
{
    Y_UNIT_TEST(ShouldCreateCounterHierarchy)
    {
        TBootstrap b;

        // Register stats to create the counter hierarchy
        auto stats = b.CreateAndRegisterStats("TestModule");

        auto fsCounters =
            b.Counters->FindSubgroup("component", b.Component + "_fs");
        UNIT_ASSERT(fsCounters);

        fsCounters = fsCounters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(fsCounters);

        fsCounters = fsCounters->FindSubgroup("filesystem", b.FileSystemId);
        UNIT_ASSERT(fsCounters);

        fsCounters = fsCounters->FindSubgroup("client", b.ClientId);
        UNIT_ASSERT(fsCounters);

        fsCounters = fsCounters->FindSubgroup("cloud", b.CloudId);
        UNIT_ASSERT(fsCounters);

        fsCounters = fsCounters->FindSubgroup("folder", b.FolderId);
        UNIT_ASSERT(fsCounters);

        fsCounters = fsCounters->FindSubgroup("module", "TestModule");
        UNIT_ASSERT(fsCounters);
    }

    Y_UNIT_TEST(ShouldRegisterAndUpdateStats)
    {
        TBootstrap b;

        auto stats = b.CreateAndRegisterStats("TestModule");
        stats->Add(100);

        b.Registry->UpdateStats(true);

        auto maxCounter =
            b.GetModuleCounters("TestModule")->FindCounter("MaxValue");
        UNIT_ASSERT(maxCounter);
        UNIT_ASSERT_VALUES_EQUAL(100, maxCounter->Val());
    }

    Y_UNIT_TEST(ShouldSupportMultipleModulesPerFilesystem)
    {
        TBootstrap b;

        auto stats1 = b.CreateAndRegisterStats("Module1");
        auto stats2 = b.CreateAndRegisterStats("Module2");

        stats1->Add(100);
        stats2->Add(200);
        b.Registry->UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(
            100,
            b.GetModuleCounters("Module1")->FindCounter("MaxValue")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            b.GetModuleCounters("Module2")->FindCounter("MaxValue")->Val());
    }

    Y_UNIT_TEST(ShouldIsolateStatsByFilesystemAndClient)
    {
        TBootstrap b;
        const TString fsId2 = "test_fs_2";
        const TString clientId2 = "test_client_2";

        auto stats1 = b.CreateAndRegisterStats("TestModule");
        auto stats2 = b.CreateAndRegisterStats("TestModule", fsId2, clientId2);

        stats1->Add(100);
        stats2->Add(200);
        b.Registry->UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(
            100,
            b.GetModuleCounters("TestModule")->FindCounter("MaxValue")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            b.GetModuleCounters("TestModule", fsId2, clientId2)
                ->FindCounter("MaxValue")
                ->Val());
    }

    Y_UNIT_TEST(ShouldUnregisterAndRemoveCounters)
    {
        TBootstrap b;

        auto stats = b.CreateAndRegisterStats("TestModule");
        stats->Add(100);
        b.Registry->UpdateStats(true);

        auto fsCounters =
            b.Counters->FindSubgroup("component", b.Component + "_fs");
        fsCounters = fsCounters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(fsCounters->FindSubgroup("filesystem", b.FileSystemId));

        // Registry->Unregister calls FsCountersRegistry->Unregister internally
        b.Registry->Unregister(b.FileSystemId, b.ClientId);

        auto fsSubgroup =
            fsCounters->FindSubgroup("filesystem", b.FileSystemId);
        UNIT_ASSERT(fsSubgroup);
        UNIT_ASSERT(!fsSubgroup->FindSubgroup("client", b.ClientId));
    }

    Y_UNIT_TEST(ShouldAggregateStats)
    {
        TBootstrap b;

        auto stats1 = b.CreateAndRegisterStats("TestModule", "fs1", "client1");
        auto stats2 = b.CreateAndRegisterStats("TestModule", "fs2", "client2");

        stats1->Add(100);
        stats2->Add(200);
        b.Registry->UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(
            100,
            b.GetModuleCounters("TestModule", "fs1", "client1")
                ->FindCounter("SumValue")
                ->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            200,
            b.GetModuleCounters("TestModule", "fs2", "client2")
                ->FindCounter("SumValue")
                ->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            300,
            b.GetAggregateModuleCounters("TestModule")
                ->FindCounter("SumValue")
                ->Val());

        auto stats3 = b.CreateAndRegisterStats("TestModule", "fs3", "client3");
        stats3->Add(300);
        b.Registry->UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(
            300,
            b.GetModuleCounters("TestModule", "fs3", "client3")
                ->FindCounter("SumValue")
                ->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            600,
            b.GetAggregateModuleCounters("TestModule")
                ->FindCounter("SumValue")
                ->Val());

        b.Registry->Unregister("fs1", "client1");
        b.Registry->UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(
            500,
            b.GetAggregateModuleCounters("TestModule")
                ->FindCounter("SumValue")
                ->Val());

        b.Registry->Unregister("fs2", "client2");
        b.Registry->Unregister("fs3", "client3");
        b.Registry->UpdateStats(true);

        UNIT_ASSERT(!b.GetAggregateModuleCounters("TestModule")
                         ->FindCounter("SumValue"));
    }
}

}   // namespace NCloud::NFileStore
