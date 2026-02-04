#include "module_stats.h"

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t TestMaxBucketCount = 60;

////////////////////////////////////////////////////////////////////////////////

class TTestModuleStats final: public IModuleStats
{
private:
    TAtomic Value = 0;
    std::unique_ptr<TMaxCalculator<TestMaxBucketCount>> MaxCalc;
    TDynamicCounters::TCounterPtr MaxCounter;

public:
    TTestModuleStats(ITimerPtr timer, TDynamicCountersPtr counters)
        : MaxCalc(std::make_unique<TMaxCalculator<TestMaxBucketCount>>(timer))
        , MaxCounter(counters->GetCounter("MaxValue", false))
    {}

    void Add(ui64 value)
    {
        AtomicSet(Value, value);
        MaxCalc->Add(value);
    }

    void UpdateStats() override
    {
        MaxCounter->Set(MaxCalc->NextValue());
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
    IModuleStatsRegistryPtr Registry =
        CreateModuleStatsRegistry(Component, Counters);

    std::shared_ptr<TTestModuleStats> CreateAndRegisterStats(
        const TString& moduleName,
        const TString& fsId,
        const TString& clientId)
    {
        auto moduleCounters = Registry->GetFileSystemModuleCounters(
            fsId,
            clientId,
            CloudId,
            FolderId,
            moduleName);

        auto stats = std::make_shared<TTestModuleStats>(Timer, moduleCounters);
        Registry->Register(fsId, clientId, stats);
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
        return Registry->GetFileSystemModuleCounters(
            fsId,
            clientId,
            CloudId,
            FolderId,
            moduleName);
    }

    TDynamicCountersPtr GetModuleCounters(const TString& moduleName) const
    {
        return GetModuleCounters(moduleName, FileSystemId, ClientId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TModuleStatsRegistryTest)
{
    Y_UNIT_TEST(ShouldCreateCounterHierarchy)
    {
        TBootstrap b;

        auto moduleCounters = b.GetModuleCounters("TestModule");
        UNIT_ASSERT(moduleCounters);

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

        b.Registry->Unregister(b.FileSystemId, b.ClientId);

        auto fsSubgroup =
            fsCounters->FindSubgroup("filesystem", b.FileSystemId);
        UNIT_ASSERT(fsSubgroup);
        UNIT_ASSERT(!fsSubgroup->FindSubgroup("client", b.ClientId));
    }
}

}   // namespace NCloud::NFileStore
