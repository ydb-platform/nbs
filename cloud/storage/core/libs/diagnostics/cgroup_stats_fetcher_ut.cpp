#include "stats_fetcher.h"

#include "critical_events.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString ComponentName = "STORAGE_CGROUPS";

auto SetupCriticalEvents(IMonitoringServicePtr monitoring)
{
    auto rootGroup = monitoring->GetCounters()
        ->GetSubgroup("counters", "storage");

    auto serverGroup = rootGroup->GetSubgroup("component", "server");
    InitCriticalEventsCounter(serverGroup);

    return serverGroup;
}

void UpdateCGroupWaitDuration(TTempFileHandle& file, TDuration value)
{
    file.Seek(0, sSet);
    // Cgroup wait time is calculated in nano seconds
    TString buffer = ToString(value.MicroSeconds() * 1000);
    buffer += '\n';

    file.Write(buffer.c_str(), buffer.size());
    file.Resize(buffer.size());
}

};  //namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCGroupStatFetcherTest)
{
    Y_UNIT_TEST(ShouldReadStats)
    {
        TTempFileHandle statsFile("test");

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(10));

        auto fetcher = CreateCgroupStatsFetcher(
            ComponentName,
            CreateLoggingService("console"),
            statsFile.Name());
        fetcher->Start();

        auto cpuWait = fetcher->GetCpuWait();
        UNIT_ASSERT_C(!HasError(cpuWait), cpuWait.GetError());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(0),
            cpuWait.GetResult());

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(20));
        cpuWait = fetcher->GetCpuWait();
        UNIT_ASSERT_C(!HasError(cpuWait), cpuWait.GetError());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(10),
            cpuWait.GetResult());

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldReportErrorIfFileIsMissing)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto serverGroup = SetupCriticalEvents(monitoring);

        auto fetcher = CreateCgroupStatsFetcher(
            ComponentName,
            CreateLoggingService("console"),
            "noname");
        fetcher->Start();

        auto cpuWait = fetcher->GetCpuWait();
        UNIT_ASSERT_C(HasError(cpuWait), cpuWait.GetError());
        cpuWait = fetcher->GetCpuWait();
        UNIT_ASSERT_C(HasError(cpuWait), cpuWait.GetError());

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldReportErrorIfNewValueIsLowerThanPrevious)
    {
        TTempFileHandle statsFile("test");

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(100));

        auto fetcher = CreateCgroupStatsFetcher(
            ComponentName,
            CreateLoggingService("console"),
            "test");
        fetcher->Start();

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(80));
        auto cpuWait = fetcher->GetCpuWait();
        UNIT_ASSERT_C(HasError(cpuWait), cpuWait.GetError());

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(100));
        cpuWait = fetcher->GetCpuWait();
        UNIT_ASSERT_C(!HasError(cpuWait), cpuWait.GetError());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(20),
            cpuWait.GetResult());

        fetcher->Stop();
    }
}

}   // namespace NCloud::NStorage
