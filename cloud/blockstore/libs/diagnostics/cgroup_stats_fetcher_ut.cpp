#include "cgroup_stats_fetcher.h"

#include "critical_events.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

auto SetupCriticalEvents(IMonitoringServicePtr monitoring)
{
    auto rootGroup = monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore");

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
        auto monitoring = CreateMonitoringServiceStub();
        auto serverGroup = SetupCriticalEvents(monitoring);
        TTempFileHandle statsFile("test");

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(10));

        auto fetcher = CreateCgroupStatsFetcher(
            CreateLoggingService("console"),
            monitoring,
            statsFile.Name());
        fetcher->Start();

        UNIT_ASSERT_VALUES_EQUAL(TDuration(), fetcher->GetCpuWait());

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(20));
        UNIT_ASSERT_VALUES_EQUAL(TDuration::MicroSeconds(10), fetcher->GetCpuWait());

        fetcher->Stop();

        UNIT_ASSERT_VALUES_EQUAL(0, serverGroup->GetCounter("AppCriticalEvents/CpuWaitCounterReadError", true)->Val());
    }

    Y_UNIT_TEST(ShouldReportErrorIfFileIsMissing)
    {
        auto monitoring = CreateMonitoringServiceStub();
        SetupCriticalEvents(monitoring);

        auto fetcher = CreateCgroupStatsFetcher(
            CreateLoggingService("console"),
            monitoring,
            "noname");
        fetcher->Start();

        auto failCounter = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server")
            ->GetCounter("CpuWaitFailure", false);

        UNIT_ASSERT_VALUES_EQUAL(1, failCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(TDuration(), fetcher->GetCpuWait());
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), fetcher->GetCpuWait());

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldReportErrorIfNewValueIsLowerThanPrevious)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto serverGroup = SetupCriticalEvents(monitoring);
        TTempFileHandle statsFile("test");

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(100));

        auto fetcher = CreateCgroupStatsFetcher(
            CreateLoggingService("console"),
            monitoring,
            "test");
        fetcher->Start();

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(80));
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), fetcher->GetCpuWait());
        UNIT_ASSERT_VALUES_EQUAL(1, serverGroup->GetCounter("AppCriticalEvents/CpuWaitCounterReadError", true)->Val());

        UpdateCGroupWaitDuration(statsFile, TDuration::MicroSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(TDuration::MicroSeconds(20), fetcher->GetCpuWait());
        UNIT_ASSERT_VALUES_EQUAL(1, serverGroup->GetCounter("AppCriticalEvents/CpuWaitCounterReadError", true)->Val());

        fetcher->Stop();
    }
}

}   // namespace NCloud::NBlockStore
