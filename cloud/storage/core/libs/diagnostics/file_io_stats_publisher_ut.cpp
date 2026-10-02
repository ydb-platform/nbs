#include "file_io_stats_publisher.h"

#include "max_calculator.h"
#include "stats_handler.h"

#include <cloud/storage/core/libs/common/file_io_stats.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/size_literals.h>

namespace NCloud {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    TFileIOStatsRegistryPtr Registry =
        std::make_shared<TFileIOStatsRegistry>();
    TDynamicCountersPtr Counters = MakeIntrusive<TDynamicCounters>();
    IStatsHandlerPtr Publisher =
        CreateFileIOStatsPublisher(Timer, Registry, Counters);

    TDynamicCountersPtr FindRequestGroup(
        const TString& backend,
        const TString& serviceId,
        const TString& request) const
    {
        auto group = Counters->FindSubgroup("component", "file_io");
        if (group) {
            group = group->FindSubgroup("backend", backend);
        }
        if (group) {
            group = group->FindSubgroup("io_service", serviceId);
        }
        if (group) {
            group = group->FindSubgroup("request", request);
        }
        return group;
    }

    i64 GetCounter(
        const TString& backend,
        const TString& serviceId,
        const TString& request,
        const TString& name) const
    {
        auto group = FindRequestGroup(backend, serviceId, request);
        UNIT_ASSERT_C(
            group,
            "no group for " << backend << "/" << serviceId << "/" << request);
        auto counter = group->FindCounter(name);
        UNIT_ASSERT_C(counter, "no counter " << name);
        return counter->Val();
    }

    void Tick(TDuration delay = TDuration::Seconds(1))
    {
        Timer->AdvanceTime(delay);
        Publisher->UpdateStats(false);
    }
};

void Complete(
    TFileIOStats& stats,
    EFileIORequest request,
    ui64 bytes,
    bool failed = false,
    TDuration time = {})
{
    const ui64 start = stats.RequestStarted(request);
    stats.RequestCompleted(
        request,
        start - DurationToCycles(time),
        bytes,
        failed);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileIOStatsPublisherTest)
{
    Y_UNIT_TEST_F(ShouldPublishCumulativeCounters, TFixture)
    {
        auto stats = Registry->Register("aio");

        Complete(*stats, EFileIORequest::Read, 4096);
        Complete(*stats, EFileIORequest::Read, 8192, true);
        Complete(*stats, EFileIORequest::Write, 1024);
        stats->RequestStarted(EFileIORequest::Write);

        // repeated publication should not double-count cumulative values
        for (int i = 0; i != 3; ++i) {
            Tick();

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                GetCounter("aio", "0", "ReadBlocks", "Count"));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                GetCounter("aio", "0", "ReadBlocks", "Errors"));
            UNIT_ASSERT_VALUES_EQUAL(
                4096 + 8192,
                GetCounter("aio", "0", "ReadBlocks", "RequestBytes"));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                GetCounter("aio", "0", "ReadBlocks", "InProgress"));

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                GetCounter("aio", "0", "WriteBlocks", "Count"));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                GetCounter("aio", "0", "WriteBlocks", "Errors"));
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                GetCounter("aio", "0", "WriteBlocks", "RequestBytes"));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                GetCounter("aio", "0", "WriteBlocks", "InProgress"));
        }
    }

    Y_UNIT_TEST_F(ShouldPublishRequestBytesRate, TFixture)
    {
        auto stats = Registry->Register("io_uring");

        // traffic before the start of the collection is not included in the
        // rate
        Complete(*stats, EFileIORequest::Write, 1_MB);
        Tick();
        UNIT_ASSERT_VALUES_EQUAL(
            1_MB,
            GetCounter("io_uring", "0", "WriteBlocks", "RequestBytes"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetCounter("io_uring", "0", "WriteBlocks", "MaxRequestBytes"));

        Complete(*stats, EFileIORequest::Write, 1000);
        Tick();
        UNIT_ASSERT_VALUES_EQUAL(
            1000,
            GetCounter("io_uring", "0", "WriteBlocks", "MaxRequestBytes"));

        // the rate is calculated from the traffic since the previous update
        Complete(*stats, EFileIORequest::Write, 500);
        Tick();
        UNIT_ASSERT_VALUES_EQUAL(
            1000,
            GetCounter("io_uring", "0", "WriteBlocks", "MaxRequestBytes"));

        // the peak ages out during idle periods
        for (size_t i = 0; i != DEFAULT_BUCKET_COUNT; ++i) {
            Tick();
        }
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetCounter("io_uring", "0", "WriteBlocks", "MaxRequestBytes"));
    }

    Y_UNIT_TEST_F(ShouldPublishMaxTime, TFixture)
    {
        auto stats = Registry->Register("aio");

        Complete(
            *stats,
            EFileIORequest::Read,
            4096,
            false,
            TDuration::Seconds(10));
        Tick();

        const i64 maxTime = GetCounter("aio", "0", "ReadBlocks", "MaxTime");
        UNIT_ASSERT_LE(10'000'000, maxTime);
        UNIT_ASSERT_LE(maxTime, GetCounter("aio", "0", "ReadBlocks", "Time"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetCounter("aio", "0", "WriteBlocks", "MaxTime"));

        // the peak ages out during idle periods
        for (size_t i = 0; i != DEFAULT_BUCKET_COUNT; ++i) {
            Tick();
        }
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetCounter("aio", "0", "ReadBlocks", "MaxTime"));
    }

    Y_UNIT_TEST_F(ShouldPublishLateRegisteredServices, TFixture)
    {
        auto aio0 = Registry->Register("aio");
        Tick();

        UNIT_ASSERT(FindRequestGroup("aio", "0", "ReadBlocks"));
        UNIT_ASSERT(!FindRequestGroup("aio", "1", "ReadBlocks"));

        auto aio1 = Registry->Register("aio");
        auto uring0 = Registry->Register("io_uring");

        Complete(*aio1, EFileIORequest::Read, 100);
        Complete(*uring0, EFileIORequest::Write, 200);
        Tick();

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetCounter("aio", "0", "ReadBlocks", "Count"));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetCounter("aio", "1", "ReadBlocks", "Count"));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetCounter("io_uring", "0", "WriteBlocks", "Count"));
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            GetCounter("io_uring", "0", "WriteBlocks", "RequestBytes"));
    }

    Y_UNIT_TEST_F(ShouldNotPublishUnregisteredStats, TFixture)
    {
        auto stats = RegisterFileIOStats(nullptr, "aio");
        Complete(*stats, EFileIORequest::Read, 100);
        Tick();

        UNIT_ASSERT(!FindRequestGroup("aio", "0", "ReadBlocks"));
    }
}

}   // namespace NCloud
