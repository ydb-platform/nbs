#include "file_io_stats.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileIOStatsTest)
{
    Y_UNIT_TEST(ShouldAccountRequests)
    {
        TFileIOStats stats;

        const ui64 read1 = stats.RequestStarted(EFileIORequest::Read);
        const ui64 read2 = stats.RequestStarted(EFileIORequest::Read);
        const ui64 write = stats.RequestStarted(EFileIORequest::Write);

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            stats.GetStats(EFileIORequest::Read).InProgress);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            stats.GetStats(EFileIORequest::Write).InProgress);

        stats.RequestCompleted(EFileIORequest::Read, read1, 4096, false);
        stats.RequestCompleted(
            EFileIORequest::Read,
            read2 - DurationToCycles(TDuration::Seconds(1)),
            8192,
            true);   // failed

        {
            const auto s = stats.GetStats(EFileIORequest::Read);
            UNIT_ASSERT_VALUES_EQUAL(1, s.Count);
            UNIT_ASSERT_VALUES_EQUAL(1, s.Errors);
            UNIT_ASSERT_VALUES_EQUAL(4096 + 8192, s.RequestBytes);
            UNIT_ASSERT_VALUES_EQUAL(0, s.InProgress);
            UNIT_ASSERT_LE(1'000'000, s.Time);
        }

        UNIT_ASSERT_LE(1'000'000, stats.ResetMaxTime(EFileIORequest::Read));
        UNIT_ASSERT_VALUES_EQUAL(0, stats.ResetMaxTime(EFileIORequest::Read));

        {
            const auto s = stats.GetStats(EFileIORequest::Write);
            UNIT_ASSERT_VALUES_EQUAL(0, s.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, s.Errors);
            UNIT_ASSERT_VALUES_EQUAL(0, s.RequestBytes);
            UNIT_ASSERT_VALUES_EQUAL(1, s.InProgress);
        }

        stats.RequestCompleted(EFileIORequest::Write, write, 512, false);

        {
            const auto s = stats.GetStats(EFileIORequest::Write);
            UNIT_ASSERT_VALUES_EQUAL(1, s.Count);
            UNIT_ASSERT_VALUES_EQUAL(512, s.RequestBytes);
            UNIT_ASSERT_VALUES_EQUAL(0, s.InProgress);
        }
    }

    Y_UNIT_TEST(ShouldRegisterStatsWithUniqueIds)
    {
        TFileIOStatsRegistry registry;

        auto aio0 = registry.Register("aio");
        auto uring0 = registry.Register("io_uring");
        auto aio1 = registry.Register("aio");

        UNIT_ASSERT(aio0 != aio1);

        auto entries = registry.GetEntries();
        UNIT_ASSERT_VALUES_EQUAL(3, entries.size());

        UNIT_ASSERT_VALUES_EQUAL("aio", entries[0].Backend);
        UNIT_ASSERT_VALUES_EQUAL("0", entries[0].ServiceId);
        UNIT_ASSERT_EQUAL(aio0, entries[0].Stats);

        UNIT_ASSERT_VALUES_EQUAL("io_uring", entries[1].Backend);
        UNIT_ASSERT_VALUES_EQUAL("0", entries[1].ServiceId);
        UNIT_ASSERT_EQUAL(uring0, entries[1].Stats);

        UNIT_ASSERT_VALUES_EQUAL("aio", entries[2].Backend);
        UNIT_ASSERT_VALUES_EQUAL("1", entries[2].ServiceId);
        UNIT_ASSERT_EQUAL(aio1, entries[2].Stats);

        entries = registry.GetEntries(2);
        UNIT_ASSERT_VALUES_EQUAL(1, entries.size());
        UNIT_ASSERT_EQUAL(aio1, entries[0].Stats);

        UNIT_ASSERT_VALUES_EQUAL(0, registry.GetEntries(3).size());
        UNIT_ASSERT_VALUES_EQUAL(0, registry.GetEntries(10).size());
    }
}

}   // namespace NCloud
