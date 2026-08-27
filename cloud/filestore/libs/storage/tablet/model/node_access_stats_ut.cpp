#include "node_access_stats.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeAccessStatsTrackerTest)
{
    Y_UNIT_TEST(ShouldTrackRequestCountAndScore)
    {
        TNodeAccessStatsTracker tracker(1, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);

        UNIT_ASSERT(tracker.UpdateAccessStats(1, start));

        const auto stats = tracker.GetStats(start, 3);

        UNIT_ASSERT_VALUES_EQUAL(1, stats.size());
        UNIT_ASSERT_VALUES_EQUAL(1, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[0].RequestCount);
        UNIT_ASSERT_DOUBLES_EQUAL(1.0, stats[0].AccessScore, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(start, stats[0].LastAccessed);
    }

    Y_UNIT_TEST(ShouldDecayScoreBeforeAdding)
    {
        TNodeAccessStatsTracker tracker(1, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);

        UNIT_ASSERT(tracker.UpdateAccessStats(1, start));
        UNIT_ASSERT(tracker.UpdateAccessStats(1, start));

        const auto stats = tracker.GetStats(start, 3);

        UNIT_ASSERT_VALUES_EQUAL(1, stats.size());
        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].RequestCount);
        UNIT_ASSERT_DOUBLES_EQUAL(2.0, stats[0].AccessScore, 1e-9);

        const auto decayed = CalculateDecayedAccessScore(
            stats[0],
            start + TDuration::Minutes(10),
            TDuration::Minutes(10));

        UNIT_ASSERT_DOUBLES_EQUAL(1.0, decayed, 1e-9);
    }

    Y_UNIT_TEST(ShouldOrderByCurrentDecayedScore)
    {
        TNodeAccessStatsTracker tracker(2, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);
        const auto old = start - TDuration::Minutes(10);

        for (ui32 i = 0; i < 10; ++i) {
            UNIT_ASSERT(tracker.UpdateAccessStats(1, old));
        }
        for (ui32 i = 0; i < 9; ++i) {
            UNIT_ASSERT(tracker.UpdateAccessStats(2, start));
        }

        const auto stats = tracker.GetStats(start, 5);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
    }

    Y_UNIT_TEST(ShouldUseNodeIdAsTieBreaker)
    {
        TNodeAccessStatsTracker tracker(2, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);

        UNIT_ASSERT(tracker.UpdateAccessStats(1, start));
        UNIT_ASSERT(tracker.UpdateAccessStats(2, start));

        const auto stats = tracker.GetStats(start, 5);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
    }

    Y_UNIT_TEST(ShouldEvictLeastAccessedNode)
    {
        TNodeAccessStatsTracker tracker(2, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);

        UNIT_ASSERT(
            tracker.UpdateAccessStats(1, start - TDuration::Minutes(10)));
        UNIT_ASSERT(tracker.UpdateAccessStats(2, start));
        UNIT_ASSERT(tracker.UpdateAccessStats(3, start));
        UNIT_ASSERT(tracker.UpdateAccessStats(4, start));
        UNIT_ASSERT(tracker.UpdateAccessStats(4, start));

        const auto stats = tracker.GetStats(start, 5);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(4, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(3, stats[1].NodeId);
    }
}
}   // namespace NCloud::NFileStore::NStorage
