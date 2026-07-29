#include "tablet_state.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeAccessStatsTrackerTest)
{
    Y_UNIT_TEST(ShouldTrackRequestCountAndScore)
    {
        TNodeAccessStatsTracker tracker;
        tracker.Initialise(1);

        const auto now = TInstant::Now();

        tracker.RequestStarted(1, now);

        const auto stats = tracker.GetStats(now);

        UNIT_ASSERT_VALUES_EQUAL(1, stats.size());
        UNIT_ASSERT_VALUES_EQUAL(1, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[0].RequestCount);
        UNIT_ASSERT_DOUBLES_EQUAL(1.0, stats[0].AccessScore, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(now, stats[0].LastAccessed);
    }

    Y_UNIT_TEST(ShouldDecayScoreBeforeAdding)
    {
        TNodeAccessStatsTracker tracker;
        tracker.Initialise(1);

        const auto now = TInstant::Now();

        tracker.RequestStarted(1, now);
        tracker.RequestStarted(1, now);

        const auto stats = tracker.GetStats(now);

        UNIT_ASSERT_VALUES_EQUAL(1, stats.size());
        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].RequestCount);
        UNIT_ASSERT_DOUBLES_EQUAL(2.0, stats[0].AccessScore, 1e-9);

        const auto decayed =
            TNodeAccessStatsTracker::DecayedScore(
                stats[0],
                now + TDuration::Minutes(10));

        UNIT_ASSERT_DOUBLES_EQUAL(1.0, decayed, 1e-9);
    }

    Y_UNIT_TEST(ShouldOrderByCurrentDecayedScore)
    {
        TNodeAccessStatsTracker tracker;
        tracker.Initialise(2);

        const auto now = TInstant::Now();
        const auto old = now - TDuration::Minutes(10);

        for (ui32 i = 0; i < 10; ++i) {
            tracker.RequestStarted(1, old);
        }
        for (ui32 i = 0; i < 9; ++i) {
            tracker.RequestStarted(2, now);
        }

        const auto stats = tracker.GetStats(now);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
        }

    Y_UNIT_TEST(ShouldUseNodeIdAsTieBreaker)
    {
        TNodeAccessStatsTracker tracker;
        tracker.Initialise(2);

        const auto now = TInstant::Now();

        tracker.RequestStarted(1, now);
        tracker.RequestStarted(2, now);

        const auto stats = tracker.GetStats(now);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
    }

    Y_UNIT_TEST(ShouldEvictLeastAccessedNode)
    {
        TNodeAccessStatsTracker tracker;
        tracker.Initialise(2);

        const auto now = TInstant::Now();

        tracker.RequestStarted(1, now - TDuration::Minutes(10));
        tracker.RequestStarted(2, now);
        tracker.RequestStarted(3, now);
        tracker.RequestStarted(4, now);
        tracker.RequestStarted(4, now);

        const auto stats = tracker.GetStats(now);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(4, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(3, stats[1].NodeId);
    }
}
} // namespace NCloud::NFileStore::NStorage
