#include "node_latency_stats.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeLatencyStatsTrackerTest)
{
    Y_UNIT_TEST(ShouldTrackRequestCountAndRequestTypeAndLatency)
    {
        TNodeLatencyStatsTracker tracker;
        tracker.Initialize(2);

        const auto now = TInstant::Now();

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            now,
            TDuration::MilliSeconds(50));
        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::WriteData,
            now,
            TDuration::MilliSeconds(60));

        const auto stats = tracker.GetLatencyStats(now);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());
        UNIT_ASSERT_VALUES_EQUAL(1, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[0].RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(
            GetFileStoreRequestName(EFileStoreRequest::WriteData),
            GetFileStoreRequestName(stats[0].RequestType));
        UNIT_ASSERT_VALUES_EQUAL(
            GetFileStoreRequestName(EFileStoreRequest::ReadData),
            GetFileStoreRequestName(stats[1].RequestType));
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].RequestCount);
        UNIT_ASSERT_DOUBLES_EQUAL(60.0, stats[0].AverageLatencyDecayedMs, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(50.0, stats[1].AverageLatencyDecayedMs, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(now, stats[0].LastAccessed);
        UNIT_ASSERT_VALUES_EQUAL(now, stats[1].LastAccessed);
    }

    Y_UNIT_TEST(DecayCalculation)
    {
        TNodeLatencyStatsTracker tracker;
        tracker.Initialize(1);

        const auto now = TInstant::Now();

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            now,
            TDuration::MilliSeconds(50));
        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            now,
            TDuration::MilliSeconds(150));

        auto stats = tracker.GetLatencyStats(now);

        UNIT_ASSERT_VALUES_EQUAL(1, stats.size());
        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].RequestCount);
        UNIT_ASSERT_DOUBLES_EQUAL(
            100.0,
            stats[0].AverageLatencyDecayedMs,
            1e-9);

        const auto decayed = TNodeLatencyStatsTracker::CalculateLatencyDecay(
            stats[0],
            now + TDuration::Minutes(10));

        UNIT_ASSERT_DOUBLES_EQUAL(50.0, decayed, 1e-9);
    }

    Y_UNIT_TEST(ShouldOrderByDecayedLatency)
    {
        TNodeLatencyStatsTracker tracker;
        tracker.Initialize(2);

        const auto now = TInstant::Now();
        const auto old = now - TDuration::Minutes(10);

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            old,
            TDuration::MilliSeconds(50));
        tracker.UpdateLatencyStats(
            2,
            EFileStoreRequest::ReadData,
            now,
            TDuration::MilliSeconds(50));

        const auto stats = tracker.GetLatencyStats(now);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
    }

    Y_UNIT_TEST(ShouldUseNodeIdAsTieBreaker)
    {
        TNodeLatencyStatsTracker tracker;
        tracker.Initialize(2);

        const auto now = TInstant::Now();

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            now,
            TDuration::MilliSeconds(50));
        tracker.UpdateLatencyStats(
            2,
            EFileStoreRequest::ReadData,
            now,
            TDuration::MilliSeconds(50));

        const auto stats = tracker.GetLatencyStats(now);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
    }

    Y_UNIT_TEST(ShouldEvictLeastLatentNodeAndRequest)
    {
        TNodeLatencyStatsTracker tracker;
        tracker.Initialize(2);

        const auto now = TInstant::Now();

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            now - TDuration::Minutes(10),
            TDuration::MilliSeconds(100));
        tracker.UpdateLatencyStats(
            2,
            EFileStoreRequest::WriteData,
            now,
            TDuration::MilliSeconds(50));
        tracker.UpdateLatencyStats(
            2,
            EFileStoreRequest::WriteData,
            now,
            TDuration::MilliSeconds(150));
        tracker.UpdateLatencyStats(
            3,
            EFileStoreRequest::AddData,
            now,
            TDuration::MilliSeconds(200));
        tracker.UpdateLatencyStats(
            4,
            EFileStoreRequest::GetNodeAttr,
            now,
            TDuration::MilliSeconds(300));

        const auto stats = tracker.GetLatencyStats(now);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(4, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(3, stats[1].NodeId);
    }
}
}   // namespace NCloud::NFileStore::NStorage
