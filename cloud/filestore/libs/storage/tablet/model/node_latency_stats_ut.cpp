#include "node_latency_stats.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeLatencyStatsTrackerTest)
{
    Y_UNIT_TEST(ShouldTrackRequestCountAndRequestTypeAndLatency)
    {
        TNodeLatencyStatsTracker tracker(2, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            start,
            TDuration::MicroSeconds(50));
        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::WriteData,
            start,
            TDuration::MicroSeconds(60));

        const auto stats = tracker.GetLatencyStats(start, 10);

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
        UNIT_ASSERT_DOUBLES_EQUAL(60.0, stats[0].AverageLatencyDecayedUs, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(50.0, stats[1].AverageLatencyDecayedUs, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(start, stats[0].LastAccessed);
        UNIT_ASSERT_VALUES_EQUAL(start, stats[1].LastAccessed);
    }

    Y_UNIT_TEST(DecayCalculation)
    {
        TNodeLatencyStatsTracker tracker(2, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            start,
            TDuration::MicroSeconds(50));
        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            start,
            TDuration::MicroSeconds(150));

        auto stats = tracker.GetLatencyStats(start, 2);

        UNIT_ASSERT_VALUES_EQUAL(1, stats.size());
        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].RequestCount);
        UNIT_ASSERT_DOUBLES_EQUAL(
            100.0,
            stats[0].AverageLatencyDecayedUs,
            1e-9);

        const auto decayed = TNodeLatencyStatsTracker::CalculateLatencyDecay(
            stats[0],
            start + TDuration::Minutes(10), TDuration::Minutes(10));

        UNIT_ASSERT_DOUBLES_EQUAL(50.0, decayed, 1e-9);
    }

    Y_UNIT_TEST(ShouldOrderByDecayedLatency)
    {
        TNodeLatencyStatsTracker tracker(2, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);
        const auto old = start - TDuration::Minutes(10);

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            old,
            TDuration::MicroSeconds(50));
        tracker.UpdateLatencyStats(
            2,
            EFileStoreRequest::ReadData,
            start,
            TDuration::MicroSeconds(50));

        const auto stats = tracker.GetLatencyStats(start, 2);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
    }

    Y_UNIT_TEST(ShouldUseNodeIdAsTieBreaker)
    {
        TNodeLatencyStatsTracker tracker(2, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            start,
            TDuration::MicroSeconds(50));
        tracker.UpdateLatencyStats(
            2,
            EFileStoreRequest::ReadData,
            start,
            TDuration::MicroSeconds(50));

        const auto stats = tracker.GetLatencyStats(start, 2);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].NodeId);
    }

    Y_UNIT_TEST(ShouldEvictLeastLatentNodeAndRequest)
    {
        TNodeLatencyStatsTracker tracker(2, TDuration::Minutes(10));

        const auto start = TInstant::Hours(1);

        tracker.UpdateLatencyStats(
            1,
            EFileStoreRequest::ReadData,
            start - TDuration::Minutes(10),
            TDuration::MicroSeconds(100));
        tracker.UpdateLatencyStats(
            2,
            EFileStoreRequest::WriteData,
            start,
            TDuration::MicroSeconds(50));
        tracker.UpdateLatencyStats(
            2,
            EFileStoreRequest::WriteData,
            start,
            TDuration::MicroSeconds(150));
        tracker.UpdateLatencyStats(
            3,
            EFileStoreRequest::AddData,
            start,
            TDuration::MicroSeconds(200));
        tracker.UpdateLatencyStats(
            4,
            EFileStoreRequest::GetNodeAttr,
            start,
            TDuration::MicroSeconds(300));

        const auto stats = tracker.GetLatencyStats(start, 2);

        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

        UNIT_ASSERT_VALUES_EQUAL(4, stats[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(3, stats[1].NodeId);
    }
}
}   // namespace NCloud::NFileStore::NStorage
