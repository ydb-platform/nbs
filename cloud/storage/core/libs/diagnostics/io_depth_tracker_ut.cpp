#include "io_depth_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <limits>
#include <thread>

namespace NCloud {

Y_UNIT_TEST_SUITE(TIoDepthTrackerTest)
{
    Y_UNIT_TEST(ShouldAccumulateWithoutCompletions)
    {
        ui64 nowNs = 0;
        TIoDepthTracker tracker(2, [&] { return nowNs; });

        tracker.Started(0);
        nowNs = 60'000'000'000ULL;

        const auto snapshot = tracker.Snapshot();
        UNIT_ASSERT(snapshot.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.TimestampNs, nowNs);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].Current, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].IntegralUs, 60'000'000);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[1].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[1].IntegralUs, 0);
    }

    Y_UNIT_TEST(ShouldIntegrateOverlappingRequests)
    {
        ui64 nowNs = 0;
        TIoDepthTracker tracker(1, [&] { return nowNs; });

        tracker.Started(0);
        nowNs = 1'000'000'000;
        tracker.Started(0);
        nowNs = 3'000'000'000;
        UNIT_ASSERT(tracker.Completed(0));
        nowNs = 4'000'000'000;
        UNIT_ASSERT(tracker.Completed(0));

        const auto snapshot = tracker.Snapshot();
        UNIT_ASSERT(snapshot.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].IntegralUs, 6'000'000);
    }

    Y_UNIT_TEST(ShouldMeasureBurstWithinLongWindow)
    {
        ui64 nowNs = 0;
        TIoDepthTracker tracker(1, [&] { return nowNs; });

        for (ui32 i = 0; i < 32; ++i) {
            tracker.Started(0);
        }
        nowNs = 1'000'000'000;
        for (ui32 i = 0; i < 32; ++i) {
            UNIT_ASSERT(tracker.Completed(0));
        }
        nowNs = 10'000'000'000ULL;

        const auto snapshot = tracker.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].IntegralUs, 32'000'000);
        UNIT_ASSERT_DOUBLES_EQUAL(
            double(snapshot.Lanes[0].IntegralUs) /
                (double(snapshot.TimestampNs) / 1000),
            3.2,
            1e-9);
    }

    Y_UNIT_TEST(ShouldKeepReadAndWriteIndependent)
    {
        ui64 nowNs = 0;
        TIoDepthTracker tracker(2, [&] { return nowNs; });

        tracker.Started(0);
        nowNs = 1'000'000'000;
        tracker.Started(1);
        nowNs = 2'000'000'000;
        UNIT_ASSERT(tracker.Completed(0));
        nowNs = 3'000'000'000;

        const auto snapshot = tracker.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].IntegralUs, 2'000'000);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[1].Current, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[1].IntegralUs, 2'000'000);
    }

    Y_UNIT_TEST(ShouldPreserveAreaAcrossSnapshots)
    {
        ui64 nowNs = 0;
        TIoDepthTracker tracker(1, [&] { return nowNs; });
        tracker.Started(0);

        nowNs = 5'000'000'000;
        const auto first = tracker.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(first.Lanes[0].IntegralUs, 5'000'000);
        UNIT_ASSERT_VALUES_EQUAL(
            tracker.Snapshot().Lanes[0].IntegralUs,
            5'000'000);

        nowNs = 10'000'000'000ULL;
        const auto second = tracker.Snapshot();
        UNIT_ASSERT(first.Generation == second.Generation);
        UNIT_ASSERT_VALUES_EQUAL(second.Lanes[0].IntegralUs, 10'000'000);

        nowNs = 12'000'000'000ULL;
        UNIT_ASSERT(tracker.Completed(0));
        const auto completed = tracker.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(completed.Lanes[0].IntegralUs, 12'000'000);
        UNIT_ASSERT_VALUES_EQUAL(completed.Lanes[0].Current, 0);
    }

    Y_UNIT_TEST(ShouldPreserveSubMicrosecondContributions)
    {
        ui64 nowNs = 0;
        TIoDepthTracker tracker(1, [&] { return nowNs; });

        for (ui32 i = 0; i < 10; ++i) {
            tracker.Started(0);
            nowNs += 100;
            UNIT_ASSERT(tracker.Completed(0));
            tracker.Snapshot();
        }

        UNIT_ASSERT_VALUES_EQUAL(tracker.Snapshot().Lanes[0].IntegralUs, 1);
    }

    Y_UNIT_TEST(ShouldRejectUnbalancedCompletion)
    {
        ui64 nowNs = 0;
        TIoDepthTracker tracker(1, [&] { return nowNs; });

        UNIT_ASSERT(!tracker.Completed(0));
        tracker.Started(0);
        nowNs = 1000;
        UNIT_ASSERT(tracker.Completed(0));
        UNIT_ASSERT(!tracker.Completed(0));

        const auto snapshot = tracker.Snapshot();
        UNIT_ASSERT(!snapshot.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].IntegralUs, 1);
    }

    Y_UNIT_TEST(ShouldInvalidateClockRollbackWithoutUnderflow)
    {
        ui64 nowNs = 1000;
        TIoDepthTracker tracker(1, [&] { return nowNs; });
        tracker.Started(0);
        nowNs = 3000;
        tracker.Snapshot();
        nowNs = 2000;
        const auto rollback = tracker.Snapshot();
        UNIT_ASSERT(!rollback.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(rollback.TimestampNs, 3000);
        UNIT_ASSERT_VALUES_EQUAL(rollback.Lanes[0].IntegralUs, 2);

        nowNs = 4000;
        UNIT_ASSERT(tracker.Completed(0));
        UNIT_ASSERT_VALUES_EQUAL(tracker.Snapshot().Lanes[0].IntegralUs, 3);
    }

    Y_UNIT_TEST(ShouldKeepDiscontinuityUntilNewGeneration)
    {
        ui64 nowNs = 0;
        TIoDepthTracker oldSource(1, [&] { return nowNs; });
        UNIT_ASSERT(!oldSource.Completed(0));
        oldSource.Started(0);
        nowNs = 2000;
        UNIT_ASSERT(oldSource.Completed(0));

        const auto oldSnapshot = oldSource.Snapshot();
        UNIT_ASSERT(!oldSnapshot.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(oldSnapshot.Lanes[0].IntegralUs, 2);

        TIoDepthTracker newSource(1, [&] { return nowNs; });
        const auto newSnapshot = newSource.Snapshot();
        UNIT_ASSERT(newSnapshot.Continuous);
        UNIT_ASSERT(!(oldSnapshot.Generation == newSnapshot.Generation));
        UNIT_ASSERT_VALUES_EQUAL(newSnapshot.Lanes[0].IntegralUs, 0);
    }

    Y_UNIT_TEST(ShouldInvalidateIntegralOverflow)
    {
        ui64 nowNs = 0;
        TIoDepthTracker tracker(1, [&] { return nowNs; });
        for (ui32 i = 0; i < 1001; ++i) {
            tracker.Started(0);
        }
        nowNs = std::numeric_limits<ui64>::max();

        const auto snapshot = tracker.Snapshot();
        UNIT_ASSERT(!snapshot.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].Current, 1001);
    }

    Y_UNIT_TEST(ShouldSerializeConcurrentUpdatesAndSnapshots)
    {
        std::atomic<ui64> nowNs = 0;
        std::atomic<bool> completed = true;
        TIoDepthTracker tracker(2, [&] { return nowNs.load(); });

        TVector<std::thread> threads;
        for (ui32 index = 0; index < 4; ++index) {
            threads.emplace_back(
                [&, lane = index % 2]
                {
                    for (ui32 i = 0; i < 1000; ++i) {
                        tracker.Started(lane);
                        nowNs.fetch_add(1000);
                        if (!tracker.Completed(lane)) {
                            completed = false;
                        }
                        tracker.Snapshot();
                    }
                });
        }
        for (auto& thread: threads) {
            thread.join();
        }

        const auto snapshot = tracker.Snapshot();
        UNIT_ASSERT(completed.load());
        UNIT_ASSERT(snapshot.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[1].Current, 0);
    }
}

}   // namespace NCloud
