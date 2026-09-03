#include "lsn_barriers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 GetWatermark(const TWatermarkTracker& tracker)
{
    const auto watermark = tracker.Acquire();
    tracker.Release(watermark);
    return watermark;
}

}   // namespace

Y_UNIT_TEST_SUITE(TWatermarkTrackerTest)
{
    Y_UNIT_TEST(ShouldStartWithZeroWatermark)
    {
        TWatermarkTracker tracker;

        UNIT_ASSERT_VALUES_EQUAL(0, GetWatermark(tracker));
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.GetMinAcquired());
    }

    Y_UNIT_TEST(ShouldAdvanceWatermarkMonotonically)
    {
        TWatermarkTracker tracker;

        tracker.AdvanceWatermark(10);
        UNIT_ASSERT_VALUES_EQUAL(10, GetWatermark(tracker));

        // stale watermarks should be ignored
        tracker.AdvanceWatermark(5);
        UNIT_ASSERT_VALUES_EQUAL(10, GetWatermark(tracker));

        tracker.AdvanceWatermark(10);
        UNIT_ASSERT_VALUES_EQUAL(10, GetWatermark(tracker));

        tracker.AdvanceWatermark(20);
        UNIT_ASSERT_VALUES_EQUAL(20, GetWatermark(tracker));
    }

    Y_UNIT_TEST(ShouldReportWatermarkWhenNothingAcquired)
    {
        TWatermarkTracker tracker;

        tracker.AdvanceWatermark(10);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetMinAcquired());

        const auto watermark = tracker.Acquire();
        tracker.Release(watermark);

        tracker.AdvanceWatermark(20);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetMinAcquired());
    }

    Y_UNIT_TEST(ShouldPinLowAcquiredWhileWatermarkAdvances)
    {
        TWatermarkTracker tracker;

        tracker.AdvanceWatermark(10);

        const auto watermark = tracker.Acquire();
        UNIT_ASSERT_VALUES_EQUAL(10, watermark);

        tracker.AdvanceWatermark(20);

        // the acquired watermark holds back reclamation, the high one moves on
        UNIT_ASSERT_VALUES_EQUAL(20, GetWatermark(tracker));
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetMinAcquired());

        tracker.Release(watermark);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetMinAcquired());
    }

    Y_UNIT_TEST(ShouldTrackLowestOfSeveralAcquiredWatermarks)
    {
        TWatermarkTracker tracker;

        tracker.AdvanceWatermark(10);
        const auto first = tracker.Acquire();

        tracker.AdvanceWatermark(20);
        const auto second = tracker.Acquire();

        tracker.AdvanceWatermark(30);
        const auto third = tracker.Acquire();

        UNIT_ASSERT_VALUES_EQUAL(10, first);
        UNIT_ASSERT_VALUES_EQUAL(20, second);
        UNIT_ASSERT_VALUES_EQUAL(30, third);

        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetMinAcquired());

        tracker.Release(first);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetMinAcquired());

        tracker.Release(second);
        UNIT_ASSERT_VALUES_EQUAL(30, tracker.GetMinAcquired());

        tracker.Release(third);
        UNIT_ASSERT_VALUES_EQUAL(30, tracker.GetMinAcquired());
    }

    Y_UNIT_TEST(ShouldRefcountRepeatedAcquiresOfSameWatermark)
    {
        TWatermarkTracker tracker;

        tracker.AdvanceWatermark(10);

        const auto first = tracker.Acquire();
        const auto second = tracker.Acquire();
        const auto third = tracker.Acquire();

        UNIT_ASSERT_VALUES_EQUAL(10, first);
        UNIT_ASSERT_VALUES_EQUAL(10, second);
        UNIT_ASSERT_VALUES_EQUAL(10, third);

        tracker.AdvanceWatermark(20);

        tracker.Release(first);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetMinAcquired());

        tracker.Release(second);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetMinAcquired());

        // only the last release lets the low watermark move
        tracker.Release(third);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetMinAcquired());
    }

    Y_UNIT_TEST(ShouldAllowOutOfOrderRelease)
    {
        TWatermarkTracker tracker;

        tracker.AdvanceWatermark(10);
        const auto first = tracker.Acquire();

        tracker.AdvanceWatermark(20);
        const auto second = tracker.Acquire();

        tracker.Release(second);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetMinAcquired());

        tracker.Release(first);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetMinAcquired());
    }

    Y_UNIT_TEST(ShouldAcquireZeroWatermarkBeforeFirstAdvance)
    {
        TWatermarkTracker tracker;

        const auto watermark = tracker.Acquire();
        UNIT_ASSERT_VALUES_EQUAL(0, watermark);

        tracker.AdvanceWatermark(10);
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.GetMinAcquired());

        tracker.Release(watermark);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetMinAcquired());
    }

    Y_UNIT_TEST(ShouldNotReclaimPastAcquiredWatermarkUnderConcurrency)
    {
        // The contract readers rely on: while a watermark is held,
        // GetMinAcquired() never moves past it, so records with a greater lsn
        // stay alive.
        constexpr ui64 ReaderCount = 4;
        constexpr ui64 IterationCount = 2000;

        TWatermarkTracker tracker;

        std::atomic<bool> stop = false;
        std::atomic<ui64> contractViolations = 0;
        std::atomic<ui64> orderViolations = 0;

        TVector<std::thread> readers;
        readers.reserve(ReaderCount);

        for (ui64 i = 0; i < ReaderCount; ++i) {
            readers.emplace_back(
                [&]
                {
                    for (ui64 j = 0; j < IterationCount; ++j) {
                        const auto watermark = tracker.Acquire();

                        if (tracker.GetMinAcquired() > watermark) {
                            ++contractViolations;
                        }

                        // read the low one first - the high one only grows, so
                        // this ordering keeps the comparison meaningful
                        const auto minAcquired = tracker.GetMinAcquired();
                        if (minAcquired > GetWatermark(tracker)) {
                            ++orderViolations;
                        }

                        tracker.Release(watermark);
                    }
                });
        }

        std::thread writer(
            [&]
            {
                ui64 watermark = 0;
                while (!stop.load(std::memory_order_relaxed)) {
                    tracker.AdvanceWatermark(++watermark);
                }
            });

        for (auto& reader: readers) {
            reader.join();
        }

        stop.store(true, std::memory_order_relaxed);
        writer.join();

        UNIT_ASSERT_VALUES_EQUAL(0, contractViolations.load());
        UNIT_ASSERT_VALUES_EQUAL(0, orderViolations.load());

        // every acquire was released, so nothing is pinned any more
        UNIT_ASSERT_VALUES_EQUAL(
            GetWatermark(tracker),
            tracker.GetMinAcquired());
    }
}

}   // namespace NCloud::NJournalled
