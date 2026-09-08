#include "watermark_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 GetWatermark(TWatermarkTracker& tracker)
{
    const auto watermark = tracker.Pin();
    tracker.Unpin(watermark);
    return watermark;
}

}   // namespace

Y_UNIT_TEST_SUITE(TWatermarkTrackerTest)
{
    Y_UNIT_TEST(ShouldStartWithZeroWatermark)
    {
        TWatermarkTracker tracker;

        UNIT_ASSERT_VALUES_EQUAL(0, GetWatermark(tracker));
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldAdvanceWatermarkMonotonically)
    {
        TWatermarkTracker tracker;

        tracker.Advance(10);
        UNIT_ASSERT_VALUES_EQUAL(10, GetWatermark(tracker));

        // stale watermarks should be ignored
        tracker.Advance(5);
        UNIT_ASSERT_VALUES_EQUAL(10, GetWatermark(tracker));

        tracker.Advance(10);
        UNIT_ASSERT_VALUES_EQUAL(10, GetWatermark(tracker));

        tracker.Advance(20);
        UNIT_ASSERT_VALUES_EQUAL(20, GetWatermark(tracker));
    }

    Y_UNIT_TEST(ShouldReportWatermarkWhenNothingPinned)
    {
        TWatermarkTracker tracker;

        tracker.Advance(10);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());

        const auto watermark = tracker.Pin();
        tracker.Unpin(watermark);

        tracker.Advance(20);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldHoldPinnedWatermarkWhileWatermarkAdvances)
    {
        TWatermarkTracker tracker;

        tracker.Advance(10);

        const auto watermark = tracker.Pin();
        UNIT_ASSERT_VALUES_EQUAL(10, watermark);

        tracker.Advance(20);

        // the pinned watermark holds back reclamation, the high one moves on
        UNIT_ASSERT_VALUES_EQUAL(20, GetWatermark(tracker));
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());

        tracker.Unpin(watermark);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldTrackOldestOfSeveralPins)
    {
        TWatermarkTracker tracker;

        tracker.Advance(10);
        const auto first = tracker.Pin();

        tracker.Advance(20);
        const auto second = tracker.Pin();

        tracker.Advance(30);
        const auto third = tracker.Pin();

        UNIT_ASSERT_VALUES_EQUAL(10, first);
        UNIT_ASSERT_VALUES_EQUAL(20, second);
        UNIT_ASSERT_VALUES_EQUAL(30, third);

        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());

        tracker.Unpin(first);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetPinnedWatermark());

        tracker.Unpin(second);
        UNIT_ASSERT_VALUES_EQUAL(30, tracker.GetPinnedWatermark());

        tracker.Unpin(third);
        UNIT_ASSERT_VALUES_EQUAL(30, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldRefcountRepeatedPinsOfSameWatermark)
    {
        TWatermarkTracker tracker;

        tracker.Advance(10);

        const auto first = tracker.Pin();
        const auto second = tracker.Pin();
        const auto third = tracker.Pin();

        UNIT_ASSERT_VALUES_EQUAL(10, first);
        UNIT_ASSERT_VALUES_EQUAL(10, second);
        UNIT_ASSERT_VALUES_EQUAL(10, third);

        tracker.Advance(20);

        tracker.Unpin(first);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());

        tracker.Unpin(second);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());

        // only the last unpin lets the low watermark move
        tracker.Unpin(third);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldAllowOutOfOrderUnpin)
    {
        TWatermarkTracker tracker;

        tracker.Advance(10);
        const auto first = tracker.Pin();

        tracker.Advance(20);
        const auto second = tracker.Pin();

        tracker.Unpin(second);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());

        tracker.Unpin(first);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldPinZeroWatermarkBeforeFirstAdvance)
    {
        TWatermarkTracker tracker;

        const auto watermark = tracker.Pin();
        UNIT_ASSERT_VALUES_EQUAL(0, watermark);

        tracker.Advance(10);
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.GetPinnedWatermark());

        tracker.Unpin(watermark);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldPinWatermarkAheadOfTheCurrentOne)
    {
        TWatermarkTracker tracker;

        tracker.Advance(10);

        const auto watermark = tracker.PinAtLeast(20);
        UNIT_ASSERT_VALUES_EQUAL(20, watermark);

        // a pin placed ahead of the watermark holds nothing back yet
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());

        tracker.Advance(20);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetPinnedWatermark());

        // ... and starts holding once the watermark has reached it
        tracker.Advance(30);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetPinnedWatermark());

        tracker.Unpin(watermark);
        UNIT_ASSERT_VALUES_EQUAL(30, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldNotPinWatermarkBehindTheCurrentOne)
    {
        TWatermarkTracker tracker;

        tracker.Advance(10);

        // a position that has already been passed cannot be pinned, the caller
        // learns that from the returned value
        const auto watermark = tracker.PinAtLeast(5);
        UNIT_ASSERT_VALUES_EQUAL(10, watermark);

        tracker.Advance(20);
        UNIT_ASSERT_VALUES_EQUAL(10, tracker.GetPinnedWatermark());

        tracker.Unpin(watermark);
        UNIT_ASSERT_VALUES_EQUAL(20, tracker.GetPinnedWatermark());
    }

    Y_UNIT_TEST(ShouldNotReclaimPastPinnedWatermarkUnderConcurrency)
    {
        // The contract readers rely on: while a watermark is pinned,
        // GetPinnedWatermark() never moves past it, so records with a greater
        // lsn stay alive.
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
                        const auto watermark = tracker.Pin();

                        if (tracker.GetPinnedWatermark() > watermark) {
                            ++contractViolations;
                        }

                        // read the low one first - the high one only grows, so
                        // this ordering keeps the comparison meaningful
                        const auto pinned = tracker.GetPinnedWatermark();
                        if (pinned > GetWatermark(tracker)) {
                            ++orderViolations;
                        }

                        tracker.Unpin(watermark);
                    }
                });
        }

        std::thread writer(
            [&]
            {
                ui64 watermark = 0;
                while (!stop.load(std::memory_order_relaxed)) {
                    tracker.Advance(++watermark);
                }
            });

        for (auto& reader: readers) {
            reader.join();
        }

        stop.store(true, std::memory_order_relaxed);
        writer.join();

        UNIT_ASSERT_VALUES_EQUAL(0, contractViolations.load());
        UNIT_ASSERT_VALUES_EQUAL(0, orderViolations.load());

        // every pin was released, so nothing is pinned any more
        UNIT_ASSERT_VALUES_EQUAL(
            GetWatermark(tracker),
            tracker.GetPinnedWatermark());
    }
}

}   // namespace NCloud::NJournalled
