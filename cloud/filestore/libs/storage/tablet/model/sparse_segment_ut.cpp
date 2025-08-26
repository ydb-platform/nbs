#include "sparse_segment.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

TVector<TSparseSegment::TRange> GetRanges(TSparseSegment& sparseSegment)
{
    TVector<TSparseSegment::TRange> ret;
    for (const auto& segment: sparseSegment) {
        ret.push_back(segment);
    }

    return ret;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSparseSegmentTest)
{
    Y_UNIT_TEST(ShouldPunchHoles)
    {
        TSparseSegment segment(TDefaultAllocator::Instance(), 0, 100);
        UNIT_ASSERT(!segment.Empty());
        segment.PunchHole(10, 20);
        UNIT_ASSERT(!segment.Empty());
        segment.PunchHole(5, 15);
        UNIT_ASSERT(!segment.Empty());
        segment.PunchHole(30, 75);
        UNIT_ASSERT(!segment.Empty());
        segment.PunchHole(75, 99);
        UNIT_ASSERT(!segment.Empty());
        segment.PunchHole(20, 30);
        UNIT_ASSERT(!segment.Empty());
        segment.PunchHole(99, 100);
        UNIT_ASSERT(!segment.Empty());
        segment.PunchHole(0, 5);
        UNIT_ASSERT(segment.Empty());
        segment.PunchHole(0, 111);
        UNIT_ASSERT(segment.Empty());

        TSparseSegment segment2(TDefaultAllocator::Instance(), 100, 200);
        UNIT_ASSERT(!segment2.Empty());
        segment2.PunchHole(95, 222);
        UNIT_ASSERT(segment2.Empty());

        TSparseSegment segment3(TDefaultAllocator::Instance(), 100, 200);
        UNIT_ASSERT(!segment3.Empty());
        segment3.PunchHole(100, 120);
        UNIT_ASSERT(!segment3.Empty());
        segment3.PunchHole(120, 140);
        UNIT_ASSERT(!segment3.Empty());
        segment3.PunchHole(140, 160);
        UNIT_ASSERT(!segment3.Empty());
        segment3.PunchHole(160, 180);
        UNIT_ASSERT(!segment3.Empty());
        segment3.PunchHole(180, 200);
        UNIT_ASSERT(segment3.Empty());
    }

    Y_UNIT_TEST(ShouldSplitIntervals)
    {
        TSparseSegment segment(TDefaultAllocator::Instance(), 0, 100);
        segment.PunchHole(10, 90);

        auto ranges = GetRanges(segment);
        TVector<TSparseSegment::TRange> expectedRanges = {
            {.Start = 0, .End = 10},
            {.Start = 90, .End = 100}};
        UNIT_ASSERT_VALUES_EQUAL(ranges, expectedRanges);
    }

    Y_UNIT_TEST(ShouldMergePunchedIntervals)
    {
        TSparseSegment segment(TDefaultAllocator::Instance(), 0, 100);
        segment.PunchHole(20, 30);
        segment.PunchHole(80, 90);
        segment.PunchHole(10, 90);

        auto ranges = GetRanges(segment);
        TVector<TSparseSegment::TRange> expectedRanges = {
            {.Start = 0, .End = 10},
            {.Start = 90, .End = 100}};
        UNIT_ASSERT_VALUES_EQUAL(ranges, expectedRanges);
    }

    Y_UNIT_TEST(ShouldIgnoreEmptyInterval)
    {
        TSparseSegment segment(TDefaultAllocator::Instance(), 0, 100);
        segment.PunchHole(0, 0);
        segment.PunchHole(100, 100);

        auto ranges = GetRanges(segment);
        TVector<TSparseSegment::TRange> expectedRanges = {
            {.Start = 0, .End = 100}};
        UNIT_ASSERT_VALUES_EQUAL(ranges, expectedRanges);
    }

    Y_UNIT_TEST(ShouldIgnoreOutOfRangeInterval)
    {
        TSparseSegment segment(TDefaultAllocator::Instance(), 20, 100);
        segment.PunchHole(0, 5);
        segment.PunchHole(100, 200);
        segment.PunchHole(200, 500);

        auto ranges = GetRanges(segment);
        TVector<TSparseSegment::TRange> expectedRanges = {
            {.Start = 20, .End = 100}};
        UNIT_ASSERT_VALUES_EQUAL(ranges, expectedRanges);
    }

    Y_UNIT_TEST(ShouldPunchIntervalsOnEdges)
    {
        TSparseSegment segment(TDefaultAllocator::Instance(), 10, 100);
        segment.PunchHole(0, 11);
        segment.PunchHole(90, 100);

        auto ranges = GetRanges(segment);
        TVector<TSparseSegment::TRange> expectedRanges = {
            {.Start = 11, .End = 90}};
        UNIT_ASSERT_VALUES_EQUAL(ranges, expectedRanges);
    }

    Y_UNIT_TEST(ShouldPunchWholeInterval)
    {
        {
            TSparseSegment segment(TDefaultAllocator::Instance(), 10, 100);
            segment.PunchHole(10, 100);

            auto ranges = GetRanges(segment);
            UNIT_ASSERT(ranges.empty());
        }

        {
            TSparseSegment segment(TDefaultAllocator::Instance(), 10, 100);
            segment.PunchHole(0, 200);

            auto ranges = GetRanges(segment);
            UNIT_ASSERT(ranges.empty());
        }

        {
            TSparseSegment segment(TDefaultAllocator::Instance(), 10, 100);
            segment.PunchHole(10, 20);
            segment.PunchHole(20, 30);
            segment.PunchHole(30, 100);

            auto ranges = GetRanges(segment);
            UNIT_ASSERT(ranges.empty());
        }
    }

    Y_UNIT_TEST(ShouldPunchRandomIntervals)
    {
        auto currentTime = time(0);
        srand(currentTime);

        auto testContext = TStringBuilder();
        testContext << "currentTime: " << currentTime;

        ui64 segmentStart = rand() % 1_KB;
        ui64 segmentEnd = segmentStart + 1 + rand() % 8_KB;

        testContext << " segmentStart: " << segmentStart
                    << " segmentEnd: " << segmentEnd;

        TSparseSegment segment(
            TDefaultAllocator::Instance(),
            segmentStart,
            segmentEnd);

        size_t numHoles = rand() % 10 + 1;

        testContext << " numHoles: " << numHoles;

       // Set each vector element to true at the indices where there are holes
        std::vector<bool> segmentView(segmentEnd, false);
        for (size_t i = 0; i < numHoles; ++i) {
            ui64 point1 = rand() % 10_KB;
            ui64 point2 = rand() % 10_KB;

            ui64 holeStart = std::min(point1, point2);
            ui64 holeEnd = std::max(point1, point2);

            testContext << " holeStart: " << holeStart;
            testContext << " holeEnd: " << holeEnd;

            segment.PunchHole(holeStart, holeEnd);
            for (size_t i = holeStart; i < holeEnd; ++i) {
                segmentView[i] = true;
            }
        }

        size_t startIndex = segmentStart;
        for (const auto& range: segment) {
            // Test that values outside of segment are positive
            for (size_t i = startIndex; i < range.Start; ++i) {
                UNIT_ASSERT_C(
                    segmentView[i],
                    testContext << " invalid index: " << i);
            }
            startIndex = range.End;
            // Test that values inside of segment are positive
            for (size_t i = range.Start; i < range.End; ++i) {
                UNIT_ASSERT_C(
                    !segmentView[i],
                    testContext << " invalid index: " << i);
            }
        }

        if (segment.Empty()) {
            // Test that all values in the original range are positive if
            // the segment is empty
            for (size_t i = startIndex; i < segmentEnd; ++i) {
                UNIT_ASSERT_C(
                    segmentView[i],
                    testContext << " invalid index: " << i);
            }
        } else {
            // Test that all values behind last range of the segment are
            // positive
            for (size_t i = startIndex; i < segmentEnd; ++i) {
                if (segmentView[i]) {
                    UNIT_ASSERT_C(
                        segmentView[i],
                        testContext << " invalid index: " << i);
                }
            }
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
