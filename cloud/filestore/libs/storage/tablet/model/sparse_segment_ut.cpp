#include "sparse_segment.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

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
}

}   // namespace NCloud::NFileStore::NStorage
