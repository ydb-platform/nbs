#include "byte_range.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TByteRangeTest)
{
    Y_UNIT_TEST(UnalignedTest)
    {
        TByteRange byteRange0(1034, 10, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(10, byteRange0.UnalignedHeadLength());
        UNIT_ASSERT_VALUES_EQUAL(0, byteRange0.UnalignedTailLength());
        UNIT_ASSERT_VALUES_EQUAL(0, byteRange0.AlignedBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1, byteRange0.BlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0, byteRange0.FirstBlock());

        TByteRange byteRange1(1034, 10 + 4_KB, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(4_KB - 1034, byteRange1.UnalignedHeadLength());
        UNIT_ASSERT_VALUES_EQUAL(1044, byteRange1.UnalignedTailLength());
        UNIT_ASSERT_VALUES_EQUAL(0, byteRange1.AlignedBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(2, byteRange1.BlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0, byteRange1.FirstBlock());
        UNIT_ASSERT_VALUES_EQUAL(1, byteRange1.LastBlock());

        TByteRange byteRange2(1034, 10 + 8_KB, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(4_KB - 1034, byteRange2.UnalignedHeadLength());
        UNIT_ASSERT_VALUES_EQUAL(1044, byteRange2.UnalignedTailLength());
        UNIT_ASSERT_VALUES_EQUAL(1, byteRange2.AlignedBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1, byteRange2.FirstAlignedBlock());
        UNIT_ASSERT_VALUES_EQUAL(3, byteRange2.BlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0, byteRange2.FirstBlock());
        UNIT_ASSERT_VALUES_EQUAL(2, byteRange2.LastBlock());
    }

    Y_UNIT_TEST(AlignedSuperRange)
    {
        auto range1 = TByteRange{1_KB, 1_KB, 4_KB}.AlignedSuperRange();
        UNIT_ASSERT_VALUES_EQUAL(range1.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(range1.Length, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(range1.BlockSize, 4_KB);
        UNIT_ASSERT(range1.IsAligned());

        auto range2 = TByteRange{6_KB, 5_KB, 4_KB}.AlignedSuperRange();
        UNIT_ASSERT_VALUES_EQUAL(range2.Offset, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(range2.Length, 8_KB);
        UNIT_ASSERT_VALUES_EQUAL(range2.BlockSize, 4_KB);
        UNIT_ASSERT(range2.IsAligned());
    }

    Y_UNIT_TEST(ZeroRange)
    {
        TByteRange range(0, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(range.BlockCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.AlignedBlockCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.End(), 0);

        UNIT_ASSERT_VALUES_EQUAL(range.FirstBlock(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.FirstAlignedBlock(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.UnalignedHeadLength(), 0);

        UNIT_ASSERT_VALUES_EQUAL(range.LastBlock(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.UnalignedTailLength(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.UnalignedTailOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.RelativeUnalignedTailOffset(), 0);

        TByteRange other = range.Intersect({0, 100, 4_KB});
        UNIT_ASSERT_VALUES_EQUAL(other.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(other.Length, 0);
        UNIT_ASSERT_VALUES_EQUAL(other.BlockSize, 4_KB);

        UNIT_ASSERT(!range.Overlaps({0, 100, 4_KB}));
        UNIT_ASSERT(!range.Overlaps({0, 0, 4_KB}));
    }

    Y_UNIT_TEST(ZeroRangeWithOffset)
    {
        TByteRange range(16_KB + 100, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(range.BlockCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.AlignedBlockCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.End(), 16_KB + 100);

        UNIT_ASSERT_VALUES_EQUAL(range.FirstBlock(), 4);
        UNIT_ASSERT_VALUES_EQUAL(range.FirstAlignedBlock(), 5);
        UNIT_ASSERT_VALUES_EQUAL(range.UnalignedHeadLength(), 0);

        UNIT_ASSERT_VALUES_EQUAL(range.LastBlock(), 4);
        UNIT_ASSERT_VALUES_EQUAL(range.UnalignedTailLength(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.UnalignedTailOffset(), 16_KB + 100);
        UNIT_ASSERT_VALUES_EQUAL(range.RelativeUnalignedTailOffset(), 0);

        UNIT_ASSERT(!range.Overlaps({0, 0, 4_KB}));
    }
}

}   // namespace NCloud
