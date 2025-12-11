#include "block_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockBufferTest)
{
    struct TTest
    {
        TByteRange Range;
        TByteRange AlignedRange;
        TString Data;
        IBlockBufferPtr BlockBuffer;
        TString Out;

        TTest(ui64 offset, ui64 length, ui32 blockSize, ui64 fileSize)
            : Range(offset, length, blockSize)
            , AlignedRange(Range.AlignedSuperRange())
        {
            Data.ReserveAndResize(AlignedRange.Length);
            for (ui32 i = 0; i < Data.Size(); ++i) {
                Data[i] = 'a' + RandomNumber<ui32>('z' - 'a' + 1);
            }
            BlockBuffer = CreateBlockBuffer(AlignedRange, Data);
            CopyFileData(
                "logtag",
                Range,
                AlignedRange,
                fileSize,
                *BlockBuffer,
                &Out);
        }
    };

    Y_UNIT_TEST(ShouldCopyFileDataAligned)
    {
        TTest test(100_KB, 8_KB, 4_KB, 200_KB);
        UNIT_ASSERT_VALUES_EQUAL(test.Data, test.Out);
    }

    Y_UNIT_TEST(ShouldCopyFileDataUnaligned)
    {
        TTest test(101_KB, 10_KB, 4_KB, 107_KB);
        UNIT_ASSERT_VALUES_EQUAL(test.Data.substr(1_KB, 6_KB), test.Out);
    }

    Y_UNIT_TEST(ShouldCopyFileDataUnalignedSmall)
    {
        TTest test(101_KB, 10_KB, 4_KB, 103_KB);
        UNIT_ASSERT_VALUES_EQUAL(test.Data.substr(1_KB, 2_KB), test.Out);
    }
}

}   // namespace NCloud::NFileStore::NStorage
