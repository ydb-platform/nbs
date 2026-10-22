#include "block_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockBufferTest)
{
    template <typename TBuffer>
    struct TTest
    {
        TByteRange Range;
        TByteRange AlignedRange;
        TBuffer Data;
        IBlockBufferPtr BlockBuffer;
        TString Out;

        TTest(
                ui64 offset,
                ui64 length,
                ui32 blockSize,
                ui64 fileSize)
            : Range(offset, length, blockSize)
            , AlignedRange(Range.AlignedSuperRange())
        {
            TString data;
            data.ReserveAndResize(AlignedRange.Length);
            for (ui32 i = 0; i < data.size(); ++i) {
                data[i] = 'a' + RandomNumber<ui32>('z' - 'a' + 1);
            }
            Data = TBuffer(std::move(data));
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

    template <typename TBuffer>
    void ShouldCopyFileDataAligned()
    {
        TTest<TBuffer> test(100_KB, 8_KB, 4_KB, 200_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(test.Data.data(), test.Data.size()),
            test.Out);
    }

    template <typename TBuffer>
    void ShouldCopyFileDataUnaligned()
    {
        TTest<TBuffer> test(101_KB, 10_KB, 4_KB, 107_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(test.Data.data(), test.Data.size()).SubStr(1_KB, 6_KB),
            test.Out);
    }

    template <typename TBuffer>
    void ShouldCopyFileDataUnalignedSmall()
    {
        TTest<TBuffer> test(101_KB, 10_KB, 4_KB, 103_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(test.Data.data(), test.Data.size()).SubStr(1_KB, 2_KB),
            test.Out);
    }

    Y_UNIT_TEST(ShouldCopyFileDataAligned_TString)
    {
        ShouldCopyFileDataAligned<TString>();
    }

    Y_UNIT_TEST(ShouldCopyFileDataAligned_TRcBuf)
    {
        ShouldCopyFileDataAligned<TRcBuf>();
    }

    Y_UNIT_TEST(ShouldCopyFileDataUnaligned_TString)
    {
        ShouldCopyFileDataUnaligned<TString>();
    }

    Y_UNIT_TEST(ShouldCopyFileDataUnaligned_TRcBuf)
    {
        ShouldCopyFileDataUnaligned<TRcBuf>();
    }

    Y_UNIT_TEST(ShouldCopyFileDataUnalignedSmall_TString)
    {
        ShouldCopyFileDataUnalignedSmall<TString>();
    }

    Y_UNIT_TEST(ShouldCopyFileDataUnalignedSmall_TRcBuf)
    {
        ShouldCopyFileDataUnalignedSmall<TRcBuf>();
    }

    Y_UNIT_TEST(ShouldSupportCopyOnWrite)
    {
        const ui32 kBlockScount = 4;
        const ui32 kBlockSize = 4_KB;
        TRcBuf buffer(TString(kBlockScount * kBlockSize, 'A'));
        auto blockBuffer = CreateBlockBuffer(
            TByteRange{0, kBlockScount * kBlockSize, kBlockSize},
            buffer);

        TString block1(kBlockSize, 'B');
        blockBuffer->SetBlock(0, block1);

        auto receivedBlock = blockBuffer->GetBlock(0);

        UNIT_ASSERT_VALUES_EQUAL(
            block1,
            TString(receivedBlock.data(), kBlockSize));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(kBlockSize, 'A'),
            TString(buffer.GetData(), kBlockSize));
    }
}

}   // namespace NCloud::NFileStore::NStorage
