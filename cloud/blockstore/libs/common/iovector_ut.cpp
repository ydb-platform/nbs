#include "iovector.h"
#include "block_range.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 BlockSize = 8;

////////////////////////////////////////////////////////////////////////////////

TSgList SplitBuffer(
    const TVector<char>& buffer,
    std::initializer_list<int> blocks)
{
    TSgList sglist(Reserve(blocks.size()));

    auto p = buffer.data();
    for (int n: blocks) {
        const ui64 len = n * BlockSize;
        sglist.push_back({ p, len });
        p += len;
    }

    return sglist;
}

NProto::TIOVector CreateIOVector(ui64 blockCount, char data)
{
    NProto::TIOVector iov;
    auto& buffers = *iov.MutableBuffers();

    for (ui64 i = 0; i != blockCount; ++i) {
        buffers.Add()->resize(BlockSize, data);
    }

    return iov;
}

TStringBuf SubBuffer(
    const TVector<char>& buffer,
    ui64 startBlock,
    size_t blockCount)
{
    return {buffer.data() + startBlock * BlockSize, blockCount * BlockSize};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIOVectorTest)
{
    Y_UNIT_TEST(ShouldCopyToSgList)
    {
        TVector<char> buffer(16 * BlockSize, 'X');

        TSgList sglist = SplitBuffer(buffer, { 4, 4, 6, 2 });

        {
            NProto::TIOVector iov = CreateIOVector(10, 'A');

            CopyToSgList(iov, sglist, 0, BlockSize);

            for (auto c: SubBuffer(buffer, 0, 10)) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, 10, 6)) {
                UNIT_ASSERT_VALUES_EQUAL('X', c);
            }
        }

        {
            NProto::TIOVector iov = CreateIOVector(4, 'B');

            CopyToSgList(iov, sglist, 5, BlockSize);

            for (auto c: SubBuffer(buffer, 0, 5)) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, 5, 4)) {
                UNIT_ASSERT_VALUES_EQUAL('B', c);
            }

            for (auto c: SubBuffer(buffer, 9, 1)) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, 10, 6)) {
                UNIT_ASSERT_VALUES_EQUAL('X', c);
            }
        }

        {
            NProto::TIOVector iov = CreateIOVector(4, 'C');

            CopyToSgList(iov, sglist, 12, BlockSize);

            for (auto c: SubBuffer(buffer, 0, 5)) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, 5, 4)) {
                UNIT_ASSERT_VALUES_EQUAL('B', c);
            }

            for (auto c: SubBuffer(buffer, 9, 1)) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, 10, 2)) {
                UNIT_ASSERT_VALUES_EQUAL('X', c);
            }

            for (auto c: SubBuffer(buffer, 12, 4)) {
                UNIT_ASSERT_VALUES_EQUAL('C', c);
            }
        }

        {
            NProto::TIOVector iov = CreateIOVector(16, 'Z');

            CopyToSgList(iov, sglist, 0, BlockSize);

            for (auto c: buffer) {
                UNIT_ASSERT_VALUES_EQUAL('Z', c);
            }
        }

        {
            NProto::TIOVector iov = CreateIOVector(2, 'D');

            CopyToSgList(iov, sglist, 1, BlockSize);

            for (auto c: SubBuffer(buffer, 0, 1)) {
                UNIT_ASSERT_VALUES_EQUAL('Z', c);
            }

            for (auto c: SubBuffer(buffer, 1, 2)) {
                UNIT_ASSERT_VALUES_EQUAL('D', c);
            }

            for (auto c: SubBuffer(buffer, 3, 13)) {
                UNIT_ASSERT_VALUES_EQUAL('Z', c);
            }
        }
    }

    Y_UNIT_TEST(ShouldCopyAndTrimVoidBuffers)
    {
        const ui64 blockCount = 10;
        TVector<char> buffer(blockCount * BlockSize, 0);

        // Fill last two blocks with data
        std::fill(buffer.begin() + 8 * BlockSize, buffer.end(), 255);

        // Check that the first 8 blocks will be optimized.
        NProto::TIOVector ioVector;
        auto handledByteCount = CopyAndTrimVoidBuffers(
            TBlockDataRef{buffer.data(), buffer.size()},
            blockCount,
            BlockSize,
            &ioVector);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), handledByteCount);
        for (size_t i = 0; i < blockCount; ++i) {
            const auto& buf  = ioVector.GetBuffers(i);
            UNIT_ASSERT_VALUES_EQUAL(
                i < 8 ? 0 : BlockSize,
                buf.size());
        }
        TVoidBuffersStat stat = CountVoidBuffers(ioVector);
        UNIT_ASSERT_VALUES_EQUAL(8, stat.VoidBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(2, stat.NonVoidBlockCount);
    }

    Y_UNIT_TEST(ShouldTrimVoidBuffers)
    {
        const ui64 blockCount = 10;
        TVector<char> buffer(blockCount * BlockSize, 0);

        // Fill last two blocks with data
        std::fill(buffer.begin() + 8 * BlockSize, buffer.end(), 255);

        // Fill ioVector without optimizations
        NProto::TIOVector ioVector;
        auto sgList = ResizeIOVector(ioVector, blockCount, BlockSize);
        auto bytesCopied =
            SgListCopy(TBlockDataRef{buffer.data(), buffer.size()}, sgList);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), bytesCopied);

        // Check that no void buffers have been created.
        TVoidBuffersStat stat = CountVoidBuffers(ioVector);
        UNIT_ASSERT_VALUES_EQUAL(0, stat.VoidBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(blockCount, stat.NonVoidBlockCount);

        // Remove void buffers.
        TrimVoidBuffers(&ioVector);
        for (size_t i = 0; i < blockCount; ++i) {
            const auto& buf = ioVector.GetBuffers(i);
            UNIT_ASSERT_VALUES_EQUAL(i < 8 ? 0 : BlockSize, buf.size());
        }
        stat = CountVoidBuffers(ioVector);
        UNIT_ASSERT_VALUES_EQUAL(8, stat.VoidBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(2, stat.NonVoidBlockCount);
    }
}

}   // namespace NCloud::NBlockStore
