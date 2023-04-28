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

TStringBuf SubBuffer(const TVector<char>& buffer, TBlockRange64 range)
{
    return {
        buffer.data() + range.Start * BlockSize,
        range.Size() * BlockSize
    };
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

            for (auto c: SubBuffer(buffer, { 0, 9 })) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, { 10, 15 })) {
                UNIT_ASSERT_VALUES_EQUAL('X', c);
            }
        }

        {
            NProto::TIOVector iov = CreateIOVector(4, 'B');

            CopyToSgList(iov, sglist, 5, BlockSize);

            for (auto c: SubBuffer(buffer, { 0, 4 })) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, { 5, 8 })) {
                UNIT_ASSERT_VALUES_EQUAL('B', c);
            }

            for (auto c: SubBuffer(buffer, { 9, 9 })) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, { 10, 15 })) {
                UNIT_ASSERT_VALUES_EQUAL('X', c);
            }
        }

        {
            NProto::TIOVector iov = CreateIOVector(4, 'C');

            CopyToSgList(iov, sglist, 12, BlockSize);

            for (auto c: SubBuffer(buffer, { 0, 4 })) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, { 5, 8 })) {
                UNIT_ASSERT_VALUES_EQUAL('B', c);
            }

            for (auto c: SubBuffer(buffer, { 9, 9 })) {
                UNIT_ASSERT_VALUES_EQUAL('A', c);
            }

            for (auto c: SubBuffer(buffer, { 10, 11 })) {
                UNIT_ASSERT_VALUES_EQUAL('X', c);
            }

            for (auto c: SubBuffer(buffer, { 12, 15 })) {
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

            for (auto c: SubBuffer(buffer, { 0, 0 })) {
                UNIT_ASSERT_VALUES_EQUAL('Z', c);
            }

            for (auto c: SubBuffer(buffer, { 1, 2 })) {
                UNIT_ASSERT_VALUES_EQUAL('D', c);
            }

            for (auto c: SubBuffer(buffer, { 3, 15 })) {
                UNIT_ASSERT_VALUES_EQUAL('Z', c);
            }
        }
    }
}

}   // namespace NCloud::NBlockStore
