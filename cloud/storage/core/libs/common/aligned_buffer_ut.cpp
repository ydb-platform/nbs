#include "aligned_buffer.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>

namespace NCloud {

namespace {

#define UNIT_ASSERT_PTR_EQUAL(A, B) \
    UNIT_ASSERT_VALUES_EQUAL(static_cast<void*>(A), static_cast<void*>(B))

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAlignedBufferTest)
{
    Y_UNIT_TEST(ShouldAlignToPowerOf2)
    {
        TVector<TAlignedBuffer> buffers;
        for (ui32 size: {123, 1234, 12345, 123456, 1234567}) {
            for (ui32 alignShift = 0; alignShift < 17; alignShift++) {
                auto align = 1 << alignShift;
                TAlignedBuffer buffer(size, align);

                Cerr << "size=" << size << ", align=" << align
                     << ", offset=" << buffer.AlignedDataOffset()
                     << ", begin=" << static_cast<void*>(buffer.Begin())
                     << Endl;
                UNIT_ASSERT_EQUAL(size, buffer.Size());
                UNIT_ASSERT_VALUES_EQUAL_C(
                    reinterpret_cast<uintptr_t>(buffer.Begin()) % align,
                    0,
                    "size=" << size << " ,align=" << align << " ,buffer="
                            << static_cast<void*>(buffer.Begin()));
                UNIT_ASSERT_VALUES_EQUAL_C(
                    buffer.AlignedDataOffset(),
                    buffer.Begin() - buffer.GetBuffer().begin(),
                    "size=" << size << " ,align=" << align << " ,buffer="
                            << static_cast<void*>(buffer.Begin()));
                buffers.push_back(std::move(buffer));
            }
        }
    }

    Y_UNIT_TEST(ShouldAlignTo0)
    {
        TAlignedBuffer buffer(5678, 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), 5678);
    }

    Y_UNIT_TEST(ShouldReconstructAlignedBuffer)
    {
        ui32 align = 1 << 21;
        ui32 size = 5678;

        TAlignedBuffer buffer1(size, align);
        UNIT_ASSERT_VALUES_EQUAL(buffer1.Size(), size);

        auto* buffer1Mem = buffer1.Begin();

        TAlignedBuffer buffer2(buffer1.TakeBuffer(), align);
        UNIT_ASSERT_VALUES_EQUAL(size, buffer2.Size());

        auto* buffer2Mem = buffer2.Begin();
        UNIT_ASSERT_PTR_EQUAL(buffer1Mem, buffer2Mem);
    }

    Y_UNIT_TEST(ShouldMoveAlignedBuffer)
    {
        ui32 align = 1 << 21;
        ui32 size = 5678;

        TAlignedBuffer buffer0(size, align);
        UNIT_ASSERT_VALUES_EQUAL(buffer0.Size(), size);

        auto* bufferMem = buffer0.Begin();

        TAlignedBuffer buffer;
        UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), 0);

        buffer = std::move(buffer0);
        UNIT_ASSERT_PTR_EQUAL(bufferMem, buffer.Begin());
        UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), size);
        UNIT_ASSERT_VALUES_EQUAL(0, buffer0.Size());

        TAlignedBuffer buffer2(std::move(buffer));
        UNIT_ASSERT_PTR_EQUAL(bufferMem, buffer2.Begin());
        UNIT_ASSERT_VALUES_EQUAL(buffer2.Size(), size);
        UNIT_ASSERT_VALUES_EQUAL(0, buffer.Size());

        TAlignedBuffer buffer3 = std::move(buffer2);
        UNIT_ASSERT_PTR_EQUAL(bufferMem, buffer3.Begin());
        UNIT_ASSERT_VALUES_EQUAL(buffer3.Size(), size);
        UNIT_ASSERT_VALUES_EQUAL(0, buffer2.Size());
    }

    Y_UNIT_TEST(ShouldResizeAlignedBuffer)
    {
        ui32 align = 1 << 21;
        ui32 size = 5678;

        TAlignedBuffer buffer(size, align);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), size);

        auto* bufferBegin = buffer.Begin();
        buffer.TrimSize(5555);
        UNIT_ASSERT_VALUES_EQUAL(5555, buffer.Size());
        UNIT_ASSERT_PTR_EQUAL(bufferBegin, buffer.Begin());

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            buffer.TrimSize(5556),
            TServiceError,
            "Tried to trim to size 5556 > 5555");
    }
}

}   // namespace NCloud
