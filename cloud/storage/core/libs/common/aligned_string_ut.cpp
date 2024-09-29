#include "aligned_string.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAlignedStringTest)
{
    Y_UNIT_TEST(ShouldAlignToPowerOf2)
    {
        TVector<TString> buffers;
        for (ui32 size: {123, 1234, 12345, 123456, 1234567}) {
            for (ui32 alignShift = 0; alignShift < 17; alignShift++) {
                auto align = 1 << alignShift;
                auto [buffer, alignOffset] = AlignedString(size, align);
                UNIT_ASSERT_EQUAL(alignOffset + size, buffer.size());
                if (align != 0) {
                    auto alignedBuffer =
                        reinterpret_cast<uintptr_t>(buffer.data() + alignOffset);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        alignedBuffer % align,
                        0,
                        "size=" << size <<
                        " ,align=" << align <<
                        " ,buffer=" << (void*)buffer.data() <<
                        " ,alignOffset=" << alignOffset);
                }
                buffers.push_back(std::move(buffer));
            }
        }
    }

    Y_UNIT_TEST(ShouldAlignTo0)
    {
        auto [buffer, offset] = AlignedString(5678, 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 5678);
        UNIT_ASSERT_VALUES_EQUAL(offset, 0);
    }
}

}   // namespace NCloud
