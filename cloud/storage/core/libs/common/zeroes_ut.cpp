#include "zeroes.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TZeroesTest)
{
    Y_UNIT_TEST(ShouldCheckThatBufferContainsOnlyZeroes)
    {
        UNIT_ASSERT(IsAllZeroes(nullptr, 0));

        // sizes around the word size, aligned and unaligned starts
        for (size_t size = 1; size <= 64; ++size) {
            for (size_t shift = 0; shift < 8; ++shift) {
                TString buffer(shift + size, 0);
                const char* data = buffer.data() + shift;
                UNIT_ASSERT_C(
                    IsAllZeroes(data, size),
                    "size " << size << " shift " << shift);

                for (size_t i = 0; i < size; ++i) {
                    buffer[shift + i] = 'x';
                    UNIT_ASSERT_C(
                        !IsAllZeroes(data, size),
                        "size " << size << " shift " << shift << " pos " << i);
                    buffer[shift + i] = 0;
                }
            }
        }
    }
}

}   // namespace NCloud
