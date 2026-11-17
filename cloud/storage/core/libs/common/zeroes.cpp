#include "zeroes.h"

#include <cstdint>
#include <cstring>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

bool IsAllZeroes(const char* src, size_t size)
{
    using TBigNumber = ui64;

    if (size < sizeof(TBigNumber)) {
        for (size_t i = 0; i < size; ++i) {
            if (src[i] != 0) {
                return false;
            }
        }
        return true;
    }

    const bool isAligned =
        reinterpret_cast<std::uintptr_t>(src) % sizeof(TBigNumber) == 0;

    if (isAligned) {
        const TBigNumber* const a = reinterpret_cast<const TBigNumber*>(src);
        return !a[0] && !memcmp(
                            src,
                            src + sizeof(TBigNumber),
                            size - sizeof(TBigNumber));
    }
    return !src[0] && !memcmp(src, src + sizeof(char), size - sizeof(char));
}

}   // namespace NCloud
