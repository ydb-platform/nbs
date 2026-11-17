#pragma once

#include "public.h"

#include <util/system/types.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Checks that the buffer contains only zeros.
[[nodiscard]] bool IsAllZeroes(const char* src, size_t size);

}   // namespace NCloud
