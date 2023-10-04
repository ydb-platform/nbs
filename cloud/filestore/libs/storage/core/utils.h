#pragma once

#include "public.h"

#include <util/generic/bitops.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline bool IsAligned(size_t len, size_t align) noexcept
{
    Y_ASSERT(IsPowerOf2(align));
    return (len & (align - 1)) == 0;
}

}   // namespace NCloud::NFileStore::NStorage
