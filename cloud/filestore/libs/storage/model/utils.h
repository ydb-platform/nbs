#pragma once

#include <util/generic/bitops.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline bool IsAligned(size_t len, size_t align) noexcept
{
    Y_ASSERT(IsPowerOf2(align));
    return (len & (align - 1)) == 0;
}

inline ui64 ShardedId(ui64 id, ui32 shardNo)
{
    const auto realBits = 56U;
    const auto realMask = Max<ui64>() >> (64 - realBits);
    return (static_cast<ui64>(shardNo) << realBits) | (realMask & id);
}

}   // namespace NCloud::NFileStore::NStorage
