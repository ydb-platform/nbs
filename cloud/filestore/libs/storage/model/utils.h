#pragma once

#include <util/generic/algorithm.h>
#include <util/generic/bitops.h>
#include <util/generic/vector.h>

#include <google/protobuf/repeated_ptr_field.h>

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
    Y_DEBUG_ABORT_UNLESS(shardNo < (1UL << (64 - realBits)));
    return (static_cast<ui64>(shardNo) << realBits) | (realMask & id);
}

inline ui32 ExtractShardNo(ui64 id)
{
    const auto realBits = 56U;
    return id >> realBits;
}

template <typename T>
void RemoveByIndices(
    google::protobuf::RepeatedPtrField<T>& field,
    TVector<ui32>& indices)
{
    Sort(indices);

    int j = 0;
    for (int i = static_cast<int>(indices[0]); i < field.size(); ++i) {
        if (j < static_cast<int>(indices.size())
                && i == static_cast<int>(indices[j]))
        {
            ++j;
            continue;
        }

        field[i - j] = std::move(field[i]);
    }

    while (j) {
        field.RemoveLast();
        --j;
    }
}

}   // namespace NCloud::NFileStore::NStorage
