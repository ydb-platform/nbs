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

namespace {

constexpr ui64 SeventhByteMask = static_cast<ui64>(Max<ui8>()) << (6U * 8U);

constexpr ui32 IdBitsCount = 48;
constexpr ui64 IdMask = Max<ui64>() >> (64 - IdBitsCount);

// It is assumed that two most significant bytes are equal to zero
inline ui32 SwapTwoLeastSignificantBytes(ui32 shardNo)
{
    ui32 leastSignificantByte = (shardNo & 0xff);
    shardNo >>= CHAR_BIT;
    return shardNo | (leastSignificantByte << CHAR_BIT);
}

}   // namespace

inline ui64 ShardedId(ui64 id, ui32 shardNo)
{
    // Historically, shardNo occupied only the 8th byte.
    // To place the second byte of shardNo into the 7th byte of the
    // resulting id, we need to swap the 7th and 8th bytes.
    shardNo = SwapTwoLeastSignificantBytes(shardNo);
    Y_DEBUG_ABORT_UNLESS(shardNo < (1UL << (64 - IdBitsCount)));
    return (static_cast<ui64>(shardNo) << IdBitsCount) | (IdMask & id);
}

inline ui32 ExtractShardNo(ui64 id)
{
    ui32 shardNo = static_cast<ui32>(id >> IdBitsCount);
    shardNo = SwapTwoLeastSignificantBytes(shardNo);

    return shardNo;
}

inline bool IsSeventhByteUsed(ui64 id)
{
    return (id & SeventhByteMask) != 0;
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
