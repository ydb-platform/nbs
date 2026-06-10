#include "blocks_bloom_filter.h"

#include <contrib/ydb/core/tablet_flat/flat_bloom_writer.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

TBlocksBloomFilter::TBlocksBloomFilter(ui32 elementsCount, double errorRate)
{
    NKikimr::NTable::NBloom::TEstimator estimator(
        static_cast<float>(errorRate));
    Hashes = estimator.Hashes();
    Items = estimator.Bits(elementsCount);
    Y_ABORT_UNLESS(Hashes && Items);

    size_t size = (Items >> 6) * sizeof(ui64);

    Raw = NActors::TSharedData::Uninitialized(size);

    Array = {
        NKikimr::TDeref<ui64>::At(Raw.mutable_begin(), 0),
        size_t(Items >> 6)};

    Y_ABORT_UNLESS(
        NKikimr::TDeref<char>::At(Array.end(), 0) == Raw.mutable_end());

    std::ranges::fill(Array, 0);
}

void TBlocksBloomFilter::Add(ui32 blockIndex)
{
    NKikimr::NTable::NBloom::THash hashRoot(
        TArrayRef<const char>(
            reinterpret_cast<const char*>(&blockIndex),
            sizeof(blockIndex)));

    NKikimr::NTable::NBloom::THash hash(hashRoot);

    for (ui32 seq = 0; seq++ < Hashes;) {
        const ui64 num = hash.Next() % Items;

        Array[num >> 6] |= ui64(1) << (num & 0x3f);
    }
}

bool TBlocksBloomFilter::Test(ui32 blockIndex) const
{
    NKikimr::NTable::NBloom::THash hashRoot(
        TArrayRef<const char>(
            reinterpret_cast<const char*>(&blockIndex),
            sizeof(blockIndex)));

    NKikimr::NTable::NBloom::THash hash(hashRoot);

    for (ui32 seq = 0; seq++ < Hashes;) {
        const ui64 num = hash.Next() % Items;

        if (!(Array[num >> 6] & (ui64(1) << (num & 0x3f)))) {
            return false;
        }
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
