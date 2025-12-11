#pragma once

#include "fresh_blob.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 FirstRequestDeletionId = 4321;

TVector<TVector<TString>> GetBuffers(ui32 bs)
{
    return {
        // [0; 0]
        TVector<TString>{TString(bs, 'A')},
        // [1000, 1000 + 7]
        TVector<TString>{
            TString(bs, 'B'),
            TString(bs, 'C'),
            TString(bs, 'D'),
            TString(bs, 'E'),
            TString(bs, 'F'),
            TString(bs, 'G'),
            TString(bs, 'H'),
            TString(bs, 'I'),
        },
        // [2000, 2000 + 1]
        TVector<TString>{
            TString(bs, 'J'),
            TString(bs, 'K'),
        },
        // [3000, 3000 + 3]
        TVector<TString>{
            TString(bs, 'L'),
            TString(bs, 'M'),
            TString(bs, 'N'),
            TString(bs, 'O'),
        },
    };
}

TVector<TBlockRange32> GetBlockRanges()
{
    return {
        TBlockRange32::MakeOneBlock(0),
        TBlockRange32::WithLength(1000, 8),
        TBlockRange32::WithLength(2000, 2),
        TBlockRange32::WithLength(3000, 4),
    };
}

TVector<ui32> GetBlockIndices(const TVector<TBlockRange32>& blockRanges)
{
    TVector<ui32> result;
    for (const auto& blockRange: blockRanges) {
        for (const ui32 blockIndex: xrange(blockRange)) {
            result.push_back(blockIndex);
        }
    }
    return result;
}

TVector<TGuardHolder> GetHolders(const TVector<TVector<TString>>& buffers)
{
    TVector<TGuardHolder> holders;

    for (const auto& sub: buffers) {
        TSgList sgList;
        for (const auto& buf: sub) {
            sgList.emplace_back(buf.data(), buf.size());
        }

        holders.emplace_back(TGuardedSgList{std::move(sgList)});
    }

    return holders;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
