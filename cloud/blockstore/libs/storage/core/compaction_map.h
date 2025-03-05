#pragma once

#include "compaction_policy.h"
#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/vector.h>
#include <util/system/align.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TCompactionCounter
{
    ui32 BlockIndex;
    TRangeStat Stat;

    TCompactionCounter(ui32 blockIndex, TRangeStat stat)
        : BlockIndex(blockIndex)
        , Stat(stat)
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionMap
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    // Number of ranges in one group
    static constexpr ui32 GroupSize = MaxBlocksCount;

    static ui32 GetGroupStart(ui32 blockIndex, ui32 rangeSize)
    {
        return AlignDown(blockIndex, rangeSize * GroupSize);
    }

    static ui32 GetRangeStart(ui32 blockIndex, ui32 rangeSize)
    {
        return AlignDown(blockIndex, rangeSize);
    }

    TCompactionMap(ui32 rangeSize, ICompactionPolicyPtr policy);
    ~TCompactionMap();

    static void UpdateCompactionCounter(ui32 source, ui16* target)
    {
        *target = source > Max<ui16>() ? Max<ui16>() : source;
    }

    void Update(
        const TVector<TCompactionCounter>& counters,
        const TCompressedBitmap* used);

    void Update(
        ui32 blockIndex,
        ui32 blobCount,
        ui32 blockCount,
        ui32 usedBlockCount,
        bool compacted);
    void RegisterRead(ui32 blockIndex, ui32 blobCount, ui32 blockCount);
    void Clear();

    TRangeStat Get(ui32 blockIndex) const;
    TCompactionCounter GetTop() const;
    TVector<TCompactionCounter> GetTopsFromGroups(size_t groupCount) const;
    TCompactionCounter GetTopByGarbageBlockCount() const;

    TVector<TCompactionCounter> GetTop(size_t count) const;
    TVector<TCompactionCounter> GetTopByGarbageBlockCount(size_t count) const;
    TVector<ui32> GetNonEmptyRanges() const;
    ui32 GetNonEmptyRangeCount() const;
    ui32 GetRangeStart(ui32 blockIndex) const;
    ui32 GetRangeIndex(ui32 blockIndex) const;
    ui32 GetRangeIndex(TBlockRange32 blockRange) const;
    TBlockRange32 GetBlockRange(ui32 rangeIdx) const;
    ui32 GetRangeSize() const;
};

}   // namespace NCloud::NBlockStore::NStorage
