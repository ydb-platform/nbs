#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/tablet/model/alloc.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TCompactionCounter
{
    ui32 RangeId = 0;
    ui32 Score = 0;
};

struct TCompactionStats
{
    ui32 BlobsCount = 0;
    ui32 DeletionsCount = 0;
    ui32 GarbageBlocksCount = 0;
};

struct TCompactionRangeInfo
{
    ui32 RangeId = 0;
    TCompactionStats Stats;

    TCompactionRangeInfo() = default;
    TCompactionRangeInfo(ui32 rangeId, TCompactionStats stats)
        : RangeId(rangeId)
        , Stats(stats)
    {}
};

struct TCompactionMapStats
{
    ui64 UsedRangesCount = 0;
    ui64 AllocatedRangesCount = 0;
    ui64 TotalBlobsCount = 0;
    ui64 TotalDeletionsCount = 0;
    ui64 TotalGarbageBlocksCount = 0;

    TVector<TCompactionRangeInfo> TopRangesByCompactionScore;
    TVector<TCompactionRangeInfo> TopRangesByCleanupScore;
    TVector<TCompactionRangeInfo> TopRangesByGarbageScore;
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionMap
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    static constexpr ui32 GroupSize = 256;

public:
    TCompactionMap(IAllocator* alloc);
    ~TCompactionMap();

    void Update(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount,
        ui32 garbageBlocksCount,
        bool compacted);
    void Update(const TVector<TCompactionRangeInfo>& ranges);

    TCompactionStats Get(ui32 rangeId) const;

    TCompactionCounter GetTopCompactionScore() const;
    TCompactionCounter GetTopCleanupScore() const;
    TCompactionCounter GetTopGarbageScore() const;

    TVector<TCompactionRangeInfo> GetTopRangesByCompactionScore(
        ui32 topSize) const;
    TVector<TCompactionRangeInfo> GetTopRangesByCleanupScore(
        ui32 topSize) const;
    TVector<TCompactionRangeInfo> GetTopRangesByGarbageScore(
        ui32 topSize) const;

    TVector<ui32> GetNonEmptyCompactionRanges() const;
    TVector<ui32> GetAllCompactionRanges() const;

    TCompactionMapStats GetStats(ui32 topSize) const;
};

}   // namespace NCloud::NFileStore::NStorage
