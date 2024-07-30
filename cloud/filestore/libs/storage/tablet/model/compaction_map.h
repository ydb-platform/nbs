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
};

struct TCompactionRangeInfo
{
    ui32 RangeId = 0;
    TCompactionStats Stats;
};

struct TCompactionMapStats
{
    ui64 UsedRangesCount = 0;
    ui64 AllocatedRangesCount = 0;
    ui64 TotalBlobsCount = 0;
    ui64 TotalDeletionsCount = 0;

    TVector<TCompactionRangeInfo> TopRangesByCleanupScore;
    TVector<TCompactionRangeInfo> TopRangesByCompactionScore;
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

    void Update(ui32 rangeId, ui32 blobsCount, ui32 deletionsCount);
    void Update(const TVector<TCompactionRangeInfo>& ranges);

    TCompactionStats Get(ui32 rangeId) const;

    TCompactionCounter GetTopCompactionScore() const;
    TCompactionCounter GetTopCleanupScore() const;

    TVector<TCompactionRangeInfo> GetTopRangesByCompactionScore(ui32 topSize) const;
    TVector<TCompactionRangeInfo> GetTopRangesByCleanupScore(ui32 topSize) const;

    TVector<ui32> GetNonEmptyCompactionRanges() const;
    TVector<ui32> GetAllCompactionRanges() const;
    TVector<ui32> GetEmptyCompactionRanges() const;

    TCompactionMapStats GetStats(ui32 topSize) const;
};

}   // namespace NCloud::NFileStore::NStorage
