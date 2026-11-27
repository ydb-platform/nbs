#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TPageRangeRef
{
    ui64 FirstPageNo = 0;
    ui64 PageCount = 0;
};

struct TPageRange
{
    ui64 FirstPageNo = 0;
    TVector<TBuffer> Pages;
};

struct TPageRangeView
{
    ui64 FirstPageNo = 0;
    TVector<TArrayRef<char>> Pages;
};

// Resizes |pages| to one buffer of |pageSize| bytes per page of |rangeRefs|
// and returns the views of the buffers, in the order of the refs.
TVector<TPageRangeView> MakePageRangeViews(
    const TVector<TPageRangeRef>& rangeRefs,
    ui32 pageSize,
    TVector<TBuffer>* pages);

TVector<TPageRangeRef> MakePageRangeRefs(
    const TVector<TPageRangeView>& rangeViews);

////////////////////////////////////////////////////////////////////////////////

struct TJournalRecord
{
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;

    TVector<TPageRange> Ranges;
};

struct TJournalTail
{
    ui64 LsnLowWatermark = 0;
    TVector<TJournalRecord> Records;
};

}   // namespace NCloud::NJournalled
