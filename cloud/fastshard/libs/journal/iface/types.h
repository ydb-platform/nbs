#pragma once

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TPageRangeRef
{
    ui64 FirstPageNo = 0;
    ui64 PageCount = 0;
};

struct TPageRangeView
{
    ui64 FirstPageNo = 0;
    TVector<TStringBuf> Pages;
};

struct TPageRange
{
    ui64 FirstPageNo = 0;
    TVector<TBuffer> Pages;
};

////////////////////////////////////////////////////////////////////////////////

struct TReadPagesResult
{
    ui64 LastAckedLsn = 0;

    TVector<TBuffer> Pages;
};

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

////////////////////////////////////////////////////////////////////////////////

// The views point to the pages of the argument. The argument must live as
// long as the views are used.

TVector<TPageRangeView> MakePageRangeViews(const TVector<TPageRange>& ranges);

}   // namespace NCloud::NJournalled
