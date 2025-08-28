#include "sparse_segment.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator<<(IOutputStream& out, const TSparseSegment::TRange& rhs)
{
    out
        << (TStringBuilder()
            << "{Start: " << rhs.Start << " End: " << rhs.End << "}");
    return out;
}

////////////////////////////////////////////////////////////////////////////////

TSparseSegment::TSparseSegment(IAllocator* alloc, ui64 start, ui64 end)
    : Ranges(alloc)
{
    Ranges.emplace(TRange{start, end});
}

void TSparseSegment::PunchHole(ui64 start, ui64 end)
{
    auto lo = Ranges.upper_bound(start);
    auto hi = Ranges.upper_bound(end);
    TRange newLo;
    if (lo != Ranges.end() && lo->Start < start) {
        newLo = {lo->Start, start};
    }

    if (hi != Ranges.end() && hi->Start < end) {
        const_cast<TRange&>(*hi).Start = end;
    }

    while (lo != hi) {
        lo = Ranges.erase(lo);
    }

    if (newLo.Start < newLo.End) {
        Ranges.emplace(newLo);
    }
}

}   // namespace NCloud::NFileStore::NStorage
