#include "sparse_segment.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TSparseSegment::TSparseSegment(IAllocator* alloc, ui64 start, ui64 end)
    : TSet<NDetails::TRange, NDetails::TRangeLess, TStlAllocator>(alloc)
{
    emplace(start, end);
}

void TSparseSegment::PunchHole(ui64 start, ui64 end)
{
    auto lo = upper_bound(start);
    auto hi = upper_bound(end);
    NDetails::TRange newLo;
    if (lo != this->end() && lo->Start < start) {
        newLo = {lo->Start, start};
    }

    if (hi != this->end() && hi->Start < end) {
        const_cast<NDetails::TRange&>(*hi).Start = end;
    }

    while (lo != hi) {
        lo = erase(lo);
    }

    if (newLo.Start < newLo.End) {
        emplace(newLo);
    }
}

}   // namespace NCloud::NFileStore::NStorage
