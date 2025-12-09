#include "block_range_list.h"

#include <util/stream/str.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void TBlockRangeList::AddRange(TBlockRange64 range)
{
    MaxLength = Max(MaxLength, range.Size());
    Ranges.insert(range);
}

bool TBlockRangeList::RemoveRange(TBlockRange64 range)
{
    auto it = Ranges.find(range);
    if (it != Ranges.end()) {
        Ranges.erase(it);
        return true;
    }
    return false;
}

bool TBlockRangeList::Overlaps(TBlockRange64 other) const
{
    // 1. Find the range x which: x.end >= other.start in the sorted list (by
    // end of range + length).
    // 2. Move through the list of ranges. Check overlapping x with other.
    // 3. when x.begin >= other.end + MaxLength stop iterating.

    auto left = TBlockRange64::MakeClosedInterval(0, other.Start);
    const ui64 safeRight = Max<ui64>() - MaxLength > other.End
                               ? other.End + MaxLength
                               : Max<ui64>();
    for (auto it = Ranges.lower_bound(left); it != Ranges.end(); ++it) {
        if (it->Overlaps(other)) {
            return true;
        }
        if (safeRight <= it->Start) {
            break;
        }
    }
    return false;
}

bool TBlockRangeList::Empty() const
{
    return Ranges.empty();
}

TString TBlockRangeList::DebugPrint() const
{
    TStringStream ss;

    for (const auto r: Ranges) {
        ss << r.Print();
    }
    return ss.Str();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
