#include "block_range_map.h"

#include <util/stream/str.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

bool TBlockRangeMap::AddRange(TKey key, TBlockRange64 range)
{
    if (RangeByKey.contains(key)) {
        return false;
    }
    MaxLength = Max(MaxLength, range.Size());
    auto it = Ranges.insert(TKeyAndRange{.Key = key, .Range = range});
    RangeByKey[key] = it;
    return true;
}

bool TBlockRangeMap::RemoveRange(ui64 key)
{
    auto it = RangeByKey.find(key);
    if (it != RangeByKey.end()) {
        Ranges.erase(it->second);
        RangeByKey.erase(it);
        return true;
    }

    return false;
}

std::optional<TBlockRangeMap::TKeyAndRange> TBlockRangeMap::Overlaps(
    TBlockRange64 other) const
{
    // 1. Find the range x which: x.end >= other.start in the sorted list (by
    // end of range + length).
    // 2. Move through the list of ranges. Check overlapping x with other.
    // 3. when x.begin >= other.end + MaxLength stop iterating.

    auto left = TKeyAndRange{
        .Key = 0,
        .Range = TBlockRange64::MakeClosedInterval(0, other.Start)};
    const ui64 safeRight = Max<ui64>() - MaxLength > other.End
                               ? other.End + MaxLength
                               : Max<ui64>();
    for (auto it = Ranges.lower_bound(left); it != Ranges.end(); ++it) {
        if (it->Range.Overlaps(other)) {
            return *it;
        }
        if (safeRight <= it->Range.Start) {
            break;
        }
    }
    return std::nullopt;
}

void TBlockRangeMap::AllOverlaps(
    TBlockRange64 other,
    TOnOverlappedFunc onOverlap) const
{
    auto left = TKeyAndRange{
        .Key = 0,
        .Range = TBlockRange64::MakeClosedInterval(0, other.Start)};
    const ui64 safeRight = Max<ui64>() - MaxLength > other.End
                               ? other.End + MaxLength
                               : Max<ui64>();
    for (auto it = Ranges.lower_bound(left); it != Ranges.end(); ++it) {
        if (it->Range.Overlaps(other)) {
            onOverlap(it->Key, it->Range);
        }
        if (safeRight <= it->Range.Start) {
            break;
        }
    }
}

bool TBlockRangeMap::Empty() const
{
    return Ranges.empty();
}

size_t TBlockRangeMap::Size() const
{
    return Ranges.size();
}

TString TBlockRangeMap::DebugPrint() const
{
    TStringStream ss;

    for (const auto r: Ranges) {
        ss << r.Key << r.Range.Print();
    }
    return ss.Str();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
