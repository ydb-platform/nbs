#pragma once

#include "block_range.h"

#include <util/generic/hash.h>
#include <util/generic/map.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// TBlockRangeMap is a class that manages a collection of block ranges and it
// key (ui64) with efficient overlap checking capabilities. It's designed to
// store and query block ranges and it key, particularly useful for determining
// if a given range overlaps with any of the stored ranges.
class TBlockRangeMap
{
public:
    using TKey = ui64;
    using TOnOverlappedFunc = std::function<void(TKey, TBlockRange64)>;

    struct TKeyAndRange
    {
        TKey Key = {};
        TBlockRange64 Range;
    };

private:
    struct TBlockRange64Comparator
    {
        bool operator()(const TKeyAndRange& lhs, const TKeyAndRange& rhs) const
        {
            return std::tie(lhs.Range.End, lhs.Range.Start) <
                   std::tie(rhs.Range.End, rhs.Range.Start);
        }
    };
    using TRanges = TMultiSet<TKeyAndRange, TBlockRange64Comparator>;
    using TRangeIt = TRanges::iterator;

    ui64 MaxLength = 0;
    TRanges Ranges;
    THashMap<ui64, TRangeIt> RangeByKey;

public:
    // Adds a block range to the collection. Returns false if the key already
    // exists in the collection.
    bool AddRange(TKey key, TBlockRange64 range);

    // Removes a specific block range from the collection. Returns false if
    // the range was not found in the collection.
    bool RemoveRange(TKey key);

    // Checks that the other range overlaps with any range in Ranges.
    [[nodiscard]] std::optional<TKeyAndRange> Overlaps(
        TBlockRange64 other) const;
    // Enumerate all overlapped ranges.
    void AllOverlaps(TBlockRange64 other, TOnOverlappedFunc onOverlap) const;

    [[nodiscard]] bool Empty() const;
    [[nodiscard]] size_t Size() const;

    // Returns a string representation of all ranges in the collection for
    // debugging purposes.
    [[nodiscard]] TString DebugPrint() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
