#pragma once

#include "block_range.h"

#include <util/generic/map.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// TBlockRangeList is a class that manages a collection of block ranges
// (TBlockRange64) with efficient overlap checking capabilities. It's designed
// to store and query block ranges, particularly useful for determining if a
// given range overlaps with any of the stored ranges.
class TBlockRangeList
{
private:
    struct TBlockRange64Comparator
    {
        bool operator()(
            const TBlockRange64& lhs,
            const TBlockRange64& rhs) const
        {
            return std::tie(lhs.End, lhs.Start) < std::tie(rhs.End, rhs.Start);
        }
    };

    ui64 MaxLength = 0;
    TMultiSet<TBlockRange64, TBlockRange64Comparator> Ranges;

public:
    // Adds a block range to the collection.
    void AddRange(TBlockRange64 range);

    // Removes a specific block range from the collection. Returns false if
    // the range was not found in the collection.
    bool RemoveRange(TBlockRange64 range);

    // Checks that the other range overlaps with any range in Ranges.
    [[nodiscard]] bool Overlaps(TBlockRange64 other) const;

    [[nodiscard]] bool Empty() const;

    // Returns a string representation of all ranges in the collection for
    // debugging purposes.
    [[nodiscard]] TString DebugPrint() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
