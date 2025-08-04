#pragma once

#include <util/generic/vector.h>
#include <util/generic/ylimits.h>

#include <util/system/types.h>

namespace NCloud::NFileStore::NFuse {

class TOverlappingIntervalSet
{
private:
    struct TInterval
    {
        ui64 Begin = 0;
        ui64 End = 0;
    };

    // Naive implementation.
    // Can be optimized from O(N) to O(log N) if needed
    // For example:
    // https://en.wikipedia.org/wiki/Interval_tree
    TVector<TInterval> Intervals;

public:
    void AddInterval(ui64 begin, ui64 end);
    void RemoveInterval(ui64 begin, ui64 end);
    bool HasIntersection(ui64 begin, ui64 end) const;
    bool Empty() const;
};

}   // namespace NCloud::NFileStore::NFuse
