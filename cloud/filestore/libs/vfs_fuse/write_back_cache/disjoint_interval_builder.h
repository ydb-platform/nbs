#pragma once

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TInterval
{
    ui64 Begin = 0;
    ui64 End = 0;
};

struct TIntervalPart
{
    ui64 Begin = 0;
    ui64 End = 0;
    // Index of the source interval from which this part was taken
    size_t Index = 0;
};

/**
 * Build a sequence of non-overlapping intervals from the sequence of
 * overlapping intervals. The intervals are processed in the order they appear.
 *
 * @param entries The sequence of intervals that may overlap.
 * @return A sorted vector of non-overlapping intervals including indices of
 *   the original intervals.
 *
 * Example: [1, 4), [3, 6), [2, 5) => [1, 3): 0, [2, 5): 2, [4, 6): 1
 */
TVector<TIntervalPart> BuildDisjointIntervalParts(
    const TVector<TInterval>& intervals);

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
