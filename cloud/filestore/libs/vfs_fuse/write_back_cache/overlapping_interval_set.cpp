#include "overlapping_interval_set.h"

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

void TOverlappingIntervalSet::AddInterval(ui64 begin, ui64 end)
{
    Y_ENSURE(
        begin < end,
        "Input argument [" << begin << ", " << end << ") is invalid");

    Intervals.push_back({
        .Begin = begin,
        .End = end
    });
}

void TOverlappingIntervalSet::RemoveInterval(ui64 begin, ui64 end)
{
    Y_ENSURE(
        begin < end,
        "Input argument [" << begin << ", " << end << ") is invalid");

    auto *it = FindIf(Intervals, [=](const TInterval& interval) {
        return interval.Begin == begin && interval.End == end;
    });

    Y_ENSURE(it != Intervals.end(),
        "Interval to remove [" << begin << ", " << end <<
        ") was not found in the set");

    Intervals.erase(it);
}

bool TOverlappingIntervalSet::HasIntersection(ui64 begin, ui64 end) const
{
    Y_ABORT_UNLESS(begin < end);

    return AnyOf(Intervals, [=](const TInterval& interval) {
        // Max(interval.Begin, begin) < Min(interval.End, end)
        return interval.Begin < end && begin < interval.End;
    });
}

bool TOverlappingIntervalSet::Empty() const
{
    return Intervals.empty();
}

}   // namespace NCloud::NFileStore::NFuse
