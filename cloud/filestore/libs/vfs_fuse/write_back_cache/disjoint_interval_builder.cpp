#include "disjoint_interval_builder.h"

#include <util/generic/algorithm.h>
#include <util/generic/ylimits.h>
#include <util/system/yassert.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TActiveInterval
{
    ui64 End = 0;
    // Item index in the input vector
    size_t Index = 0;

    bool operator<(const TActiveInterval& other) const
    {
        return Index < other.Index;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDisjointIntervalBuilder
{
private:
    // This field is used as a result of the algorithm
    TVector<TIntervalPart> DisjointIntervalParts;

    // Intervals containing the most recently used offset.
    // Represented as a max-heap (intervals with the larger Index come first).
    TVector<TActiveInterval> ActiveIntervals;

public:
    explicit TDisjointIntervalBuilder(size_t capacity)
        : DisjointIntervalParts(Reserve(capacity * 2))
        , ActiveIntervals(Reserve(capacity))
    {}

    void AddInterval(ui64 begin, ui64 end, size_t index)
    {
        ProcessActiveIntervalsUpToOffset(begin);

        ActiveIntervals.push_back({end, index});
        std::push_heap(ActiveIntervals.begin(), ActiveIntervals.end());

        if (DisjointIntervalParts.empty() ||
            DisjointIntervalParts.back().End <= begin)
        {
            // There are no active intervals - just start a new interval
            DisjointIntervalParts.push_back({begin, end, index});
        } else if (DisjointIntervalParts.back().Index <= index) {
            // The newly added interval overlaps with the current top interval
            // and has higher Index - we need to cut the current interval
            if (DisjointIntervalParts.back().Begin < begin) {
                DisjointIntervalParts.back().End = begin;
                DisjointIntervalParts.push_back({begin, end, index});
            } else {
                DisjointIntervalParts.back() = {begin, end, index};
            }
        }
    }

    TVector<TIntervalPart> Build()
    {
        ProcessActiveIntervalsUpToOffset(Max<ui64>());
        return std::move(DisjointIntervalParts);
    }

private:
    // Processed active intervals and builds disjoint intervals considering that
    // there will be no interval with starting point before the specified offset
    void ProcessActiveIntervalsUpToOffset(ui64 offset)
    {
        while (!ActiveIntervals.empty() &&
               ActiveIntervals.front().End <= offset)
        {
            Y_ABORT_UNLESS(!DisjointIntervalParts.empty());

            std::pop_heap(ActiveIntervals.begin(), ActiveIntervals.end());
            ActiveIntervals.pop_back();

            if (!ActiveIntervals.empty()) {
                const auto top = ActiveIntervals.front();
                if (top.End > DisjointIntervalParts.back().End) {
                    DisjointIntervalParts.push_back(
                        {DisjointIntervalParts.back().End, top.End, top.Index});
                }
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<TIntervalPart> BuildDisjointIntervalParts(
    const TVector<TInterval>& intervals)
{
    if (intervals.empty()) {
        return {};
    }

    TVector<TIntervalPart> input(Reserve(intervals.size()));

    for (size_t i = 0; i < intervals.size(); i++) {
        input.push_back({intervals[i].Begin, intervals[i].End, i});
    }

    Sort(
        input.begin(),
        input.end(),
        [](const TIntervalPart& lhs, const TIntervalPart& rhs)
        { return lhs.Begin < rhs.Begin; });

    TDisjointIntervalBuilder builder(intervals.size());

    for (const auto& interval: input) {
        builder.AddInterval(interval.Begin, interval.End, interval.Index);
    }

    return builder.Build();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
