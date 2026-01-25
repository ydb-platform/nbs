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
    size_t Index = 0;

    bool operator<(const TActiveInterval& other) const
    {
        return Index < other.Index;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBuilder
{
private:
    TVector<TIntervalPart> Parts;
    TVector<TActiveInterval> Heap;

public:
    explicit TBuilder(size_t capacity)
        : Parts(Reserve(capacity * 2))
        , Heap(Reserve(capacity))
    {}

    void AddInterval(ui64 begin, ui64 end, size_t index)
    {
        ProcessHeap(begin);

        Heap.push_back({end, index});
        std::push_heap(Heap.begin(), Heap.end());

        if (Parts.empty() || Parts.back().End <= begin) {
            Parts.push_back({begin, end, index});
        } else if (Parts.back().Index <= index) {
            if (Parts.back().Begin < begin) {
                Parts.back().End = begin;
                Parts.push_back({begin, end, index});
            } else {
                Parts.back() = {begin, end, index};
            }
        }
    }

    TVector<TIntervalPart> Build()
    {
        ProcessHeap(Max<ui64>());
        return std::move(Parts);
    }

private:
    void ProcessHeap(ui64 offset)
    {
        while (!Heap.empty() && Heap.front().End <= offset) {
            Y_ABORT_UNLESS(!Parts.empty());

            std::pop_heap(Heap.begin(), Heap.end());
            Heap.pop_back();

            if (!Heap.empty()) {
                const auto top = Heap.front();
                if (top.End > Parts.back().End) {
                    Parts.push_back({Parts.back().End, top.End, top.Index});
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

    TBuilder builder(intervals.size());

    for (const auto& interval: input) {
        builder.AddInterval(interval.Begin, interval.End, interval.Index);
    }

    return builder.Build();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
