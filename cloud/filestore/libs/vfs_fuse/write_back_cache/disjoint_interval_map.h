#pragma once

#include <util/generic/map.h>
#include <util/generic/yexception.h>

#include <functional>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TDisjointIntervalMap
{
public:
    struct TItem
    {
        const TKey Begin;
        const TKey End;
        const TValue Value;
    };

    using TData = TMap<TKey, TItem>;
    using TConstIterator = typename TData::const_iterator;
    using TVisitor = std::function<void(TConstIterator it)>;

private:
    TData Data;

public:
    // Add a new interval [begin, end) -> value into the map
    // An exception is thrown if it intersects with any of the existing
    // intervals
    void Add(TKey begin, TKey end, TValue value)
    {
        Y_ENSURE(
            begin < end,
            "Input argument [" << begin << ", " << end
                               << ") is invalid interval");

        // Find first TItem with .End > begin
        auto it = Data.upper_bound(begin);

        Y_ENSURE(
            it == Data.end() || it->second.Begin >= end,
            "Adding interval ["
                << begin << ", " << end
                << ") failed because it overlaps with the existing interval ["
                << it->second.Begin << ", " << it->second.End << ")");

        Data.emplace_hint(it, end, TItem{
            .Begin = begin,
            .End = end,
            .Value = std::move(value)});
    }

    void Remove(TConstIterator iterator)
    {
        Data.erase(iterator);
    }

    // Visit each interval that intersects with [begin, end)
    // Note: it is allowed to remove the current element from the visitor
    void VisitOverlapping(TKey begin, TKey end, const TVisitor& visitor) const
    {
        // Find first TItem with .End > begin
        auto it = Data.upper_bound(begin);

        while (it != Data.end() && it->second.Begin < end) {
            auto next = std::next(it);
            visitor(it);
            it = next;
        }
    }

    TConstIterator begin() const
    {
        return Data.begin();
    }

    TConstIterator end() const
    {
        return Data.end();
    }
};

}   // namespace NCloud::NFileStore::NFuse
