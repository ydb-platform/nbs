#pragma once

#include <util/generic/function_ref.h>
#include <util/generic/map.h>
#include <util/generic/yexception.h>

namespace NCloud {

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TDisjointIntervalMap
{
public:
    struct TItem
    {
        const TKey Begin;
        const TKey End;
        TValue Value;
    };

    struct TStats
    {
        size_t ContiguousIntervalCount = 0;
        TKey IntervalSum = 0;
    };

    using TData = TMap<TKey, TItem>;
    using TIterator = typename TData::iterator;
    using TReverseIterator = typename TData::reverse_iterator;
    using TVisitor = TFunctionRef<void(TIterator it)>;
    using TConstIterator = typename TData::const_iterator;
    using TConstReverseIterator = typename TData::const_reverse_iterator;
    using TConstVisitor = TFunctionRef<void(TConstIterator it)>;

private:
    TData Data;

protected:
    // Add a new interval [begin, end) -> value into the map
    // An exception is thrown if it intersects with any of the existing
    // intervals
    void Add(TKey begin, TKey end, TValue value, TStats* stats)
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

        it = Data.emplace_hint(
            it,
            end,
            TItem{.Begin = begin, .End = end, .Value = std::move(value)});

        if (stats) {
            UpdateStats(it, *stats, true);
        }
    }

    void Remove(TConstIterator iterator, TStats* stats)
    {
        if (stats) {
            UpdateStats(iterator, *stats, false);
        }
        Data.erase(iterator);
    }

public:
    // Visit each interval that intersects with [begin, end)
    // Note: it is allowed to remove the current element from the visitor
    void VisitOverlapping(TKey begin, TKey end, const TVisitor& visitor)
    {
        // Find first TItem with .End > begin
        auto it = Data.upper_bound(begin);

        while (it != Data.end() && it->second.Begin < end) {
            auto next = std::next(it);
            visitor(it);
            it = next;
        }
    }

    // Visit each interval that intersects with [begin, end)
    // Note: it is allowed to remove the current element from the visitor
    void VisitOverlapping(TKey begin, TKey end, const TConstVisitor& visitor) const
    {
        // Find first TItem with .End > begin
        auto it = Data.upper_bound(begin);

        while (it != Data.end() && it->second.Begin < end) {
            auto next = std::next(it);
            visitor(it);
            it = next;
        }
    }

    bool empty() const
    {
        return Data.empty();
    }

    TIterator begin()
    {
        return Data.begin();
    }

    TIterator end()
    {
        return Data.end();
    }

    TConstIterator begin() const
    {
        return Data.begin();
    }

    TConstIterator end() const
    {
        return Data.end();
    }

    TReverseIterator rbegin()
    {
        return Data.rbegin();
    }

    TReverseIterator rend()
    {
        return Data.rend();
    }

    TConstReverseIterator rbegin() const
    {
        return Data.rbegin();
    }

    TConstReverseIterator rend() const
    {
        return Data.rend();
    }

private:
    void UpdateStats(TConstIterator iterator, TStats& stats, bool isAdd) const
    {
        if (isAdd) {
            stats.IntervalSum += iterator->second.End - iterator->second.Begin;
            stats.ContiguousIntervalCount++;
        } else {
            stats.IntervalSum -= iterator->second.End - iterator->second.Begin;
            stats.ContiguousIntervalCount--;
        }

        if (iterator != Data.begin()) {
            auto prev = std::prev(iterator);
            if (prev->second.End == iterator->second.Begin) {
                if (isAdd) {
                    stats.ContiguousIntervalCount--;
                } else {
                    stats.ContiguousIntervalCount++;
                }
            }
        }

        auto next = std::next(iterator);
        if (next != Data.end() && next->second.Begin == iterator->second.End) {
            if (isAdd) {
                stats.ContiguousIntervalCount--;
            } else {
                stats.ContiguousIntervalCount++;
            }
        }
    }
};

}   // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TDisjointIntervalMap: public NPrivate::TDisjointIntervalMap<TKey, TValue>
{
private:
    using TBase = NPrivate::TDisjointIntervalMap<TKey, TValue>;

public:
    void Add(TKey begin, TKey end, TValue value)
    {
        TBase::Add(begin, end, std::move(value), nullptr);
    }

    void Remove(typename TBase::TConstIterator iterator)
    {
        TBase::Remove(iterator, nullptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TDisjointIntervalMapWithStats
    : public NPrivate::TDisjointIntervalMap<TKey, TValue>
{
private:
    using TBase = NPrivate::TDisjointIntervalMap<TKey, TValue>;

    typename TBase::TStats Stats;

public:
    void Add(TKey begin, TKey end, TValue value)
    {
        TBase::Add(begin, end, std::move(value), &Stats);
    }

    void Remove(typename TBase::TConstIterator iterator)
    {
        TBase::Remove(iterator, &Stats);
    }

    size_t GetContiguousIntervalCount() const
    {
        return Stats.ContiguousIntervalCount;
    }

    TKey GetIntervalSum() const
    {
        return Stats.IntervalSum;
    }
};

}   // namespace NCloud
