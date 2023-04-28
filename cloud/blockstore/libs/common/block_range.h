#pragma once

#include "public.h"

#include <util/generic/cast.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockIndex>
struct TBlockRange
{
    TBlockIndex Start = 0;
    TBlockIndex End = 0;

    static constexpr TBlockIndex MaxIndex = ::Max<TBlockIndex>();

    TBlockRange() = default;

    explicit TBlockRange(TBlockIndex blockIndex)
        : Start(blockIndex)
        , End(blockIndex)
    {}

    TBlockRange(TBlockIndex start, TBlockIndex end)
        : Start(start)
        , End(end)
    {}

    TBlockIndex Size() const
    {
        Y_VERIFY_DEBUG(Start <= End);
        return (End - Start) + 1;
    }

    bool Contains(TBlockIndex blockIndex) const
    {
        return Start <= blockIndex && blockIndex <= End;
    }

    bool Contains(const TBlockRange& other) const
    {
        return Start <= other.Start && other.End <= End;
    }

    bool Overlaps(const TBlockRange& other) const
    {
        return Start <= other.End && other.Start <= End;
    }

    TBlockRange Intersect(const TBlockRange& other) const
    {
        auto start = ::Max(Start, other.Start);
        auto end = ::Min(End, other.End);
        return { start, end };
    }

    TBlockRange Union(const TBlockRange& other) const
    {
        auto start = ::Min(Start, other.Start);
        auto end = ::Max(End, other.End);
        Y_VERIFY_DEBUG(Size() + other.Size() >= end - start + 1);
        return { start, end };
    }

    static TBlockRange Max()
    {
        return { 0, MaxIndex };
    }

    static TBlockRange WithLength(TBlockIndex start, TBlockIndex count)
    {
        Y_VERIFY_DEBUG(count);
        if (start < MaxIndex - (count - 1)) {
            return { start, start + (count - 1) };
        } else {
            return { start, MaxIndex };
        }
    }

    static bool TryParse(TStringBuf s, TBlockRange& range);

    friend bool operator==(const TBlockRange& lhs, const TBlockRange& rhs)
    {
        return lhs.Start == rhs.Start && lhs.End == rhs.End;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockIndex>
TString DescribeRange(const TBlockRange<TBlockIndex>& blockRange);
template <typename TBlockIndex>
TString DescribeRange(const TVector<TBlockIndex>& blocks);

template <typename TBlockIndex>
constexpr auto xrange(const TBlockRange<TBlockIndex>& range)
{
    Y_VERIFY(range.End < Max<TBlockIndex>());
    return ::xrange(range.Start, range.End + 1);
}

template <typename TBlockIndex, typename TBlockIndex2>
constexpr auto xrange(const TBlockRange<TBlockIndex>& range, TBlockIndex2 step)
{
    Y_VERIFY(range.End < Max<TBlockIndex>());
    return ::xrange(range.Start, range.End + 1, step);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockIndex>
class TBlockRangeBuilder
{
private:
    TVector<TBlockRange<TBlockIndex>>& Ranges;

public:
    TBlockRangeBuilder(TVector<TBlockRange<TBlockIndex>>& ranges)
        : Ranges(ranges)
    {
    }

public:
    void OnBlock(TBlockIndex blockIndex)
    {
        if (Ranges.empty() || Ranges.back().End + 1 != blockIndex) {
            Ranges.emplace_back(blockIndex);
        } else {
            ++Ranges.back().End;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

using TBlockRange32 = TBlockRange<ui32>;
using TBlockRange64 = TBlockRange<ui64>;
using TBlockRange32Builder = TBlockRangeBuilder<ui32>;
using TBlockRange64Builder = TBlockRangeBuilder<ui64>;

inline TBlockRange32 ConvertRangeSafe(const TBlockRange64& range)
{
    return {
        IntegerCast<ui32>(range.Start),
        IntegerCast<ui32>(range.End),
    };
}

inline TBlockRange64 ConvertRangeSafe(const TBlockRange32& range)
{
    return {range.Start, range.End};
}

}   // namespace NCloud::NBlockStore
