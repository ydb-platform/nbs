#pragma once

#include <util/generic/fwd.h>
#include <util/generic/utility.h>
#include <util/system/yassert.h>

#include <optional>
#include <utility>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// represents [Start, End) interval.
template <typename T>
struct TInterval
{
    T Start{};
    T End{};

    TInterval() = default;

    TInterval(T start, T end)
        : Start(std::move(start))
        , End(std::move(end))
    {
        Y_ABORT_UNLESS(Start < End);
    }

    [[nodiscard]] bool Empty() const
    {
        return End == Start;
    }

    bool Contains(const T& value) const
    {
        return Start <= value && value < End;
    }

    TInterval Intersection(const TInterval& otherInterval) const
    {
        const auto& start = Max(Start, otherInterval.Start);
        const auto& end = Min(End, otherInterval.End);

        if (end > start) {
            return TInterval{start, end};
        }

        return {};
    }

    T Length() const
        requires(std::is_arithmetic_v<T>)
    {
        return End - Start;
    }

    static TInterval WithLength(T start, T length)
        requires(std::is_arithmetic_v<T>)
    {
        return TInterval{start, start + length};
    }

    bool operator==(const TInterval& other) const
    {
        return Start == other.Start && End == other.End;
    }
};

using TSizeInterval = TInterval<ui64>;

}   // namespace NCloud

TString ToString(const NCloud::TSizeInterval& interval);
