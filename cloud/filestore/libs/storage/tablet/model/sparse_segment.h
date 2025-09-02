#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/alloc.h>

#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSparseSegment
{
public:
    struct TRange
    {
        ui64 Start = 0;
        ui64 End = 0;

        bool operator==(const TRange& rhs) const = default;
    };

    struct TRangeLess
    {
        using is_transparent = void;

        bool operator()(const auto& lhs, const auto& rhs) const
        {
            return GetEnd(lhs) < GetEnd(rhs);
        }

        static ui64 GetEnd(const TRange& r)
        {
            return r.End;
        }

        static ui64 GetEnd(ui64 end)
        {
            return end;
        }
    };

    using TConstIterator = typename TSet<TRange, TRangeLess, TStlAllocator>::const_iterator;

    TSparseSegment(IAllocator* alloc, ui64 start, ui64 end);

    void PunchHole(ui64 start, ui64 end);
    [[nodiscard]] bool Empty() const
    {
        return Ranges.empty();
    }

    [[nodiscard]] TConstIterator begin() const
    {
        return Ranges.cbegin();
    }

    [[nodiscard]] TConstIterator end() const
    {
        return Ranges.cend();
    }

private:
    TSet<TRange, TRangeLess, TStlAllocator> Ranges;
};

IOutputStream& operator<<(
    IOutputStream& out,
    const TSparseSegment::TRange& rhs);

}   // namespace NCloud::NFileStore::NStorage
