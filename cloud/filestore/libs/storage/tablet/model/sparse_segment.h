#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/alloc.h>

#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSparseSegment
{
private:
    struct TRange
    {
        ui64 Start = 0;
        ui64 End = 0;
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

    TSet<TRange, TRangeLess, TStlAllocator> Ranges;

public:
     using iterator = typename TSet<TRange, TRangeLess, TStlAllocator>::iterator;

    TSparseSegment(IAllocator* alloc, ui64 start, ui64 end);

public:
    void PunchHole(ui64 start, ui64 end);
    bool Empty() const
    {
        return Ranges.empty();
    }

    iterator begin()
    {
        return Ranges.begin();
    }

    iterator end()
    {
        return Ranges.end();
    }
};

}   // namespace NCloud::NFileStore::NStorage
