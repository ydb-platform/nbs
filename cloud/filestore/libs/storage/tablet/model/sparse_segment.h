#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/alloc.h>

#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace NDetails {
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

} // namespace NDetails

////////////////////////////////////////////////////////////////////////////////

class TSparseSegment
    : public TSet<NDetails::TRange, NDetails::TRangeLess, TStlAllocator>
{
public:
    TSparseSegment(IAllocator* alloc, ui64 start, ui64 end);

    void PunchHole(ui64 start, ui64 end);
};

}   // namespace NCloud::NFileStore::NStorage
