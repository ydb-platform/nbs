#pragma once

#include <util/string/builder.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <bit>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// The identifier has a size of 64 bits and mixes two sources. The lower 32
// bits the request id. The upper 32 bits are the TVolume generation.
class TCompositeId
{
private:
    struct TValues
    {
        ui32 RequestId = 0;
        ui32 Generation = 0;
    };
    static_assert(sizeof(TValues) == sizeof(ui64));

    TValues Values = {};

public:
    static TCompositeId FromGeneration(ui32 generation)
    {
        return TCompositeId{TValues{.Generation = generation}};
    }

    static TCompositeId FromRaw(ui64 raw)
    {
        return TCompositeId{std::bit_cast<TValues>(raw)};
    }

    [[nodiscard]] bool CanAdvance() const
    {
        return Values.RequestId <
               std::numeric_limits<decltype(Values.RequestId)>::max();
    }

    ui64 Advance()
    {
        Y_DEBUG_ABORT_UNLESS(CanAdvance());
        ++Values.RequestId;
        return GetValue();
    }

    [[nodiscard]] ui32 GetRequestId() const
    {
        return Values.RequestId;
    }

    [[nodiscard]] ui32 GetGeneration() const
    {
        return Values.Generation;
    }

    [[nodiscard]] ui64 GetValue() const
    {
        return std::bit_cast<ui64>(Values);
    }

    [[nodiscard]] TString Print() const
    {
        return TStringBuilder() << GetGeneration() << ":" << GetRequestId();
    }

private:
    explicit TCompositeId(TValues values)
        : Values(values)
    {}
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NCloud::NBlockStore::NStorage
