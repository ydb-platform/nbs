#pragma once

#include "public.h"

#include <library/cpp/digest/crc32c/crc32c.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TBlockChecksum final
{
private:
    ui32 Value = 0;

public:
    TBlockChecksum() = default;

    explicit TBlockChecksum(ui32 value)
        : Value(value)
    {}

    ~TBlockChecksum() = default;

    ui32 Extend(const void* data, size_t len)
    {
        Value = Crc32cExtend(Value, data, len);
        return Value;
    }

    ui32 Combine(ui32 value, size_t len)
    {
        Value = Crc32cCombine(Value, value, len);
        return Value;
    }

    ui32 GetValue() const
    {
        return Value;
    }
};

}   // namespace NCloud::NBlockStore
