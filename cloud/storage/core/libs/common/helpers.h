#pragma once

#include "public.h"

#include <util/system/defaults.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool HasProtoFlag(ui32 flags, const T flag)
{
    auto iflag = static_cast<ui32>(flag);
    return iflag ? flags & (1 << (iflag - 1)) : false;
}

template <typename T>
void SetProtoFlag(ui32& flags, const T flag)
{
    auto iflag = static_cast<ui32>(flag);
    if (iflag) {
        flags |= 1 << (iflag - 1);
    }
}

}   // namespace NCloud
