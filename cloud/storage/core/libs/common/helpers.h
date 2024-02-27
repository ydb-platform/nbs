#pragma once

#include "public.h"

#include <util/system/defaults.h>

#include <cloud/storage/core/protos/error.pb.h>

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

template <typename T>
void SetErrorProtoFlag(NProto::TError& error, const T flag)
{
    auto flags = error.GetFlags();
    SetProtoFlag(flags, flag);
    error.SetFlags(flags);
}
}

}   // namespace NCloud
