#pragma once

#include <cloud/storage/core/libs/actors/public.h>

#include <util/system/types.h>

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TThrottlingRequestInfo final
{
    ui64 ByteCount = 0;
    ui32 OpType = 0;
    ui32 PolicyVersion = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ITabletThrottler;
using ITabletThrottlerPtr = std::unique_ptr<ITabletThrottler>;

}   // namespace NCloud
