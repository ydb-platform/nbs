#pragma once

#include <util/generic/size_literals.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct TRateLimit
{
    ui32 RateLimit = 0;
    ui32 MaxBurstSz = 0;
    ui16 TypicalPktSz = 0;
};

}   // namespace NCloud::NStorage::NRdma
