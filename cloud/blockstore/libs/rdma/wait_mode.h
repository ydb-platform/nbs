#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

enum class EWaitMode
{
    Poll,
    BusyWait,
    AdaptiveWait,
};

}   // namespace NCloud::NBlockStore::NRdma
