#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

template <typename TOut, typename TIn>
TOut SafeCast(TIn value)
{
    Y_VERIFY(Min<TOut>() <= value && value <= Max<TOut>());
    return static_cast<TOut>(value);
}

}   // namespace NCloud::NBlockStore::NRdma
