#pragma once

#include "public.h"

#include <util/generic/vector.h>
#include <util/generic/utility.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

template <typename TOut, typename TIn>
TOut SafeCast(TIn value)
{
    Y_VERIFY(Min<TOut>() <= value && value <= Max<TOut>());
    return static_cast<TOut>(value);
}

template <typename T, typename A>
bool OwnedBy(uintptr_t addr, const TVector<T, A>& vec)
{
    auto ptr = reinterpret_cast<T*>(addr);
    return ptr >= &vec.front() && &vec.back() >= ptr;
}

}   // namespace NCloud::NBlockStore::NRdma
