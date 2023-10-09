#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/utility.h>
#include <util/system/error.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

template <typename TOut, typename TIn>
TOut SafeCast(TIn value)
{
    Y_ABORT_UNLESS(Min<TOut>() <= value && value <= Max<TOut>());
    return static_cast<TOut>(value);
}

template <typename T, typename A>
bool OwnedBy(T* ptr, const TVector<T, A>& vec)
{
    return ptr >= &vec.front() && &vec.back() >= ptr;
}

template <typename T, typename A>
bool OwnedBy(uintptr_t addr, const TVector<T, A>& vec)
{
    return OwnedBy(reinterpret_cast<T*>(addr), vec);
}

inline TString SafeLastSystemErrorText() {
    int err = LastSystemError();
    char buf[64];
    strerror_r(err, buf, sizeof(buf));
    return TString(buf);
}

}   // namespace NCloud::NBlockStore::NRdma
