#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/system/error.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

template <typename TOut, typename TIn>
TOut SafeCast(TIn value)
{
    Y_ABORT_UNLESS(Min<TOut>() <= value && value <= Max<TOut>());
    return static_cast<TOut>(value);
}

inline TString SafeLastSystemErrorText()
{
    int err = LastSystemError();
    char buf[64]{};
    const char* result = strerror_r(err, buf, sizeof(buf));
    return TString(result);
}

}   // namespace NCloud::NBlockStore::NRdma
