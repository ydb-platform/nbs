#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/system/error.h>
#include <util/network/address.h>

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

inline uint32_t GetScopeId(const sockaddr* address)
{
    if (address) {
        if (address->sa_family == AF_INET6) {
            return reinterpret_cast<const sockaddr_in6*>(address)
                ->sin6_scope_id;
        }
        if (address->sa_family == AF_INET) {
            return 0;
        }
    }
    return ~0;
}

}   // namespace NCloud::NBlockStore::NRdma
