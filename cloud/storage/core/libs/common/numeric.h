#pragma once

#include "public.h"

#include <util/system/yassert.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

inline ui64 PercentOf(ui64 value, ui64 percent)
{
    Y_ABORT_UNLESS(percent <= 100, "Invalid percentage %lu", percent);

    return value / 100 * percent + value % 100 * percent / 100;
}

}   // namespace NCloud
