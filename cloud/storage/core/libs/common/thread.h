#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

void SetCurrentThreadName(const TString& name, ui32 maxCharsFromProcessName = 4);

}   // namespace NCloud
