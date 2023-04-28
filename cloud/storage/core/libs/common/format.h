#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TString FormatTimestamp(TInstant ts);
TString FormatDuration(TDuration duration);
TString FormatByteSize(ui64 size);

}   // namespace NCloud
