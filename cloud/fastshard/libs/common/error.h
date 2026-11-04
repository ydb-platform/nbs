#pragma once

#include <util/generic/string.h>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

// Fiber safe strerror version.
TString SystemErrorText(int code);

}   // namespace NCloud::NFastShard
