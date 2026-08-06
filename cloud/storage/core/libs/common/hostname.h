#pragma once

#include <util/generic/string.h>
#include <util/generic/yexception.h>

#include <functional>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TString GetFqdnHostNameWithRetries(
    const std::function<void(const yexception&)>& onFail);

}   // namespace NCloud
