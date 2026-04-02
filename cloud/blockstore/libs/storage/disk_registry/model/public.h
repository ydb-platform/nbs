#pragma once

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>

namespace NCloud::NBlockStore {

constexpr TStringBuf BackFromUnavailableStateMessage = "back from unavailable";
constexpr TStringBuf AutomaticallyRestoredStateMessage =
    "automatically restored";

}   // namespace NCloud::NBlockStore
