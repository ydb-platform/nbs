#pragma once

#include "block_range.h"

#include <util/generic/strbuf.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <span>
#include <utility>
#include <variant>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using TPrintableValue = std::variant<
    std::monostate,
    TString,
    int,
    ui16,
    ui32,
    ui64,
    TBlockRange64,
    TStringBuf,
    const char*>;

using TPrintableParams =
    std::span<const std::pair<TStringBuf, TPrintableValue>>;

TString PrintParams(TPrintableParams keyValues);

}   // namespace NCloud::NBlockStore
