#pragma once

#include "time_range.h"

namespace NCloud::NFileStore::NProfileTool {

// Drop paths with unavailable file sizes when ignoreErrors is set.
TMaybe<ui64> EstimateDumpSize(
    TVector<TProfileLogFile>& profileLogFiles,
    bool ignoreErrors);

constexpr bool HasSpaceForDump(ui64 availableBytes, ui64 estimatedBytes) noexcept
{
    constexpr ui64 MinRemainingBytes = 1ULL << 30;
    return availableBytes >= MinRemainingBytes &&
           estimatedBytes <= availableBytes - MinRemainingBytes;
}

}   // namespace NCloud::NFileStore::NProfileTool
