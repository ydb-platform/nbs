#pragma once

#include <util/generic/ylimits.h>
#include <util/system/types.h>

#include <utility>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 InvalidCommitId = Max<ui64>();

constexpr ui64 MakeCommitId(ui32 gen, ui32 step)
{
    return (static_cast<ui64>(gen) << 32ull) | static_cast<ui64>(step);
}

constexpr std::pair<ui32, ui32> ParseCommitId(ui64 commitId)
{
    ui32 gen = static_cast<ui32>(commitId >> 32ull);
    ui32 step = static_cast<ui32>(commitId);
    return {gen, step};
}

constexpr ui64 ReverseCommitId(ui64 commitId)
{
    return InvalidCommitId - commitId;
}

constexpr bool
VisibleCommitId(ui64 commitId, ui64 minCommitId, ui64 maxCommitId)
{
    return minCommitId <= commitId && commitId < maxCommitId;
}

}   // namespace NCloud
