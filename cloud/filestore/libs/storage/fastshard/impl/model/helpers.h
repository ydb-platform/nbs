#pragma once

#include <util/system/defaults.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

inline ui64 RoundUp(ui64 n, ui64 by)
{
    return ((n - 1) / by + 1) * by;
}

inline ui64 RoundDown(ui64 n, ui64 by)
{
    return (n / by) * by;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
