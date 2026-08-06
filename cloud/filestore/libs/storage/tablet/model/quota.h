#pragma once

#include <util/system/types.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TQuotaUsage
{
    ui32 QuotaId = 0;
    ui64 UsedBytes = 0;
    ui64 UsedNodes = 0;
};

}   // namespace NCloud::NFileStore::NStorage
