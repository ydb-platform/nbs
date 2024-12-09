#pragma once

#include <util/system/types.h>


namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ECompactionType: ui32
{
    Forced,  // compaction initiated externally
    Tablet,  // compaction initiated by tablet
};

}   // namespace NCloud::NBlockStore::NStorage
