#pragma once

#include <util/system/types.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStateListener
{
    virtual ~IWriteBackCacheStateListener() = default;

    virtual void ShouldFlushNode(ui64 nodeId) = 0;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
