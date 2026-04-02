#pragma once

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStateStats
{
    virtual ~IWriteBackCacheStateStats() = default;

    virtual void FlushStarted() = 0;
    virtual void FlushCompleted() = 0;
    virtual void FlushFailed() = 0;
    virtual void WriteDataRequestDropped() = 0;
};

using IWriteBackCacheStateStatsPtr = std::shared_ptr<IWriteBackCacheStateStats>;

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
