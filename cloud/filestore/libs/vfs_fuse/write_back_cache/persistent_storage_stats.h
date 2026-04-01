#pragma once

#include <util/system/types.h>

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStorageStats
{
    ui64 RawCapacityByteCount = 0;
    ui64 RawUsedByteCount = 0;
    ui64 EntryCount = 0;
    bool IsCorrupted = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistentStorageStats
{
    virtual ~IPersistentStorageStats() = default;

    virtual void UpdatePersistentStorageStats(
        const TPersistentStorageStats& stats) = 0;
};

using IPersistentStorageStatsPtr = std::shared_ptr<IPersistentStorageStats>;

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
