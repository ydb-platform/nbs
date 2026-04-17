#pragma once

#include "directory_handles_storage.h"
#include "fs.h"

#include <memory>

#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandlesCache
{
private:
    TLog Log;

    TAdaptiveLock Lock;
    TDirectoryHandleMap Handles;
    TDirectoryHandlesStoragePtr Storage;

    TDirectoryHandlesStatsPtr Stats;

public:
    TDirectoryHandlesCache(
        TLog log,
        TDirectoryHandlesStatsPtr stats,
        TDirectoryHandlesStoragePtr storage);

    ui64 CreateHandle(fuse_ino_t ino);

    std::shared_ptr<TDirectoryHandle> FindHandle(ui64 handleId);

    void RemoveHandle(ui64 handleId);
    bool RemoveHandle(ui64 handleId, fuse_ino_t ino);
    void ResetHandle(
        ui64 handleId,
        const std::shared_ptr<TDirectoryHandle>& handle);
    void AppendChunk(ui64 handleId, const TDirectoryHandleChunk& handleChunk);

    void Clear();
    void Reset();
};

}   // namespace NCloud::NFileStore::NFuse
