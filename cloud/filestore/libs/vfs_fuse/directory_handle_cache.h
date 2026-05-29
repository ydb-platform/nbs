#pragma once

#include "directory_handle_storage.h"
#include "fs.h"

#include <util/system/spinlock.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandleCache
{
private:
    TLog Log;

    TAdaptiveLock Lock;
    TDirectoryHandleMap Handles;
    TDirectoryHandleStoragePtr Storage;

    TDirectoryHandleStatsPtr Stats;

private:
    void IncreaseStatsForHandle(
        const std::shared_ptr<TDirectoryHandle>& handle);
    void IncreaseStatsForHandles(const TDirectoryHandleMap& handles);
    void DecreaseStatsForHandle(
        const std::shared_ptr<TDirectoryHandle>& handle);
    void DecreaseStatsForHandles(const TDirectoryHandleMap& handles);

public:
    TDirectoryHandleCache(
        TLog log,
        TDirectoryHandleStatsPtr stats,
        TDirectoryHandleStoragePtr storage);

    ui64 CreateHandle(fuse_ino_t ino);

    std::shared_ptr<TDirectoryHandle> FindHandle(ui64 handleId);

    void RemoveHandle(ui64 handleId);
    bool RemoveHandle(ui64 handleId, fuse_ino_t ino);
    void ResetHandle(
        ui64 handleId,
        const std::shared_ptr<TDirectoryHandle>& handle);
    void AppendChunk(
        ui64 handleId,
        const std::shared_ptr<TDirectoryHandle>& handle,
        const TDirectoryHandleChunk& handleChunk);

    void Clear();
    void Reset();
};

}   // namespace NCloud::NFileStore::NFuse
