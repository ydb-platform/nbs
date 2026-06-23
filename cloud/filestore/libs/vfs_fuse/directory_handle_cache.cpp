#include "directory_handle_cache.h"

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStats SumDirectoryHandlesStats(
    const TDirectoryHandleMap& handles)
{
    TDirectoryHandleStats total;
    for (const auto& [_, handle]: handles) {
        if (handle) {
            const auto s = handle->GetStats();
            total.SerializedSize += s.SerializedSize;
            total.ChunkCount += s.ChunkCount;
        }
    }
    return total;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleCache::TDirectoryHandleCache(
    TLog log,
    TDirectoryHandleModuleStatsPtr stats,
    TDirectoryHandleStoragePtr storage,
    TDirectoryEntryVersionCachePtr directoryEntryVersionCache)
    : Log(std::move(log))
    , Storage(std::move(storage))
    , DirectoryEntryVersionCache(std::move(directoryEntryVersionCache))
    , Stats(std::move(stats))
{
    if (Storage) {
        Storage->LoadHandles(Handles);
        if (DirectoryEntryVersionCache) {
            for (const auto& [_, handle]: Handles) {
                DirectoryEntryVersionCache->RegisterHandle(handle->Index);
            }
        }
    }

    IncreaseStats(SumDirectoryHandlesStats(Handles), Handles.size());
}

ui64 TDirectoryHandleCache::CreateHandle(fuse_ino_t ino)
{
    ui64 handleId = 0;
    auto handle = std::make_shared<TDirectoryHandle>(ino);

    with_lock (Lock) {
        do {
            handleId = RandomNumber<ui64>();
        } while (!Handles.try_emplace(handleId, handle).second);

        if (DirectoryEntryVersionCache) {
            DirectoryEntryVersionCache->RegisterHandle(ino);
        }

        // A fresh handle has exactly one chunk (the default one we're about
        // to store); its serialized size equals BaseSerializedSize. Build
        // the chunk once and reuse it for both the stats delta and the
        // storage call.
        const TDirectoryHandleChunk initialChunk{.Index = ino};
        IncreaseStats({initialChunk.GetSerializedSize(), 1}, 1);

        if (Storage) {
            Storage->StoreHandle(handleId, *handle, initialChunk);
        }
    }

    return handleId;
}

std::shared_ptr<TDirectoryHandle> TDirectoryHandleCache::FindHandle(
    ui64 handleId)
{
    with_lock (Lock) {
        auto it = Handles.find(handleId);
        return it != Handles.end() ? it->second : nullptr;
    }
}

void TDirectoryHandleCache::RemoveHandle(ui64 handleId)
{
    with_lock (Lock) {
        auto it = Handles.find(handleId);
        if (it != Handles.end()) {
            const fuse_ino_t ino = it->second->Index;
            DecreaseStats(it->second->GetStats(), 1);
            Handles.erase(it);
            if (DirectoryEntryVersionCache) {
                DirectoryEntryVersionCache->UnregisterHandle(ino);
            }
        }

        if (Storage) {
            Storage->RemoveHandle(handleId);
        }
    }
}

bool TDirectoryHandleCache::RemoveHandle(ui64 handleId, fuse_ino_t ino)
{
    bool isConsistent = true;

    with_lock (Lock) {
        auto it = Handles.find(handleId);
        if (it != Handles.end()) {
            const fuse_ino_t handleIno = it->second->Index;
            isConsistent = handleIno == ino;

            DecreaseStats(it->second->GetStats(), 1);
            Handles.erase(it);
            if (DirectoryEntryVersionCache) {
                DirectoryEntryVersionCache->UnregisterHandle(handleIno);
            }
        }

        if (Storage) {
            Storage->RemoveHandle(handleId);
        }
    }

    return isConsistent;
}

void TDirectoryHandleCache::ResetHandle(ui64 handleId)
{
    // Rewinddir is rare, locking is fine here.
    with_lock (Lock) {
        auto it = Handles.find(handleId);
        if (it == Handles.end()) {
            return;
        }
        const auto& handle = it->second;

        DecreaseStats(handle->GetStats(), /* openHandles */ 0);

        handle->ResetContent();

        IncreaseStats(handle->GetStats(), /* openHandles */ 0);

        Stats->IncrementRewindCount();

        if (Storage) {
            Storage->ResetHandle(handleId);
        }
    }
}

void TDirectoryHandleCache::AppendChunk(
    ui64 handleId,
    const std::shared_ptr<TDirectoryHandle>& handle,
    const TDirectoryHandleChunk& handleChunk)
{
    // Lock-free: the delta is taken from the chunk itself. A removed-handle
    // race may briefly over-count, but it does not occur under the normal
    // workflow, only during session reset or vhost destruction, where
    // metrics are dropped on restart anyway. The impact is negligible.
    IncreaseStats({handleChunk.GetSerializedSize(), 1}, /* openHandles */ 0);

    if (Storage) {
        Storage->UpdateHandle(handleId, *handle, handleChunk);
    }
}

void TDirectoryHandleCache::Clear()
{
    with_lock (Lock) {
        STORAGE_DEBUG("clear directory cache of size %lu", Handles.size());
        DecreaseStats(SumDirectoryHandlesStats(Handles), Handles.size());
        if (DirectoryEntryVersionCache) {
            for (const auto& [_, handle]: Handles) {
                DirectoryEntryVersionCache->UnregisterHandle(handle->Index);
            }
        }
        Handles.clear();
    }
}

void TDirectoryHandleCache::Reset()
{
    with_lock (Lock) {
        STORAGE_DEBUG("reset directory cache of size %lu", Handles.size());
        DecreaseStats(SumDirectoryHandlesStats(Handles), Handles.size());
        if (DirectoryEntryVersionCache) {
            for (const auto& [_, handle]: Handles) {
                DirectoryEntryVersionCache->UnregisterHandle(handle->Index);
            }
        }
        Handles.clear();

        if (Storage) {
            Storage->Clear();
        }
    }
}

void TDirectoryHandleCache::IncreaseStats(
    const TDirectoryHandleStats& stats,
    size_t openHandles)
{
    Stats->ChangeCacheSize(static_cast<i64>(stats.SerializedSize));
    Stats->ChangeChunkCount(static_cast<i64>(stats.ChunkCount));
    if (openHandles != 0) {
        Stats->ChangeOpenHandleCount(static_cast<i64>(openHandles));
    }
}

void TDirectoryHandleCache::DecreaseStats(
    const TDirectoryHandleStats& stats,
    size_t openHandles)
{
    Stats->ChangeCacheSize(-static_cast<i64>(stats.SerializedSize));
    Stats->ChangeChunkCount(-static_cast<i64>(stats.ChunkCount));
    if (openHandles != 0) {
        Stats->ChangeOpenHandleCount(-static_cast<i64>(openHandles));
    }
}

}   // namespace NCloud::NFileStore::NFuse
