#include "directory_handle_cache.h"

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleCache::TDirectoryHandleCache(
    TLog log,
    TDirectoryHandleStatsPtr stats,
    TDirectoryHandleStoragePtr memoryAwareStorage)
    : Log(std::move(log))
    , MemoryAwareStorage(std::move(memoryAwareStorage))
    , Stats(std::move(stats))
{
    if (MemoryAwareStorage) {
        MemoryAwareStorage->LoadHandles(Handles);
    }

    for (const auto& [_, handle]: Handles) {
        IncreaseStatsForHandle(handle);
    }
}

ui64 TDirectoryHandleCache::CreateHandle(fuse_ino_t ino)
{
    ui64 handleId = 0;
    auto handle = std::make_shared<TDirectoryHandle>(ino);

    with_lock (Lock) {
        do {
            handleId = RandomNumber<ui64>();
        } while (!Handles.try_emplace(handleId, handle).second);

        IncreaseStatsForHandle(handle);

        if (MemoryAwareStorage) {
            MemoryAwareStorage->StoreHandle(
                handleId,
                TDirectoryHandleChunk{.Index = ino});
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
            DecreaseStatsForHandle(it->second);
            Handles.erase(it);
        }

        if (MemoryAwareStorage) {
            MemoryAwareStorage->RemoveHandle(handleId);
        }
    }
}

bool TDirectoryHandleCache::RemoveHandle(ui64 handleId, fuse_ino_t ino)
{
    bool isConsistent = true;

    with_lock (Lock) {
        auto it = Handles.find(handleId);
        if (it != Handles.end()) {
            isConsistent = it->second->Index == ino;

            DecreaseStatsForHandle(it->second);
            Handles.erase(it);
        }

        if (MemoryAwareStorage) {
            MemoryAwareStorage->RemoveHandle(handleId);
        }
    }

    return isConsistent;
}

void TDirectoryHandleCache::ResetHandle(
    ui64 handleId,
    const std::shared_ptr<TDirectoryHandle>& handle)
{
    if (!handle) {
        return;
    }

    DecreaseStatsForHandle(handle);

    handle->ResetContent();

    IncreaseStatsForHandle(handle);

    if (MemoryAwareStorage) {
        MemoryAwareStorage->ResetHandle(handleId);
    }
}

void TDirectoryHandleCache::AppendChunk(
    ui64 handleId,
    const TDirectoryHandleChunk& handleChunk)
{
    if (Stats) {
        Stats->IncreaseCacheSize(handleChunk.GetSerializedSize());
        Stats->IncreaseChunkCount(1);
    }

    if (MemoryAwareStorage) {
        MemoryAwareStorage->UpdateHandle(handleId, handleChunk);
    }
}

void TDirectoryHandleCache::Clear()
{
    with_lock (Lock) {
        STORAGE_DEBUG("clear directory cache of size %lu", Handles.size());
        for (const auto& [_, handle]: Handles) {
            DecreaseStatsForHandle(handle);
        }
        Handles.clear();
    }
}

void TDirectoryHandleCache::Reset()
{
    with_lock (Lock) {
        STORAGE_DEBUG("reset directory cache of size %lu", Handles.size());
        for (const auto& [_, handle]: Handles) {
            DecreaseStatsForHandle(handle);
        }
        Handles.clear();

        if (MemoryAwareStorage) {
            MemoryAwareStorage->Clear();
        }
    }
}

void TDirectoryHandleCache::IncreaseStatsForHandle(
    const std::shared_ptr<TDirectoryHandle>& handle)
{
    if (Stats && handle) {
        Stats->IncreaseCacheSize(handle->GetSerializedSize());
        Stats->IncreaseChunkCount(handle->GetChunkCount());
    }
}

void TDirectoryHandleCache::DecreaseStatsForHandle(
    const std::shared_ptr<TDirectoryHandle>& handle)
{
    if (Stats && handle) {
        Stats->DecreaseCacheSize(handle->GetSerializedSize());
        Stats->DecreaseChunkCount(handle->GetChunkCount());
    }
}

}   // namespace NCloud::NFileStore::NFuse
