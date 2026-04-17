#include "directory_handles_cache.h"

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesCache::TDirectoryHandlesCache(
    TLog log,
    TDirectoryHandlesStatsPtr stats,
    TDirectoryHandlesStoragePtr storage)
    : Log(std::move(log))
    , Storage(std::move(storage))
    , Stats(std::move(stats))
{
    if (Storage) {
        Storage->LoadHandles(Handles);

        for (const auto& [_, handle]: Handles) {
            Stats->IncreaseCacheSize(handle->GetSerializedSize());
            Stats->IncreaseChunkCount(handle->GetChunkCount());
        }
    }
}

ui64 TDirectoryHandlesCache::CreateHandle(fuse_ino_t ino)
{
    ui64 handleId = 0;
    auto handle = std::make_shared<TDirectoryHandle>(ino);

    with_lock (Lock) {
        do {
            handleId = RandomNumber<ui64>();
        } while (!Handles.try_emplace(handleId, handle).second);

        if (Storage) {
            Storage->StoreHandle(handleId, TDirectoryHandleChunk{.Index = ino});
        }

        Stats->IncreaseCacheSize(handle->GetSerializedSize());
        Stats->IncreaseChunkCount(handle->GetChunkCount());
    }

    return handleId;
}

std::shared_ptr<TDirectoryHandle> TDirectoryHandlesCache::FindHandle(
    ui64 handleId)
{
    with_lock (Lock) {
        auto it = Handles.find(handleId);
        return it != Handles.end() ? it->second : nullptr;
    }
}

void TDirectoryHandlesCache::RemoveHandle(ui64 handleId)
{
    with_lock (Lock) {
        auto it = Handles.find(handleId);
        if (it != Handles.end()) {
            Stats->DecreaseCacheSize(it->second->GetSerializedSize());
            Stats->DecreaseChunkCount(it->second->GetChunkCount());
            Handles.erase(it);
        }

        if (Storage) {
            Storage->RemoveHandle(handleId);
        }
    }
}

bool TDirectoryHandlesCache::RemoveHandle(ui64 handleId, fuse_ino_t ino)
{
    bool isConsistent = true;

    with_lock (Lock) {
        auto it = Handles.find(handleId);
        if (it != Handles.end()) {
            isConsistent = it->second->Index == ino;

            Stats->DecreaseCacheSize(it->second->GetSerializedSize());
            Stats->DecreaseChunkCount(it->second->GetChunkCount());
            Handles.erase(it);
        }

        if (Storage) {
            Storage->RemoveHandle(handleId);
        }
    }

    return isConsistent;
}

void TDirectoryHandlesCache::ResetHandle(
    ui64 handleId,
    const std::shared_ptr<TDirectoryHandle>& handle)
{
    if (!handle) {
        return;
    }

    Stats->DecreaseCacheSize(handle->GetSerializedSize());
    Stats->DecreaseChunkCount(handle->GetChunkCount());

    handle->ResetContent();

    Stats->IncreaseCacheSize(handle->GetSerializedSize());
    Stats->IncreaseChunkCount(handle->GetChunkCount());

    if (Storage) {
        Storage->ResetHandle(handleId);
    }
}

void TDirectoryHandlesCache::AppendChunk(
    ui64 handleId,
    const TDirectoryHandleChunk& handleChunk)
{
    if (Storage) {
        Storage->UpdateHandle(handleId, handleChunk);
    }

    Stats->IncreaseCacheSize(handleChunk.GetSerializedSize());
    Stats->IncreaseChunkCount(1);
}

void TDirectoryHandlesCache::Clear()
{
    with_lock (Lock) {
        STORAGE_DEBUG("clear directory cache of size %lu", Handles.size());
        Handles.clear();
    }
}

void TDirectoryHandlesCache::Reset()
{
    with_lock (Lock) {
        STORAGE_DEBUG("reset directory cache of size %lu", Handles.size());
        Handles.clear();

        if (Storage) {
            Storage->Clear();
        }
    }
}

}   // namespace NCloud::NFileStore::NFuse
