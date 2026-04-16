#include "directory_handles_manager.h"

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesManager::TDirectoryHandlesManager(
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

ui64 TDirectoryHandlesManager::CreateHandle(fuse_ino_t ino)
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

std::shared_ptr<TDirectoryHandle> TDirectoryHandlesManager::FindHandle(
    ui64 handleId)
{
    with_lock (Lock) {
        auto it = Handles.find(handleId);
        return it != Handles.end() ? it->second : nullptr;
    }
}

void TDirectoryHandlesManager::RemoveHandle(ui64 handleId)
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

bool TDirectoryHandlesManager::RemoveHandle(ui64 handleId, fuse_ino_t ino)
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

void TDirectoryHandlesManager::ResetHandle(ui64 handleId)
{
    auto handle = FindHandle(handleId);
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

void TDirectoryHandlesManager::AppendChunk(
    ui64 handleId,
    const TDirectoryHandleChunk& handleChunk)
{
    if (Storage) {
        Storage->UpdateHandle(handleId, handleChunk);
    }

    Stats->IncreaseCacheSize(handleChunk.GetSerializedSize());
    Stats->IncreaseChunkCount(1);
}

void TDirectoryHandlesManager::ClearCache()
{
    with_lock (Lock) {
        STORAGE_DEBUG("clear directory cache of size %lu", Handles.size());
        Handles.clear();
    }
}

void TDirectoryHandlesManager::Reset()
{
    with_lock (Lock) {
        STORAGE_DEBUG("clear directory cache of size %lu", Handles.size());
        Handles.clear();

        if (Storage) {
            Storage->Clear();
        }
    }
}

}   // namespace NCloud::NFileStore::NFuse
