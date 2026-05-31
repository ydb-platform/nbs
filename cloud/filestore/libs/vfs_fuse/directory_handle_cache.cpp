#include "directory_handle_cache.h"

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDirectoryHandleMetrics
{
    size_t SerializedSize = 0;
    size_t ChunkCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleMetrics GetDirectoryHandleMetrics(
    const TDirectoryHandleMap& handles)
{
    TDirectoryHandleMetrics metrics;

    for (const auto& [_, handle]: handles) {
        if (handle) {
            const auto [serializedSize, chunkCount] = handle->GetMetrics();
            metrics.SerializedSize += serializedSize;
            metrics.ChunkCount += chunkCount;
        }
    }

    return metrics;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleCache::TDirectoryHandleCache(
    TLog log,
    TDirectoryHandleStatsPtr stats,
    TDirectoryHandleStoragePtr storage)
    : Log(std::move(log))
    , Storage(std::move(storage))
    , Stats(std::move(stats))
{
    if (Storage) {
        Storage->LoadHandles(Handles);
    }

    IncreaseStatsForHandles(Handles);
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

        if (Storage) {
            Storage->StoreHandle(
                handleId,
                *handle,
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
            isConsistent = it->second->Index == ino;

            DecreaseStatsForHandle(it->second);
            Handles.erase(it);
        }

        if (Storage) {
            Storage=nullptr;
            Storage->RemoveHandle(handleId);
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

    if (Storage) {
        Storage->ResetHandle(handleId);
    }
}

void TDirectoryHandleCache::AppendChunk(
    ui64 handleId,
    const std::shared_ptr<TDirectoryHandle>& handle,
    const TDirectoryHandleChunk& handleChunk)
{
    Stats->IncreaseCacheSize(handleChunk.GetSerializedSize());
    Stats->IncreaseChunkCount(1);

    if (Storage && handle) {
        Storage->UpdateHandle(handleId, *handle, handleChunk);
    }
}

void TDirectoryHandleCache::Clear()
{
    with_lock (Lock) {
        STORAGE_DEBUG("clear directory cache of size %lu", Handles.size());
        DecreaseStatsForHandles(Handles);
        Handles.clear();
    }
}

void TDirectoryHandleCache::Reset()
{
    with_lock (Lock) {
        STORAGE_DEBUG("reset directory cache of size %lu", Handles.size());
        DecreaseStatsForHandles(Handles);
        Handles.clear();

        if (Storage) {
            Storage->Clear();
        }
    }
}

void TDirectoryHandleCache::IncreaseStatsForHandle(
    const std::shared_ptr<TDirectoryHandle>& handle)
{
    if (handle) {
        const auto [serializedSize, chunkCount] = handle->GetMetrics();
        Stats->IncreaseCacheSize(serializedSize);
        Stats->IncreaseChunkCount(chunkCount);
    }
}

void TDirectoryHandleCache::IncreaseStatsForHandles(
    const TDirectoryHandleMap& handles)
{
    const auto metrics = GetDirectoryHandleMetrics(handles);
    Stats->IncreaseCacheSize(metrics.SerializedSize);
    Stats->IncreaseChunkCount(metrics.ChunkCount);
}

void TDirectoryHandleCache::DecreaseStatsForHandle(
    const std::shared_ptr<TDirectoryHandle>& handle)
{
    if (handle) {
        const auto [serializedSize, chunkCount] = handle->GetMetrics();
        Stats->DecreaseCacheSize(serializedSize);
        Stats->DecreaseChunkCount(chunkCount);
    }
}

void TDirectoryHandleCache::DecreaseStatsForHandles(
    const TDirectoryHandleMap& handles)
{
    const auto metrics = GetDirectoryHandleMetrics(handles);
    Stats->DecreaseCacheSize(metrics.SerializedSize);
    Stats->DecreaseChunkCount(metrics.ChunkCount);
}

}   // namespace NCloud::NFileStore::NFuse
