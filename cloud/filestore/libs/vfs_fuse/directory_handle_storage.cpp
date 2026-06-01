#include "directory_handle_storage.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/buffer.h>
#include <util/system/yassert.h>

#include <utility>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStorage::TDirectoryHandleStorage(
    TDirectoryHandleStorageArgs args)
    : Log(std::move(args.Log))
    , PersistentHandleMaxSize(args.PersistentHandleMaxSize)
    , Stats(std::move(args.Stats))
{
    Y_ABORT_UNLESS(Stats);

    Table = std::make_unique<TDirectoryHandleTable>(
        args.FilePath,
        TDynamicPersistentTableConfig{
            .MaxRecords = args.MaxRecords,
            .InitialDataAreaSize = args.InitialDataAreaSize,
            .MaxDataAreaStepSize = args.MaxDataAreaStepSize,
            .InitialDataMoveBufferSize = args.InitialDataMoveBufferSize,
        },
        std::move(args.FileMapMemoryLimiter));

    UpdateStats();
}

void TDirectoryHandleStorage::StoreHandle(
    ui64 handleId,
    const TDirectoryHandle& handle,
    const TDirectoryHandleChunk& initialHandleChunk)
{
    TBuffer record = SerializeHandle(handleId, initialHandleChunk);

    TGuard guard(TableLock);

    if (HandleIdToIndices.contains(handleId)) {
        ReportDirectoryHandlesStorageError(
            "Failed to store record with existing handle id");
        RemoveRecords(handleId);
        HandlesExcludedFromStorage.insert(handleId);
        UpdateStats();
        return;
    }

    CreateRecord(handleId, record, handle.GetMetrics().first);
}

void TDirectoryHandleStorage::UpdateHandle(
    ui64 handleId,
    const TDirectoryHandle& handle,
    const TDirectoryHandleChunk& handleChunk)
{
    TBuffer record = SerializeHandle(handleId, handleChunk);

    TGuard guard(TableLock);

    if (HandlesExcludedFromStorage.contains(handleId)) {
        UpdateStats();
        return;
    }

    // update can be called when handle already deleted, in this case just
    // log info and return
    if (!HandleIdToIndices.contains(handleId)) {
        STORAGE_DEBUG(
            "failed to update record for handle %lu, handle is already deleted",
            handleId);
        UpdateStats();
        return;
    }

    CreateRecord(handleId, record, handle.GetMetrics().first);
}

void TDirectoryHandleStorage::CreateRecord(
    ui64 handleId,
    const TBuffer& record,
    ui64 handleSerializedSize)
{
    const auto dropRecordsForHandle = [&]
    {
        RemoveRecords(handleId);
        HandlesExcludedFromStorage.insert(handleId);
    };

    if (!CanStoreHandle(handleSerializedSize)) {
        STORAGE_WARN(
            "Dropping directory handle "
            << handleId << " from persistent storage: size "
            << handleSerializedSize << " exceeds limit "
            << PersistentHandleMaxSize);
        dropRecordsForHandle();
        UpdateStats();
        return;
    }

    auto result = CreateRecord(record);
    if (HasError(result)) {
        if (result.GetError().GetCode() == E_FS_NOSPC) {
            Stats->IncrementMemoryControllerRejectCount();
        }

        const auto it = HandleIdToIndices.find(handleId);
        const size_t indexCount =
            it == HandleIdToIndices.end() ? 0 : it->second.size();
        STORAGE_WARN(
            "Failed to create directory handle record for handle "
            << handleId << ", indices count: " << indexCount << ": "
            << FormatError(result.GetError()));
        dropRecordsForHandle();
        UpdateStats();
        return;
    }

    const ui64 recordIndex = result.GetResult();
    HandleIdToIndices[handleId].push_back(recordIndex);
    UpdateStats();
}

void TDirectoryHandleStorage::RemoveHandle(ui64 handleId)
{
    TGuard guard(TableLock);
    HandlesExcludedFromStorage.erase(handleId);
    RemoveRecords(handleId);
    UpdateStats();
}

void TDirectoryHandleStorage::ResetHandle(ui64 handleId)
{
    TGuard guard(TableLock);
    if (HandlesExcludedFromStorage.contains(handleId)) {
        UpdateStats();
        return;
    }

    if (HandleIdToIndices.contains(handleId)) {
        for (auto it = HandleIdToIndices[handleId].rbegin();
             it != std::prev(HandleIdToIndices[handleId].rend(), 1);
             ++it)
        {
            if (!Table->DeleteRecord(*it)) {
                STORAGE_DEBUG(
                    "failed to delete record for handle %lu using index %lu",
                    handleId,
                    *it);
            }
        }
        HandleIdToIndices[handleId].erase(
            std::next(HandleIdToIndices[handleId].begin(), 1),
            HandleIdToIndices[handleId].end());
    }

    UpdateStats();
}

void TDirectoryHandleStorage::LoadHandles(TDirectoryHandleMap& handles)
{
    // Since we store data in chunks instead of a single block, in rare cases
    // a crash during the reset or removal process can lead to inconsistent
    // chunks order. We detect this inconsistency during the load phase and
    // clean data for this handle.
    struct TChunkInfo
    {
        ui64 UpdateVersion = 0;
        ui64 StorageIndex = 0;
    };

    TMap<ui64, TVector<TChunkInfo>> chunksInfo;

    {
        TGuard guard(TableLock);

        for (auto it = Table->begin(); it != Table->end(); ++it) {
            TStringBuf record = *it;
            if (record.empty()) {
                STORAGE_ERROR(
                    "bad record from storage during load directory handles, "
                    "record index: %lu",
                    it.GetIndex());
                continue;
            }

            auto [handleId, chunk] = DeserializeHandleChunk(record);

            if (!handleId) {
                STORAGE_DEBUG(
                    "bad deserialize for record %lu from storage during load "
                    "directory handles",
                    it.GetIndex());
                continue;
            }

            chunksInfo[handleId].push_back(
                TChunkInfo{chunk->UpdateVersion, it.GetIndex()});

            if (!handles.contains(handleId)) {
                handles[handleId] =
                    std::make_shared<TDirectoryHandle>(chunk->Index);
            }

            handles[handleId]->ConsumeChunk(*chunk, Log);
        }
    }

    // When resetting a handle, we must remove chunks in reverse order
    // of their update version to avoid corruption if we crash mid-reset.
    // Therefore, after loading handles, we must keep chunks sorted
    // by update version.
    for (auto& [handleId, chunks]: chunksInfo) {
        std::sort(
            chunks.begin(),
            chunks.end(),
            [](const TChunkInfo& a, const TChunkInfo& b)
            { return a.UpdateVersion < b.UpdateVersion; });

        HandleIdToIndices[handleId].reserve(chunks.size());
        for (const auto& chunk: chunks) {
            HandleIdToIndices[handleId].push_back(chunk.StorageIndex);
        }
    }

    for (const auto& [handleId, chunks]: chunksInfo) {
        for (size_t i = 0; i < chunks.size(); ++i) {
            const ui64 expectedVersion = i;
            const auto& chunk = chunks[i];

            if (chunk.UpdateVersion != expectedVersion) {
                ReportDirectoryHandlesStorageError(
                    TStringBuilder()
                    << "Corrupted data for handle " << handleId
                    << ": expected update version " << expectedVersion
                    << ", got " << chunk.UpdateVersion);

                RemoveHandle(handleId);
                handles.erase(handleId);
                break;
            }
        }
    }

    TVector<ui64> oversizedHandleIds;
    if (PersistentHandleMaxSize) {
        for (const auto& [handleId, handle]: handles) {
            const ui64 handleSerializedSize = handle->GetMetrics().first;
            if (handleSerializedSize <= PersistentHandleMaxSize) {
                continue;
            }

            STORAGE_WARN(
                "Dropping directory handle "
                << handleId << " from persistent storage: "
                << "loaded size " << handleSerializedSize << " exceeds limit "
                << PersistentHandleMaxSize);
            oversizedHandleIds.push_back(handleId);
        }
    }

    for (const auto handleId: oversizedHandleIds) {
        RemoveHandle(handleId);
        handles.erase(handleId);
    }

    UpdateStats();
}

void TDirectoryHandleStorage::Clear()
{
    TGuard guard(TableLock);
    Table->Clear();
    HandleIdToIndices.clear();
    HandlesExcludedFromStorage.clear();
    UpdateStats();
}

// TODO: We can optimize this by counting size for serialization dynamically and
// when needed serialize it directly to the file without additional copying.
TBuffer TDirectoryHandleStorage::SerializeHandle(
    ui64 handleId,
    const TDirectoryHandleChunk& handleChunk) const
{
    TBuffer buffer;

    TBufferOutput output(buffer);
    output.Write(&handleId, sizeof(handleId));
    handleChunk.Serialize(output);

    return buffer;
}

TDirectoryHandleChunkPair TDirectoryHandleStorage::DeserializeHandleChunk(
    const TStringBuf& buffer)
{
    TMemoryInput input(buffer);
    ui64 handleId;
    if (input.Load(&handleId, sizeof(handleId)) != sizeof(handleId)) {
        return {0, std::nullopt};
    }

    auto chunk = TDirectoryHandleChunk::Deserialize(input);
    if (!chunk) {
        return {0, std::nullopt};
    }

    return {handleId, chunk};
}

TResultOrError<ui64> TDirectoryHandleStorage::CreateRecord(
    const TBuffer& record)
{
    auto result = Table->AllocRecord(record.Size());
    if (HasError(result)) {
        return result.GetError();
    }

    const ui64 index = result.GetResult();
    if (!Table->WriteRecordData(index, record.Data(), record.Size())) {
        return MakeError(E_FAIL, "Failed to write table record");
    }

    Table->CommitRecord(index);

    return index;
}

bool TDirectoryHandleStorage::CanStoreHandle(ui64 handleSerializedSize) const
{
    if (!PersistentHandleMaxSize) {
        return true;
    }

    return handleSerializedSize <= PersistentHandleMaxSize;
}

void TDirectoryHandleStorage::RemoveRecords(ui64 handleId)
{
    auto it = HandleIdToIndices.find(handleId);
    if (it == HandleIdToIndices.end()) {
        return;
    }

    for (auto recordIndex: it->second) {
        if (!Table->DeleteRecord(recordIndex)) {
            STORAGE_WARN(
                "failed to delete record for handle %lu using index %lu",
                handleId,
                recordIndex);
        }
    }

    HandleIdToIndices.erase(it);
    Table->TryDeallocateMemory();
}

void TDirectoryHandleStorage::UpdateStats()
{
    const auto stats = Table->GetStats();
    Stats->SetCounters({
        .FileMapSize = stats.FileMapSize,
        .ShrinkCount = stats.ShrinkCount,
        .ExpansionCount = stats.ExpansionCount,
        .CompactionCount = stats.CompactionCount,
        .UsedSpace = stats.UsedSpace,
    });
}

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStoragePtr CreateDirectoryHandleStorage(
    TDirectoryHandleStorageArgs args)
{
    return std::make_unique<TDirectoryHandleStorage>(std::move(args));
}

}   // namespace NCloud::NFileStore::NFuse
