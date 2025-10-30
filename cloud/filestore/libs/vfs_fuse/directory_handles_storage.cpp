#include "directory_handles_storage.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/generic/buffer.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStorage::TDirectoryHandlesStorage(
    TLog& log,
    const TString& filePath,
    ui64 recordsCount,
    ui64 initialDataAreaSize,
    ui64 initialDataCompactionBufferSize)
    : Log(log)
{
    Table = std::make_unique<TDirectoryHandleTable>(
        filePath,
        recordsCount,
        initialDataAreaSize,
        initialDataCompactionBufferSize,
        30);
}

void TDirectoryHandlesStorage::StoreHandle(
    ui64 handleId,
    const TDirectoryHandleChunk& initialHandleChunk)
{
    if (HandleIdToIndices.contains(handleId)) {
        ReportDirectoryHandlesStorageError(
            "Failed to store record with existing handle id");
        return;
    }

    TBuffer record = SerializeHandle(handleId, initialHandleChunk);

    TGuard guard(TableLock);

    CreateRecord(handleId, record);
}

void TDirectoryHandlesStorage::UpdateHandle(
    ui64 handleId,
    const TDirectoryHandleChunk& handleChunk)
{
    TBuffer record = SerializeHandle(handleId, handleChunk);

    TGuard guard(TableLock);

    // update can be called when handle already deleted, in this case just
    // report error
    if (!HandleIdToIndices.contains(handleId)) {
        ReportDirectoryHandlesStorageError(
            "Failed to update record for directory handle, previous parts are "
            "missing");
        return;
    }

    CreateRecord(handleId, record);
}

void TDirectoryHandlesStorage::CreateRecord(
    ui64 handleId,
    const TBuffer& record)
{
    ui64 recordIndex = CreateRecord(record);

    if (recordIndex == TDirectoryHandleTable::InvalidIndex) {
        ReportDirectoryHandlesStorageError(
            "Failed to create record for directory handle chunk");
        return;
    }

    HandleIdToIndices[handleId].push_back(recordIndex);
}

void TDirectoryHandlesStorage::RemoveHandle(ui64 handleId)
{
    TGuard guard(TableLock);
    if (HandleIdToIndices.contains(handleId)) {
        for (auto recordIndex: HandleIdToIndices[handleId]) {
            if (!Table->DeleteRecord(recordIndex)) {
                STORAGE_DEBUG(
                    "failed to delete record for handle %lu using index %lu",
                    handleId,
                    recordIndex);
            }
        }
    }
    HandleIdToIndices.erase(handleId);
}

void TDirectoryHandlesStorage::ResetHandle(ui64 handleId)
{
    TGuard guard(TableLock);
    if (HandleIdToIndices.contains(handleId)) {
        for (auto it = std::next(HandleIdToIndices[handleId].begin(), 1);
             it != HandleIdToIndices[handleId].end();
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
}

void TDirectoryHandlesStorage::LoadHandles(TDirectoryHandleMap& handles)
{
    TGuard guard(TableLock);

    for (auto it = Table->begin(); it != Table->end(); ++it) {
        TStringBuf record = *it;
        if (record.empty()) {
            STORAGE_TRACE(
                "bad record from storage during load directory handles");
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

        if (!handles.contains(handleId)) {
            handles[handleId] =
                std::make_shared<TDirectoryHandle>(chunk->Index);
        }

        handles[handleId]->ConsumeChunk(*chunk);
        HandleIdToIndices[handleId].push_back(it.GetIndex());
    }
}

void TDirectoryHandlesStorage::Clear()
{
    TGuard guard(TableLock);
    Table->Clear();
    HandleIdToIndices.clear();
}

TBuffer TDirectoryHandlesStorage::SerializeHandle(
    ui64 handleId,
    const TDirectoryHandleChunk& handleChunk) const
{
    TBuffer buffer;

    TBufferOutput output(buffer);
    output.Write(&handleId, sizeof(handleId));
    handleChunk.Serialize(output);

    return buffer;
}

TDirectoryHandleChunkPair TDirectoryHandlesStorage::DeserializeHandleChunk(
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

ui64 TDirectoryHandlesStorage::CreateRecord(const TBuffer& record)
{
    ui64 index = Table->AllocRecord(record.Size());
    if (index == TDirectoryHandleTable::InvalidIndex) {
        return TDirectoryHandleTable::InvalidIndex;
    }

    if (!Table->WriteRecordData(index, record.Data(), record.Size())) {
        return TDirectoryHandleTable::InvalidIndex;
    }

    Table->CommitRecord(index);

    return index;
}

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStoragePtr CreateDirectoryHandlesStorage(
    TLog& log,
    const TString& filePath,
    ui64 recordsCount,
    ui64 initialDataAreaSize,
    ui64 initialDataCompactionBufferSize)
{
    return std::make_unique<TDirectoryHandlesStorage>(
        log,
        filePath,
        recordsCount,
        initialDataAreaSize,
        initialDataCompactionBufferSize);
}

}   // namespace NCloud::NFileStore::NFuse
