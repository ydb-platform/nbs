#include "directory_handles_storage.h"

#include "fs_directory_handle.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/generic/buffer.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStorage::TDirectoryHandlesStorage(
    TLog& log,
    const TString& fileName,
    ui64 recordsCount,
    ui64 initialDataAreaSize)
    : Log(log)
{
    Table = std::make_unique<TDirectoryHandleTable>(
        fileName,
        recordsCount,
        initialDataAreaSize,
        30);
}

void TDirectoryHandlesStorage::StoreHandle(
    ui64 handleId,
    const TDirectoryHandle& handle)
{
    TBuffer record = SerializeHandle(handleId, handle);

    TGuard guard(TableLock);

    if (HandleIdToIndex.contains(handleId)) {
        ReportDirectoryHandlesStorageError(
            "Failed to store record with existing handle id");
        return;
    }

    ui64 recordIndex = CreateRecord(record);

    if (recordIndex == TDirectoryHandleTable::InvalidIndex) {
        ReportDirectoryHandlesStorageError(
            "Failed to create record for directory handle");
        return;
    }

    HandleIdToIndex[handleId] = recordIndex;
}

void TDirectoryHandlesStorage::UpdateHandle(
    ui64 handleId,
    const TDirectoryHandle& handle)
{
    TBuffer record = SerializeHandle(handleId, handle);

    ui64 recordIndex = TDirectoryHandleTable::InvalidIndex;

    TGuard guard(TableLock);

    if (HandleIdToIndex.contains(handleId)) {
        recordIndex = HandleIdToIndex[handleId];
    }

    // Not an critical error as we can remove handle before finishing listing
    if (recordIndex == TDirectoryHandleTable::InvalidIndex) {
        STORAGE_DEBUG("data for handle %lu can't be updated", handleId);
        return;
    }

    recordIndex = UpdateRecord(recordIndex, record);

    if (recordIndex == TDirectoryHandleTable::InvalidIndex) {
        ReportDirectoryHandlesStorageError(
            "Failed to update record for directory handle");
        return;
    }
    HandleIdToIndex[handleId] = recordIndex;
}

void TDirectoryHandlesStorage::RemoveHandle(ui64 handleId)
{
    TGuard guard(TableLock);
    if (HandleIdToIndex.contains(handleId)) {
        if (!Table->DeleteRecord(HandleIdToIndex[handleId])) {
            STORAGE_DEBUG(
                "failed to delete record for handle %lu using index %lu",
                handleId,
                HandleIdToIndex[handleId]);
        }
        HandleIdToIndex.erase(handleId);
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

        auto [handleId, handle] = DeserializeHandle(record);

        if (!handle) {
            STORAGE_DEBUG(
                "bad deserialize for record %lu from storage during load "
                "directory handles",
                it.GetIndex());
            continue;
        }

        handles[handleId] = std::move(handle);
        HandleIdToIndex[handleId] = it.GetIndex();
    }
}

void TDirectoryHandlesStorage::Clear()
{
    TGuard guard(TableLock);
    Table->Clear();
    HandleIdToIndex.clear();
}

TBuffer TDirectoryHandlesStorage::SerializeHandle(
    ui64 handleId,
    const TDirectoryHandle& handle) const
{
    TBuffer buffer;

    TBufferOutput output(buffer);
    output.Write(&handleId, sizeof(handleId));
    handle.Serialize(output);

    return buffer;
}

TDirectoryHandlePair TDirectoryHandlesStorage::DeserializeHandle(
    const TStringBuf& buffer) const
{
    TMemoryInput input(buffer);
    ui64 handleId;
    if (input.Load(&handleId, sizeof(handleId)) != sizeof(handleId)) {
        return {0, nullptr};
    }
    auto handle = TDirectoryHandle::Deserialize(input);
    if (!handle) {
        return {0, nullptr};
    }
    return {handleId, handle};
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

ui64 TDirectoryHandlesStorage::UpdateRecord(ui64 index, const TBuffer& record)
{
    TStringBuf currentRecord = Table->GetRecord(index);
    if (currentRecord.empty()) {
        return TDirectoryHandleTable::InvalidIndex;
    }

    if (record.Size() <= currentRecord.size()) {
        if (!Table->WriteRecordData(index, record.Data(), record.Size())) {
            return TDirectoryHandleTable::InvalidIndex;
        }

        return index;
    }

    Table->DeleteRecord(index);

    return CreateRecord(record);
}

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStoragePtr CreateDirectoryHandlesStorage(
    TLog& log,
    const TString& filePath,
    ui64 recordsCount,
    ui64 initialDataAreaSize)
{
    return std::make_unique<TDirectoryHandlesStorage>(
        log,
        filePath,
        recordsCount,
        initialDataAreaSize);
}

}   // namespace NCloud::NFileStore::NFuse
