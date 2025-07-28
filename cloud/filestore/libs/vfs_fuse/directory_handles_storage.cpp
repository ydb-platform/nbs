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
    Storage = std::make_unique<TDirectoryHandleStorage>(
        fileName,
        recordsCount,
        initialDataAreaSize);
}

void TDirectoryHandlesStorage::StoreHandle(
    ui64 handleId,
    const TDirectoryHandle& handle)
{
    TBuffer record = SerializeHandle(handleId, handle);

    TGuard guard(StorageLock);

    if (HandleIdToIndex.contains(handleId)) {
        ReportDirectoryHandlesStorageError(
            "Failed to store record with existing handle id");
        return;
    }

    ui64 recordIndex = Storage->CreateRecord(record);

    if (recordIndex == TDirectoryHandleStorage::InvalidIndex) {
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

    ui64 recordIndex = TDirectoryHandleStorage::InvalidIndex;

    TGuard guard(StorageLock);

    if (HandleIdToIndex.contains(handleId)) {
        recordIndex = HandleIdToIndex[handleId];
    }

    // Not an critical error as we can remove handle before finishing listing
    if (recordIndex == TDirectoryHandleStorage::InvalidIndex) {
        STORAGE_DEBUG("data for handle %lu can't be updated", handleId);
        return;
    }

    recordIndex = Storage->UpdateRecord(recordIndex, record);
    if (recordIndex == TDirectoryHandleStorage::InvalidIndex) {
        ReportDirectoryHandlesStorageError(
            "Failed to update record for directory handle");
        return;
    }
    HandleIdToIndex[handleId] = recordIndex;
}

void TDirectoryHandlesStorage::RemoveHandle(ui64 handleId)
{
    TGuard guard(StorageLock);
    if (HandleIdToIndex.contains(handleId)) {
        if (!Storage->DeleteRecord(HandleIdToIndex[handleId])) {
            STORAGE_DEBUG(
                "failed to delete record for handle %lu using index %lu",
                handleId,
                HandleIdToIndex[handleId]);
        }
        HandleIdToIndex.erase(handleId);
    }
}

void TDirectoryHandlesStorage::LoadHandles(
    THashMap<ui64, std::shared_ptr<TDirectoryHandle>>& handles)
{
    TGuard guard(StorageLock);

    for (auto it = Storage->begin(); it != Storage->end(); ++it) {
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
    TGuard guard(StorageLock);
    Storage->Clear();
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

std::pair<ui64, std::shared_ptr<TDirectoryHandle>>
TDirectoryHandlesStorage::DeserializeHandle(const TStringBuf& buffer) const
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
