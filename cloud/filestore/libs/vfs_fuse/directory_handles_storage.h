#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/persistent_dynamic_storage.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandle;

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandlesStorage
{
private:
    struct TDirectoryHandleTableHeader
    {
    };

    using TDirectoryHandleStorage =
        TPersistentDynamicStorage<TDirectoryHandleTableHeader>;

    TLog Log;

    // Mutex for Storage and HandleIdToIndex
    TMutex StorageLock;
    std::unique_ptr<TDirectoryHandleStorage> Storage;
    THashMap<ui64, ui64> HandleIdToIndex;

public:
    explicit TDirectoryHandlesStorage(
        TLog& log,
        const TString& filePath,
        ui64 recordsCount = 1000,
        ui64 initialDataAreaSize = 1024 * 1024);

    void StoreHandle(ui64 handleId, const TDirectoryHandle& handle);
    void UpdateHandle(ui64 handleId, const TDirectoryHandle& handle);
    void RemoveHandle(ui64 handleId);
    void LoadHandles(
        THashMap<ui64, std::shared_ptr<TDirectoryHandle>>& handles);
    void Clear();

private:
    TBuffer SerializeHandle(
        ui64 handleId,
        const TDirectoryHandle& handle) const;
    std::pair<ui64, std::shared_ptr<TDirectoryHandle>> DeserializeHandle(
        const TStringBuf& buffer) const;
};

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStoragePtr CreateDirectoryHandlesStorage(
    TLog& log,
    const TString& fileName,
    ui64 recordsCount = 1000,
    ui64 initialDataAreaSize = 1024 * 1024);

}   // namespace NCloud::NFileStore::NFuse
