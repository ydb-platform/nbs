#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/dynamic_persistent_table.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandle;

using TDirectoryHandleMap = THashMap<ui64, std::shared_ptr<TDirectoryHandle>>;
using TDirectoryHandlePair = std::pair<ui64, std::shared_ptr<TDirectoryHandle>>;

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandlesStorage
{
private:
    struct TDirectoryHandleTableHeader
    {
    };

    using TDirectoryHandleTable =
        TDynamicPersistentTable<TDirectoryHandleTableHeader>;

    TLog Log;

    // Mutex for Table and HandleIdToIndex
    TMutex TableLock;
    std::unique_ptr<TDirectoryHandleTable> Table;
    THashMap<ui64, ui64> HandleIdToIndex;

public:
    explicit TDirectoryHandlesStorage(
        TLog& log,
        const TString& filePath,
        ui64 recordsCount,
        ui64 initialDataAreaSize);

    void StoreHandle(ui64 handleId, const TDirectoryHandle& handle);
    void UpdateHandle(ui64 handleId, const TDirectoryHandle& handle);
    void RemoveHandle(ui64 handleId);
    void LoadHandles(TDirectoryHandleMap& handles);
    void Clear();

private:
    TBuffer SerializeHandle(
        ui64 handleId,
        const TDirectoryHandle& handle) const;
    TDirectoryHandlePair DeserializeHandle(const TStringBuf& buffer) const;

    ui64 CreateRecord(const TBuffer& record);
    ui64 UpdateRecord(ui64 index, const TBuffer& record);
};

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStoragePtr CreateDirectoryHandlesStorage(
    TLog& log,
    const TString& fileName,
    ui64 recordsCount = 1000,
    ui64 initialDataAreaSize = 1024 * 1024);

}   // namespace NCloud::NFileStore::NFuse
