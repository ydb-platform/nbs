#pragma once

#include "public.h"

#include "fs_directory_handle.h"

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
using TDirectoryHandleChunkPair =
    std::pair<ui64, std::optional<TDirectoryHandleChunk>>;

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

    // Mutex for Table and HandleIdToIndices
    TMutex TableLock;
    std::unique_ptr<TDirectoryHandleTable> Table;
    THashMap<ui64, std::vector<ui64>> HandleIdToIndices;

public:
    explicit TDirectoryHandlesStorage(
        TLog& log,
        const TString& filePath,
        ui64 recordsCount,
        ui64 initialDataAreaSize,
        ui64 initialDataCompactionBufferSize);

    void StoreHandle(
        ui64 handleId,
        const TDirectoryHandleChunk& initialHandleChunk);
    void UpdateHandle(ui64 handleId, const TDirectoryHandleChunk& handleChunk);
    void RemoveHandle(ui64 handleId);
    void LoadHandles(TDirectoryHandleMap& handles);
    void Clear();

private:
    TBuffer SerializeHandle(
        ui64 handleId,
        const TDirectoryHandleChunk& handleChunk) const;
    TDirectoryHandleChunkPair DeserializeHandleChunk(const TStringBuf& buffer);

    ui64 CreateRecord(const TBuffer& record);
    ui64 UpdateRecord(ui64 index, const TBuffer& record);
};

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStoragePtr CreateDirectoryHandlesStorage(
    TLog& log,
    const TString& filePath,
    ui64 recordsCount,
    ui64 initialDataAreaSize,
    ui64 initialDataCompactionBufferSize);

}   // namespace NCloud::NFileStore::NFuse
