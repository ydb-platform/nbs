#pragma once

#include "public.h"

#include "directory_handle_storage_stats.h"
#include "fs_directory_handle.h"

#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/file_backed_containers/dynamic_persistent_table.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandle;

using TDirectoryHandleMap = THashMap<ui64, std::shared_ptr<TDirectoryHandle>>;
using TDirectoryHandleChunkPair =
    std::pair<ui64, std::optional<TDirectoryHandleChunk>>;

struct TDirectoryHandleStorageArgs
{
    TLog Log;
    IFileMapMemoryLimiterPtr FileMapMemoryLimiter;
    IDirectoryHandleStorageStatsPtr Stats;
    TString FilePath;
    ui64 MaxRecords = 0;
    ui64 InitialDataAreaSize = 0;
    ui64 MaxDataAreaStepSize = 0;
    ui64 InitialDataMoveBufferSize = 0;
    ui64 PersistentHandleMaxSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Storage may drop records when the file map memory limit or persistent handle
// size limit is reached. Drops happen at handle granularity: for each handle
// we either keep all of its data or none of it.
class TDirectoryHandleStorage
{
private:
    struct TDirectoryHandleTableHeader
    {
    };

    using TDirectoryHandleTable =
        TDynamicPersistentTable<TDirectoryHandleTableHeader>;

    TLog Log;

    // Mutex for Table, HandleIdToIndices, and HandlesExcludedFromStorage
    TAdaptiveLock TableLock;
    std::unique_ptr<TDirectoryHandleTable> Table;
    THashMap<ui64, std::vector<ui64>> HandleIdToIndices;
    THashSet<ui64> HandlesExcludedFromStorage;
    const ui64 PersistentHandleMaxSize;
    IDirectoryHandleStorageStatsPtr Stats;

public:
    explicit TDirectoryHandleStorage(TDirectoryHandleStorageArgs args);

    void StoreHandle(
        ui64 handleId,
        const TDirectoryHandle& handle,
        const TDirectoryHandleChunk& initialHandleChunk);
    void UpdateHandle(
        ui64 handleId,
        const TDirectoryHandle& handle,
        const TDirectoryHandleChunk& handleChunk);
    void RemoveHandle(ui64 handleId);
    void ResetHandle(ui64 handleId);
    void LoadHandles(TDirectoryHandleMap& handles);
    void Clear();

private:
    TBuffer SerializeHandle(
        ui64 handleId,
        const TDirectoryHandleChunk& handleChunk) const;
    TDirectoryHandleChunkPair DeserializeHandleChunk(const TStringBuf& buffer);

    TResultOrError<ui64> CreateRecord(const TBuffer& record);
    void CreateRecord(
        ui64 handleId,
        const TBuffer& record,
        ui64 handleSerializedSize);
    bool CanStoreHandle(ui64 handleSerializedSize) const;
    void RemoveRecords(ui64 handleId);
    void UpdateStats();
};

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStoragePtr CreateDirectoryHandleStorage(
    TDirectoryHandleStorageArgs args);

}   // namespace NCloud::NFileStore::NFuse
