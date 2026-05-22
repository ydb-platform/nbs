#pragma once

#include "public.h"

#include "fs_directory_handle.h"

#include <cloud/storage/core/libs/common/dynamic_persistent_table.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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

////////////////////////////////////////////////////////////////////////////////

// Storage with the provided memory controller may drop records when the
// memory limit is reached. Drops happen at handle granularity: for each
// handle we either keep all of its data or none of it.
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

public:
    explicit TDirectoryHandleStorage(
        TLog& log,
        const TString& filePath,
        ui64 recordsCount,
        ui64 initialDataAreaSize,
        ui64 maxDataAreaStepSize,
        ui64 initialDataMoveBufferSize,
        IMemoryControllerPtr memoryController);

    void StoreHandle(
        ui64 handleId,
        const TDirectoryHandleChunk& initialHandleChunk);
    void UpdateHandle(ui64 handleId, const TDirectoryHandleChunk& handleChunk);
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
    NProto::TError CreateRecord(ui64 handleId, const TBuffer& record);
    void RemoveRecords(ui64 handleId);
};

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandleStoragePtr CreateDirectoryHandleStorage(
    TLog& log,
    const TString& filePath,
    ui64 recordsCount,
    ui64 initialDataAreaSize,
    ui64 maxDataAreaStepSize,
    ui64 initialDataMoveBufferSize,
    IMemoryControllerPtr memoryController = nullptr);

}   // namespace NCloud::NFileStore::NFuse
