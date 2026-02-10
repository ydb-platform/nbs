#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/function_ref.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStorageStats
{
    ui64 RawCapacityByteCount = 0;
    ui64 RawUsedByteCount = 0;
    ui64 EntryCount = 0;
    bool IsCorrupted = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistentStorageStats
{
    virtual ~IPersistentStorageStats() = default;

    virtual void UpdatePersistentStorageStats(
        const TPersistentStorageStats& stats) = 0;
};

using IPersistentStorageStatsPtr = std::shared_ptr<IPersistentStorageStats>;

////////////////////////////////////////////////////////////////////////////////

// Non-thread safe
struct IPersistentStorage
{
    using TVisitor = TFunctionRef<void(TStringBuf buffer)>;
    using TAllocationWriter = TFunctionRef<void(char* ptr, size_t size)>;

    virtual ~IPersistentStorage() = default;

    virtual bool Empty() const = 0;

    // Enumerates the contents of the persistent storage in the allocation order
    virtual void Visit(const TVisitor& visitor) = 0;

    /**
     * Gets the maximum possible buffer size that can be allocated.
     * Alloc is guaranteed to succeed for any size <=
     * MaxSupportedAllocationByteCount when the storage is empty.
     */
    virtual ui64 GetMaxSupportedAllocationByteCount() const = 0;

    /**
     * Allocates a buffer of the given size.
     *
     * On successful allocation, calls the writer, which should fill the buffer,
     * and returns a pointer to the buffer in persistent storage.
     * The returned pointer may differ from the pointer passed to the writer.
     *
     * Returns nullptr if there is not enough free space in the storage.
     */
    virtual const void* Alloc(const TAllocationWriter& writer, size_t size) = 0;

    // Frees a previously allocated buffer.
    virtual void Free(const void* ptr) = 0;

    virtual TPersistentStorageStats GetStats() const = 0;
};

using IPersistentStoragePtr = std::shared_ptr<IPersistentStorage>;

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStorageConfig
{
    TString FilePath;
    ui64 DataCapacity = 0;
    ui64 MetadataCapacity = 0;
    bool EnableChecksumValidation = false;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IPersistentStoragePtr> CreateFileRingBufferPersistentStorage(
    IPersistentStorageStatsPtr stats,
    TPersistentStorageConfig config);

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
