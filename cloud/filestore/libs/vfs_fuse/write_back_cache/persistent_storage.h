#pragma once

#include "persistent_storage_stats.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/logger/log.h>

#include <util/generic/function_ref.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// Non-thread safe
struct IPersistentStorage
{
    using TVisitor = TFunctionRef<void(ui32 tag, TStringBuf buffer)>;
    using TAllocationWriter = TFunctionRef<void(char* ptr, size_t size)>;

    virtual ~IPersistentStorage() = default;

    virtual bool Empty() const = 0;
    virtual bool IsCorrupted() const = 0;

    /**
     * Enumerates the contents of the persistent storage in the allocation order
     * Stops visiting when corruption is detected.
     */
    [[nodiscard]] virtual NProto::TError Visit(const TVisitor& visitor) = 0;

    /**
     * Returns the number of bytes that can be successfully allocated by Alloc
     * for an empty buffer without exceeding the capacity.
     *
     * Returns zero if the buffer is corrupted.
     *
     * Note: the purpose of this method is to provide a guarantee that an
     * allocation of this size will eventually succeed. Allocations of higher
     * sizes will fail with an error.
     */
    virtual ui64 GetMaxSupportedAllocationByteCount() const = 0;

    /**
     * Allocates a buffer of the given size.
     *
     * On successful allocation, returns a pointer to the buffer in persistent
     * storage. The caller should fill the buffer and call Commit.
     *
     * On failure, returns nullptr if the buffer is full or an error if
     * allocation is not possible due to corruption or invalid argument.
     */
    [[nodiscard]] virtual TResultOrError<char*> Alloc(size_t size) = 0;

    /**
     * Completes the previously made allocation by calculating checksum and
     * making the allocation visible.
     *
     * Once committed, it is not allowed to modify the contents of the allocated
     * entry. If there is a need to augment the allocation with additional data,
     * SetTag can be used.
     *
     * Memory that was allocated but not committed will be lost at buffer
     * recreation.
     *
     * An error is returned if there is no incomplete allocation corresponding
     * to the provided pointer or the buffer is corrupted.
     */
    [[nodiscard]] virtual NProto::TError Commit(const void* ptr) = 0;

    /**
     * Commits the previously allocated memory buffer but takes a checksum
     * provided by the caller instead of calculating it.
     *
     * Note: the checksum is not validated, the calling code has responsibility
     * to provide the correct Crc32c checksum. Passing an incorrect checksum may
     * lead to a corruption error.
     */
    [[nodiscard]] virtual NProto::TError Commit(
        const void* ptr,
        ui32 crc32c) = 0;

    /**
     * Frees a previously allocated and committed buffer.
     *
     * An error is returned if the pointer is invalid or the buffer is corrupted
     */
    [[nodiscard]] virtual NProto::TError Free(const void* ptr) = 0;

    /**
     * Sets the tag value associated with the allocation.
     *
     * Returns an error if the pointer is invalid, the tag value exceeds the
     * maximal supported value or if the buffer is corrupted.
     */
    [[nodiscard]] virtual NProto::TError SetTag(const void* ptr, ui32 tag) = 0;

    virtual void UpdateStats() const = 0;
};

using IPersistentStoragePtr = std::shared_ptr<IPersistentStorage>;

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStorageConfig
{
    TString FilePath;
    ui64 DataCapacity = 0;
    ui64 MetadataCapacity = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Errors are also reported as critical events in this implementation
IPersistentStoragePtr CreateFileRingBufferPersistentStorage(
    IPersistentStorageStatsPtr stats,
    TPersistentStorageConfig config,
    TLog log,
    TString logTag);

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
