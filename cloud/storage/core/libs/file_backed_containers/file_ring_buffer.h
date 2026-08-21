#pragma once

#include "file_ring_buffer_format.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/strbuf.h>

#include <functional>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Non-thread safe
class TFileRingBuffer
{
public:
    using TVisitor =
        std::function<void(ui32 checksum, ui32 tag, TStringBuf entry)>;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    /** Creates or opens an existing file ring buffer stored in the file.
    *
    * Argument dataCapacity specifies the size of the data area in bytes, it
    * has effect only when creating a new buffer. When opening an existing
    * buffer, the argument is ignored and the existing data capacity is used.
    *
    * Argument metadataCapacity specifies the size of the metadata area in
    * bytes. If the existing buffer has different metadata capacity, the
    * metadata area is resized to the specified capacity, preserving existing
    * metadata. If the size of the existing metadata is greater than the
    * specified capacity, the metadata area is shrunk to fit the existing
    * metadata.
    *
    * Argument version specifies the version of the file ring buffer format.
    * It affects capabilities (like storing tags, enforcing checksum calculation
    * for headers etc).
    */
    TFileRingBuffer(
        const TString& filePath,
        ui64 dataCapacity,
        ui64 metadataCapacity,
        EFileRingBufferVersion version);

    ~TFileRingBuffer();

    /**
     * Allocates a new entry in the buffer and copies the data into it.
     *
     * On success, return true. The entry becomes visible immediately.
     *
     * On failure, return false if the buffer is full or an error if
     * allocation is not possible due to corruption or invalid argument.
     *
     * Note: only one allocation is possible at a time. Calling PushBack while
     * an allocation made by Alloc is not committed will return an error.
     */
    TResultOrError<bool> PushBack(TStringBuf data);

    /**
     * In-place allocation of a memory block of the given size in the buffer.
     *
     * On success, returns a pointer to the allocated memory. The caller should
     * fill the allocated memory with data and then commit it. Non-committed
     * allocations are not visible and will be lost on buffer recreation.
     *
     * On failure, returns nullptr if the buffer is full or an error if
     * allocation is not possible due to corruption or invalid argument.
     *
     * Note: only one allocation is possible at a time. Repeated Alloc will
     * return an error.
     */
    TResultOrError<char*> Alloc(size_t size);

    /**
     * Completes the previously made allocation by calculating checksum and
     * making the allocation visible.
     *
     * Once committed, it is not allowed to modify the contents of the allocated
     * entry. If there is a need to augment the allocation with additional data,
     * GetTag/SetTag can be used.
     *
     * An error is returned if there is no incomplete allocation or the buffer
     * is corrupted.
     */
    NProto::TError Commit();

    /**
     * Frees a memory block that was previously allocated and committed.
     * It is possible to free memory blocks in any order.
     *
     * An error is returned if the pointer is invalid or the buffer is corrupted
     *
     * Implementation details: if the allocation is in the front of the ring
     * buffer, it is immediately freed. Otherwise, a FreeFlag is set for the
     * allocation and it will be freed when it reaches the front of the buffer.
     */
    NProto::TError Free(const void* ptr);

    /**
     * Gets the maximum tag value supported by the buffer.
     * Version 5 and later support tags in the range [0-7].
     * Earlier versions do not support tags.
     */
    ui32 GetMaxTag() const;

    /**
     * Gets the tag value associated with the allocation.
     *
     * Returns tag value on success.
     * Returns an error if the pointer is invalid or if the buffer is corrupted.
     */
    TResultOrError<ui32> GetTag(const void* ptr) const;

    /**
     * Sets the tag value associated with the allocation.
     *
     * Returns an error if the pointer is invalid, the tag value exceeds the
     * value returned by GetMaxTag() or if the buffer is corrupted.
     */
    NProto::TError SetTag(const void* ptr, ui32 tag);

    /**
     * Gets the front allocation of the buffer.
     *
     * Returns the contents of the front allocation on success.
     * Returns empty string if the buffer is empty.
     * Returns an error if storage is corrupted.
     */
    TResultOrError<TStringBuf> Front();

    /**
     * Frees the front allocation.
     *
     * Returns true on success.
     * Returns false if the buffer is empty.
     * Returns an error if the buffer is corrupted.
     */
    TResultOrError<bool> PopFront();

    /**
     * Returns the number of visible allocations in the buffer.
     * The behavior is unspecified if the buffer is corrupted.
     */
    ui64 Size() const;

    /**
     * Checks if the buffer is empty.
     * The behavior is unspecified if the buffer is corrupted.
     */
    bool Empty() const;

    /**
     * Check data and structure integrity including data checksum validation.
     * Set IsCorrupted flag if any issues are found and returns false.
     * Return true if everything is valid.
     * Fire a critical event and doesn't visit entries if a buffer is corrupted.
     */
    bool Validate();

    /**
     * Calls the visitor for each visible allocation in the buffer in the
     * allocation order.
     *
     * Stops visiting and returns an error if the buffer is corrupted.
     */
    NProto::TError Visit(const TVisitor& visitor);

    // This method is thread safe
    bool IsCorrupted() const;

    /**
     * Sets Corrupted flag and fires a critical event if the flag has not
     * been previously set. Unsetting the flag is not possible.
     *
     * All further operations on the buffer will fail once the flag is set.
     *
     * This method is thread safe
     */
    void SetCorrupted();

    ui64 GetRawCapacity() const;
    ui64 GetRawUsedBytesCount() const;
    ui32 GetVersion() const;
    ui64 GetMaxObservedEntryByteCount() const;

    /**
     * Returns the number of bytes that can be successfully allocated by
     * PushBack and Alloc right now without exceeding the capacity.
     *
     * Returns zero if the buffer is full or corrupted.
     */
    ui64 GetAvailableByteCount() const;

    /**
     * Returns the number of bytes that can be successfully allocated by
     * PushBack and Alloc for an empty buffer without exceeding the capacity.
     *
     * Returns zero if the buffer is corrupted.
     *
     * Note: the purpose of this method is to provide a guarantee that an
     * allocation of this size will eventually succeed. Allocations of higher
     * sizes will fail with an error.
     */
    ui64 GetMaxSupportedAllocationByteCount() const;

    /**
     * Gets metadata
     *
     * Returns the metadata contents on success
     * Returns an error if the buffer is corrupted
     */
    TResultOrError<TStringBuf> GetMetadata() const;

    /**
     * Sets metadata
     *
     * Returns true on success
     * Returns false if the argument exceeds the metadata capacity
     * Returns an error if the buffer is corrupted
     */
    TResultOrError<bool> SetMetadata(TStringBuf data);
};

}   // namespace NCloud
