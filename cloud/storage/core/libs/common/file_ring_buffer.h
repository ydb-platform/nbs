#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TFileRingBuffer
{
public:
    struct TBrokenFileEntry
    {
        TString Data;
        ui32 ExpectedChecksum = 0;
        ui32 ActualChecksum = 0;
    };

    using TVisitor = std::function<void(ui32 checksum, TStringBuf entry)>;

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
    */
    TFileRingBuffer(
        const TString& filePath,
        ui64 dataCapacity,
        ui64 metadataCapacity = 0);

    ~TFileRingBuffer();

public:
    bool PushBack(TStringBuf data);

    // Implement in-place allocation.
    // Returns a pointer to the allocated memory or nullptr if allocation failed
    // Commit should be called after writing data to the allocated memory
    char* Alloc(size_t size);

    // Calculate checksum for the previously allocated memory using Alloc
    // and advance the write pointer
    void Commit();

    TStringBuf Front() const;
    TStringBuf Back() const;
    void PopFront();
    ui64 Size() const;
    bool Empty() const;
    TVector<TBrokenFileEntry> Validate();
    void Visit(const TVisitor& visitor);
    bool IsCorrupted() const;
    void SetCorrupted();
    ui64 GetRawCapacity() const;
    ui64 GetRawUsedBytesCount() const;

    // Returns the number of bytes that can be allocated in the buffer without
    // exceeding its capacity (PushBack will succeed).
    // Returns zero if the buffer is full or corrupted.
    ui64 GetAvailableByteCount() const;

    // Returns the maximal number of bytes that can be allocated in the buffer
    // (PushBack will succeed for an empty buffer).
    // Returns zero if the buffer is corrupted.
    ui64 GetMaxSupportedAllocationByteCount() const;

    bool ValidateMetadata() const;
    TStringBuf GetMetadata() const;
    bool SetMetadata(TStringBuf data);
};

}   // namespace NCloud
