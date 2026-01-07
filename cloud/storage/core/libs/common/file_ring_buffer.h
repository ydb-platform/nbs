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
    TStringBuf Front() const;
    TStringBuf Back() const;
    void PopFront();
    ui64 Size() const;
    bool Empty() const;
    TVector<TBrokenFileEntry> Validate();
    void Visit(const TVisitor& visitor);
    bool IsCorrupted() const;
    ui64 GetRawCapacity() const;
    ui64 GetRawUsedBytesCount() const;
    // Returns the maximum data size that is guaranteed to be successfully
    // allocated by PushBack. Returns zero if the buffer is corrupted.
    ui64 GetMaxAllocationBytesCount() const;

    bool ValidateMetadata() const;
    TStringBuf GetMetadata() const;
    bool SetMetadata(TStringBuf data);
};

}   // namespace NCloud
