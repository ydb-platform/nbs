#pragma once

#include <util/system/types.h>

#include <memory>
#include <span>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

//  Structure contract:
//
//  1. Empty buffer:
//    - ReadPos == WritePos
//  or
//    - ReadPos points to a slack space
//    - WritePos == 0
//    This may happen when Alloc was terminated in the middle of the operation.
//    ReadPos is corrected to 0 during validation.
//
//  2. Non-empty buffer:
//    - ReadPos != WritePos
//    - ReadPos points to the first byte of the first valid entry
//    - WritePos points to the first byte right after the last valid entry
//
//  3. Valid entry:
//    - Size > 0
//    - The entry takes sizeof(TEntryHeader) + Size contiguous bytes in
//      the occupied part of the buffer
//
//  4. Occupied part of the buffer:
//     - ReadPos < WritePos: [ReadPos, WritePos)
//     - ReadPos > WritePos: [ReadPos, Capacity) + [0, WritePos)
//
//  5. Slack space entry marker:
//     - Size = 0
//     - Instructs to read the next entry at pos = 0
//     - Can appear only when ReadPos > WritePos in [ReadPos, Capacity) part
//
//  6. Implicit slack space marker:
//     - pos + sizeof(TEntryHeader) > Capacity

////////////////////////////////////////////////////////////////////////////////

enum class EFileRingBufferVersion : ui32
{
    NotInitialized = 0,

    // Entry headers are not aligned
    // Maximum allocation size is 2^31-1
    // Free flag is supported
    // Tags are not supported
    // Checksum is calculated over entry data only
    V4 = 4,

    // Entry headers are not aligned
    // Maximum allocation size is 2^28-1
    // Free flag is supported
    // Tags are supported - small values [0-7]
    // Checksum is calculated over entry data only
    V5 = 5,

    // Entry headers are aligned and read/written atomically
    // Maximum allocation size is 2^28-1
    // Free flag is supported
    // Tags are supported - small values [0-7]
    // Checksum is calculated over entry data and XORed with entry header hash
    V6 = 6,
};

////////////////////////////////////////////////////////////////////////////////

struct TFileRingBufferHeader
{
    EFileRingBufferVersion Version = EFileRingBufferVersion::NotInitialized;
    ui32 HeaderSize = 0;
    ui64 DataCapacity = 0;
    ui64 ReadPos = 0;
    ui64 WritePos = 0;
    ui64 Unused = 0;
    ui64 DataOffset = 0;
    ui64 MetadataCapacity = 0;
    ui64 MetadataOffset = 0;
    ui32 MetadataSize = 0;
    ui32 MetadataChecksum = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TFileRingBufferEntryHeader
{
    ui32 DataSize = 0;
    ui32 DataChecksum = 0;
    ui32 Tag = 0;
    bool FreeFlag = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TFileRingBufferCapabilities
{
    ui64 MaxAllocationByteCount = 0;
    ui64 MaxTag = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IFileRingBufferDataProcessor
{
    virtual ~IFileRingBufferDataProcessor() = default;

    virtual TFileRingBufferCapabilities GetCapabilities() const = 0;

    /**
     * Read entry header at the specified position
     * If position lies outside data region, returns the default value
     */
    virtual TFileRingBufferEntryHeader ReadEntryHeader(ui64 pos) const = 0;

    /**
     * Write entry header at the specified position
     * Returns true if header has been written or false if position lies
     * outside data region
     */
    virtual bool WriteEntryHeader(
        ui64 pos,
        const TFileRingBufferEntryHeader& header) = 0;

    /**
     * Get total entry size including header and data
     * Takes alignment into account
     */
    virtual ui64 GetEntrySize(ui64 dataSize) const = 0;

    /**
     * Get maximum amount of data that can be stored in an entry that
     * has size no more than maxEntrySize.
     */
    virtual ui64 GetMaxAllocationByteCount(ui64 maxEntrySize) const = 0;

    /**
     * Get data pointer to entry data at the specified position.
     * Returns nullptr if the requested range lays outside data mapping.
     */
    virtual const char* GetEntryDataPtr(ui64 pos, ui64 dataSize) const = 0;
    virtual char* GetEntryDataPtr(ui64 pos, ui64 dataSize) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IFileRingBufferDataProcessor> CreateFileRingBufferDataProcessor(
    EFileRingBufferVersion version,
    std::span<char> data);

}   // namespace NCloud
