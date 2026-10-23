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
    // Reserve space for new header fields without requiring data migration
    static constexpr ui64 ReserveSize = 256;

    EFileRingBufferVersion Version = EFileRingBufferVersion::NotInitialized;
    ui32 HeaderSize = 0;
    ui64 DataCapacity = 0;
    ui64 ReadPos = 0;
    ui64 WritePos = 0;
    // Previously was: LastEntrySize
    ui64 Unused = 0;
    ui64 DataOffset = 0;
    ui64 MetadataCapacity = 0;
    ui64 MetadataOffset = 0;
    ui32 MetadataSize = 0;
    ui32 MetadataChecksum = 0;
};

static_assert(
    sizeof(TFileRingBufferHeader) <= TFileRingBufferHeader::ReserveSize);

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
    ui64 Alignment = 0;
    bool EntryHeaderIsProcessedAtomically = false;
};

////////////////////////////////////////////////////////////////////////////////

// Canonical state file layout:
// [reserved header area][metadata][alignment padding][data]
//
// Metadata starts right after the reserved header area;
// Data starts right after the metadata area and is aligned to sizeof(ui64) -
//   this is needed to read and write entry headers atomically.
//
// During metadata resizing, data and/or metadata may temporarily be stored
// farther into the file. This is valid but not canonical; use the offsets from
// TFileRingBufferHeader to access it.
struct TFileRingBufferLayout
{
    ui64 DataOffset = 0;
    ui64 MetadataOffset = 0;
    ui64 StateFileSize = 0;

    // Returns the default layout if calculating offsets or file size overflows
    static TFileRingBufferLayout FromCapacity(
        ui64 dataCapacity,
        ui64 metadataCapacity);
};

////////////////////////////////////////////////////////////////////////////////

struct IFileRingBufferDataProcessor
{
    virtual ~IFileRingBufferDataProcessor() = default;

    /**
     * Returns maximal values that can be stored in TFileRingBufferEntryHeader
     * and alignment requirements for entry headers and data.
     */
    virtual TFileRingBufferCapabilities GetCapabilities(
        bool takeBufferSizeIntoAccount) const = 0;

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

bool IsSupportedFileRingBufferVersion(EFileRingBufferVersion version);

}   // namespace NCloud
