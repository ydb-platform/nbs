#include "state_file_processor.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_data_request.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_format.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/utility.h>

#include <cstring>

namespace NCloud::NFileStore::NStateFileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillHeader(
    NProto::TStateFileHeader& protoHeader,
    const TFileRingBufferHeader& header)
{
    protoHeader.SetVersion(static_cast<ui32>(header.Version));
    protoHeader.SetHeaderSize(header.HeaderSize);
    protoHeader.SetDataCapacity(header.DataCapacity);
    protoHeader.SetReadPos(header.ReadPos);
    protoHeader.SetWritePos(header.WritePos);
    protoHeader.SetDataOffset(header.DataOffset);
    protoHeader.SetMetadataCapacity(header.MetadataCapacity);
    protoHeader.SetMetadataOffset(header.MetadataOffset);
    protoHeader.SetMetadataSize(header.MetadataSize);
    protoHeader.SetMetadataChecksum(header.MetadataChecksum);
}

void FillWriteDataRequestInfo(
    NProto::TStateFileEntry& entry,
    const char* dataPtr,
    ui32 dataSize)
{
    using NFuse::NWriteBackCache::TSerializedWriteDataRequestHeader;

    if (dataPtr == nullptr ||
        dataSize <= sizeof(TSerializedWriteDataRequestHeader))
    {
        return;
    }

    // Preserve request information for free entries as well: their stale
    // payload can still be useful while diagnosing or repairing a file.
    TSerializedWriteDataRequestHeader header;
    std::memcpy(&header, dataPtr, sizeof(header));

    auto& requestInfo = *entry.MutableWriteDataRequestInfo();
    requestInfo.SetNodeId(header.NodeId);
    requestInfo.SetHandle(header.Handle);
    requestInfo.SetOffset(header.Offset);
    requestInfo.SetSize(dataSize - sizeof(TSerializedWriteDataRequestHeader));
}

void FillEntryInfo(
    NProto::TStateFileEntry& entry,
    const TFileRingBufferEntryHeader& entryHeader,
    ui64 pos,
    const char* dataPtr,
    NProto::EStateFileType fileType)
{
    entry.SetDataSize(entryHeader.DataSize);
    entry.SetDataChecksum(entryHeader.DataChecksum);
    entry.SetTag(entryHeader.Tag);
    entry.SetFreeFlag(entryHeader.FreeFlag);

    if (dataPtr != nullptr && !entryHeader.FreeFlag) {
        entry.SetActualDataChecksum(Crc32c(dataPtr, entryHeader.DataSize));
    }
    entry.SetEntryPos(pos);

    switch (fileType) {
        case NProto::EStateFileType::WriteBackCache:
            FillWriteDataRequestInfo(entry, dataPtr, entryHeader.DataSize);
            break;
        default:
            break;
    }
}

enum class EDumpEntryResult
{
    Dumped,
    Slack,
    Invalid,
};

EDumpEntryResult DumpEntry(
    NProto::TStateFileDump& state,
    IFileRingBufferDataProcessor& dataProcessor,
    const TFileRingBufferCapabilities& capabilities,
    ui64& pos,
    NProto::EStateFileType fileType)
{
    const auto entryHeader = dataProcessor.ReadEntryHeader(pos);
    if (entryHeader.DataSize == 0) {
        if (IsSlackSpaceMarker(entryHeader, capabilities)) {
            return EDumpEntryResult::Slack;
        }

        FillEntryInfo(*state.AddEntries(), entryHeader, pos, nullptr, fileType);
        return EDumpEntryResult::Invalid;
    }

    const auto* dataPtr =
        dataProcessor.GetEntryDataPtr(pos, entryHeader.DataSize);
    FillEntryInfo(*state.AddEntries(), entryHeader, pos, dataPtr, fileType);

    if (dataPtr == nullptr) {
        return EDumpEntryResult::Invalid;
    }

    pos += dataProcessor.GetEntrySize(entryHeader.DataSize);
    return EDumpEntryResult::Dumped;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TStateFileDump TStateFileProcessor::DumpStateFile(
    TFileRingBufferAccessor& accessor,
    NProto::EStateFileType fileType)
{
    NProto::TStateFileDump result;

    const auto rawData = accessor.GetRawData();
    const auto validationResult = accessor.ValidateAndInitialize();

    // Detect changes since the dump so patching can reject a stale snapshot.
    // Cover the header, used metadata and data without reading unused sparse
    // capacity, which would commit tmpfs pages to RAM.
    const auto headerData =
        rawData.first(Min(rawData.size(), sizeof(TFileRingBufferHeader)));
    result.SetChecksum(Crc32c(headerData.data(), headerData.size()));

    result.SetIsCorrupted(
        validationResult == EFileRingBufferAccessorValidationStatus::Failed);

    if (result.GetIsCorrupted()) {
        result.SetValidationError(
            accessor.GetLastValidationError().GetMessage());
    }

    const auto* header = accessor.GetHeader();
    if (header != nullptr) {
        FillHeader(*result.MutableHeader(), *header);

        const auto rawMetadata = accessor.GetRawMetadata();
        if (header->MetadataSize <= rawMetadata.size()) {
            const auto metadata = rawMetadata.subspan(0, header->MetadataSize);
            const auto metadataChecksum =
                Crc32c(metadata.data(), metadata.size());
            result.SetActualMetadataChecksum(metadataChecksum);
            result.SetChecksum(Crc32cCombine(
                result.GetChecksum(),
                metadataChecksum,
                metadata.size()));
        }
    }

    auto* dataProcessor = accessor.GetDataProcessor();
    if (header == nullptr || dataProcessor == nullptr) {
        return result;
    }

    const auto readPos = header->ReadPos;
    const auto writePos = header->WritePos;

    if (readPos <= header->DataCapacity && writePos <= header->DataCapacity) {
        // For a wrapped buffer, assume the entire data area has already been
        // used and include it all in the snapshot checksum.
        ui64 begin = readPos <= writePos ? readPos : 0;
        ui64 end = readPos <= writePos ? writePos : header->DataCapacity;
        auto data = rawData.subspan(header->DataOffset + begin, end - begin);

        result.SetChecksum(Crc32cExtend(
            result.GetChecksum(),
            data.data(),
            data.size()));
    }

    const auto capabilities = dataProcessor->GetCapabilities(false);
    auto pos = readPos;
    while (pos > header->WritePos) {
        const auto entryResult = DumpEntry(
            result,
            *dataProcessor,
            capabilities,
            pos,
            fileType);

        if (entryResult != EDumpEntryResult::Dumped) {
            // A slack marker at ReadPos is invalid unless WritePos == 0. Do
            // not wrap in that case: doing so would report unrelated entries
            // at the start of a corrupt buffer as if they were live.
            if (entryResult == EDumpEntryResult::Slack && pos != readPos) {
                pos = 0;
            }
            break;
        }
    }

    while (pos < header->WritePos) {
        if (DumpEntry(
                result,
                *dataProcessor,
                capabilities,
                pos,
                fileType) != EDumpEntryResult::Dumped)
        {
            // A slack marker is not valid in the second, low-address segment.
            break;
        }
    }

    return result;
}

}   // namespace NCloud::NFileStore::NStateFileTool
