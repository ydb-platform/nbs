#include "state_file_processor.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_data_request.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_format.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <cstring>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

using namespace NFuse::NWriteBackCache;

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

void FillEntryInfo(
    NProto::TStateFileEntry& entry,
    const TFileRingBufferEntryHeader& entryHeader,
    ui64 pos,
    const char* dataPtr)
{
    entry.SetDataSize(entryHeader.DataSize);
    entry.SetDataChecksum(entryHeader.DataChecksum);
    entry.SetTag(entryHeader.Tag);
    entry.SetFreeFlag(entryHeader.FreeFlag);

    if (entryHeader.FreeFlag) {
        entry.SetActualDataChecksum(0);
    } else if (dataPtr != nullptr) {
        entry.SetActualDataChecksum(Crc32c(dataPtr, entryHeader.DataSize));
    }
    entry.SetEntryPos(pos);

    if (dataPtr != nullptr &&
        sizeof(TSerializedWriteDataRequestHeader) < entryHeader.DataSize)
    {
        // Preserve request information for free entries as well: their stale
        // payload can still be useful while diagnosing or repairing a file.
        TSerializedWriteDataRequestHeader header;
        std::memcpy(&header, dataPtr, sizeof(header));

        auto& requestInfo = *entry.MutableWriteDataRequestInfo();
        requestInfo.SetNodeId(header.NodeId);
        requestInfo.SetHandle(header.Handle);
        requestInfo.SetOffset(header.Offset);
        requestInfo.SetSize(
            entryHeader.DataSize - sizeof(TSerializedWriteDataRequestHeader));
    }
}

enum class EDumpEntryResult
{
    Dumped,
    Slack,
    Invalid,
};

bool IsSlackMarker(
    const TFileRingBufferEntryHeader& entryHeader,
    const TFileRingBufferCapabilities& capabilities)
{
    return entryHeader.DataSize == 0 && !entryHeader.FreeFlag &&
           entryHeader.Tag == 0 &&
           (!capabilities.EntryHeaderIsProcessedAtomically ||
            entryHeader.DataChecksum == 0);
}

EDumpEntryResult DumpEntry(
    NProto::TStateFileDump& state,
    IFileRingBufferDataProcessor& dataProcessor,
    const TFileRingBufferCapabilities& capabilities,
    ui64 dataCapacity,
    ui64& pos)
{
    const auto entryHeader = dataProcessor.ReadEntryHeader(pos);
    if (entryHeader.DataSize == 0) {
        if (IsSlackMarker(entryHeader, capabilities)) {
            return EDumpEntryResult::Slack;
        }

        FillEntryInfo(*state.AddEntries(), entryHeader, pos, nullptr);
        return EDumpEntryResult::Invalid;
    }

    const auto* dataPtr =
        dataProcessor.GetEntryDataPtr(pos, entryHeader.DataSize);
    FillEntryInfo(*state.AddEntries(), entryHeader, pos, dataPtr);

    const ui64 entrySize = dataProcessor.GetEntrySize(entryHeader.DataSize);
    if (dataPtr == nullptr || pos > dataCapacity ||
        entrySize > dataCapacity - pos)
    {
        return EDumpEntryResult::Invalid;
    }

    pos += entrySize;
    return EDumpEntryResult::Dumped;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TStateFileDump TStateFileProcessor::DumpStateFile(
    TFileRingBufferAccessor& accessor)
{
    NProto::TStateFileDump result;

    const auto rawData = accessor.GetRawData();
    const auto validationResult = accessor.ValidateAndInitialize();

    result.SetChecksum(Crc32c(rawData.data(), rawData.size()));
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
            result.SetActualMetadataChecksum(
                Crc32c(metadata.data(), metadata.size()));
        }
    }

    auto* dataProcessor = accessor.GetDataProcessor();
    if (header == nullptr || dataProcessor == nullptr) {
        return result;
    }

    const auto readPos = header->ReadPos;
    const auto capabilities = dataProcessor->GetCapabilities(false);
    auto pos = readPos;
    while (pos > header->WritePos) {
        const auto entryResult = DumpEntry(
            result,
            *dataProcessor,
            capabilities,
            header->DataCapacity,
            pos);

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
                header->DataCapacity,
                pos) != EDumpEntryResult::Dumped)
        {
            // A slack marker is not valid in the second, low-address segment.
            break;
        }
    }

    return result;
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
