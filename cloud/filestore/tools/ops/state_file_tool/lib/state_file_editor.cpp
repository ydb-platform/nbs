#include "state_file_editor.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_data_request.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_format.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/utility.h>
#include <util/generic/ylimits.h>
#include <util/string/printf.h>
#include <util/system/unaligned_mem.h>

#include <cstring>

namespace NCloud::NFileStore::NStateFileTool {

namespace {

using namespace NFuse::NWriteBackCache;

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

void FillWriteBackCacheEntryPayloadInfo(
    NProto::TStateFileEntry& entry,
    const char* dataPtr,
    ui32 dataSize)
{
    using NFuse::NWriteBackCache::HasWriteDataRequestPayload;
    using NFuse::NWriteBackCache::TSerializedWriteDataRequestHeader;

    if (dataPtr == nullptr || !HasWriteDataRequestPayload(dataSize)) {
        return;
    }

    // Preserve request information for free entries as well: their stale
    // payload can still be useful while diagnosing or repairing a file.
    auto header = ReadUnaligned<TSerializedWriteDataRequestHeader>(dataPtr);

    auto& requestMetadata = *entry.MutableWriteDataRequestMetadata();
    requestMetadata.SetNodeId(header.NodeId);
    requestMetadata.SetHandle(header.Handle);
    requestMetadata.SetOffset(header.Offset);
    requestMetadata.SetSize(
        dataSize - sizeof(TSerializedWriteDataRequestHeader));
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
            FillWriteBackCacheEntryPayloadInfo(
                entry,
                dataPtr,
                entryHeader.DataSize);
            break;
        default:
            break;
    }
}

NCloud::NProto::TError MakeArgumentError(TString message)
{
    return MakeError(E_ARGUMENT, std::move(message));
}

NCloud::NProto::TError MakeInvalidStateError(TString message)
{
    return MakeError(E_INVALID_STATE, std::move(message));
}

struct TEntryRange
{
    size_t Begin = 0;
    size_t End = 0;

    bool Contains(size_t index) const
    {
        return Begin <= index && index < End;
    }
};

// Consumer validation and overlap checks apply only to entries retained by
// the proposed ring-buffer boundaries.
TResultOrError<TEntryRange> FindRetainedEntryRange(
    const NProto::TStateFileDump& state,
    ui64 readPos,
    ui64 writePos,
    const IFileRingBufferDataProcessor& dataProcessor)
{
    if (readPos == writePos) {
        return TEntryRange{};
    }

    TEntryRange range;
    bool beginFound = false;
    bool endFound = false;
    const ui64 dataCapacity = state.GetHeader().GetDataCapacity();
    const size_t entryCount = state.GetEntries().size();

    for (size_t i = 0; i < entryCount; ++i) {
        const auto& entry = state.GetEntries(i);
        if (entry.GetEntryPos() == readPos) {
            range.Begin = i;
            beginFound = true;
        }
        if (entry.GetEntryPos() == writePos) {
            range.End = i;
            endFound = true;
        }

        const ui64 entrySize = dataProcessor.GetEntrySize(entry.GetDataSize());
        if (entry.GetEntryPos() <= dataCapacity &&
            entrySize <= dataCapacity - entry.GetEntryPos() &&
            entry.GetEntryPos() + entrySize == writePos)
        {
            range.End = i + 1;
            endFound = true;
        }
    }

    if (writePos == state.GetHeader().GetWritePos()) {
        range.End = entryCount;
        endFound = true;
    }

    if (!beginFound || !endFound || range.End <= range.Begin) {
        return MakeArgumentError(Sprintf(
            "ReadPos/WritePos pair (%lu, %lu) does not select a contiguous "
            "range of dumped entries",
            readPos,
            writePos));
    }

    return range;
}

bool IsKnownEntryBoundary(
    const NProto::TStateFileDump& curState,
    ui64 pos,
    const IFileRingBufferDataProcessor& dataProcessor)
{
    if (pos == curState.GetHeader().GetWritePos()) {
        return true;
    }

    const ui64 dataCapacity = curState.GetHeader().GetDataCapacity();
    for (const auto& entry: curState.GetEntries()) {
        if (pos == entry.GetEntryPos()) {
            return true;
        }

        const ui64 entrySize = dataProcessor.GetEntrySize(entry.GetDataSize());
        if (entry.GetEntryPos() <= dataCapacity &&
            entrySize <= dataCapacity - entry.GetEntryPos() &&
            pos == entry.GetEntryPos() + entrySize)
        {
            return true;
        }
    }

    return false;
}

NCloud::NProto::TError ValidateHeaderPositionPatch(
    TStringBuf fieldName,
    ui64 curPos,
    ui64 newPos,
    const NProto::TStateFileDump& curState,
    const IFileRingBufferDataProcessor& dataProcessor)
{
    if (curPos == newPos) {
        return {};
    }

    const ui64 dataCapacity = curState.GetHeader().GetDataCapacity();
    if (newPos > dataCapacity) {
        return MakeArgumentError(Sprintf(
            "%s %lu exceeds data capacity %lu",
            fieldName.data(),
            newPos,
            dataCapacity));
    }

    const auto capabilities = dataProcessor.GetCapabilities(false);
    if (capabilities.Alignment > 1 && newPos % capabilities.Alignment != 0) {
        return MakeArgumentError(Sprintf(
            "%s %lu is not aligned to %lu",
            fieldName.data(),
            newPos,
            capabilities.Alignment));
    }

    if (!IsKnownEntryBoundary(curState, newPos, dataProcessor)) {
        return MakeArgumentError(Sprintf(
            "%s %lu does not point to a known entry boundary",
            fieldName.data(),
            newPos));
    }

    return {};
}

NCloud::NProto::TError ValidateHeaderRangePatch(
    const NProto::TStateFileDump& curState,
    ui64 newReadPos,
    ui64 newWritePos,
    const IFileRingBufferDataProcessor& dataProcessor)
{
    const auto& curHeader = curState.GetHeader();
    if ((newReadPos == curHeader.GetReadPos() &&
         newWritePos == curHeader.GetWritePos()) ||
        newReadPos == newWritePos)
    {
        return {};
    }

    const auto range = FindRetainedEntryRange(
        curState,
        newReadPos,
        newWritePos,
        dataProcessor);

    if (HasError(range)) {
        return range.GetError();
    }

    return {};
}

NCloud::NProto::TError ValidateHeaderPatch(
    const NProto::TStateFileDump& curState,
    const NProto::TStateFileDump& newState,
    const IFileRingBufferDataProcessor& dataProcessor)
{
    if (!newState.HasHeader()) {
        return MakeArgumentError("Patch does not contain a state file header");
    }

    const auto& curHeader = curState.GetHeader();
    const auto& newHeader = newState.GetHeader();

    const char* messagePattern =
        "Changing header field %s is not allowed (cur: %lu, new: %lu)";

    if (curHeader.GetVersion() != newHeader.GetVersion()) {
        return MakeArgumentError(Sprintf(
            messagePattern,
            "Version",
            static_cast<ui64>(curHeader.GetVersion()),
            static_cast<ui64>(newHeader.GetVersion())));
    }

    if (curHeader.GetHeaderSize() != newHeader.GetHeaderSize()) {
        return MakeArgumentError(Sprintf(
            messagePattern,
            "HeaderSize",
            static_cast<ui64>(curHeader.GetHeaderSize()),
            static_cast<ui64>(newHeader.GetHeaderSize())));
    }

    if (curHeader.GetDataCapacity() != newHeader.GetDataCapacity()) {
        return MakeArgumentError(Sprintf(
            messagePattern,
            "DataCapacity",
            curHeader.GetDataCapacity(),
            newHeader.GetDataCapacity()));
    }

    if (curHeader.GetDataOffset() != newHeader.GetDataOffset()) {
        return MakeArgumentError(Sprintf(
            messagePattern,
            "DataOffset",
            curHeader.GetDataOffset(),
            newHeader.GetDataOffset()));
    }

    if (curHeader.GetMetadataCapacity() != newHeader.GetMetadataCapacity()) {
        return MakeArgumentError(Sprintf(
            messagePattern,
            "MetadataCapacity",
            curHeader.GetMetadataCapacity(),
            newHeader.GetMetadataCapacity()));
    }

    if (curHeader.GetMetadataOffset() != newHeader.GetMetadataOffset()) {
        return MakeArgumentError(Sprintf(
            messagePattern,
            "MetadataOffset",
            curHeader.GetMetadataOffset(),
            newHeader.GetMetadataOffset()));
    }

    if (curHeader.GetMetadataSize() != newHeader.GetMetadataSize()) {
        return MakeArgumentError(Sprintf(
            messagePattern,
            "MetadataSize",
            static_cast<ui64>(curHeader.GetMetadataSize()),
            static_cast<ui64>(newHeader.GetMetadataSize())));
    }

    NCloud::NProto::TError error;
    const bool clearBuffer =
        newHeader.GetReadPos() == 0 && newHeader.GetWritePos() == 0;
    if (!clearBuffer) {
        error = ValidateHeaderPositionPatch(
            "ReadPos",
            curHeader.GetReadPos(),
            newHeader.GetReadPos(),
            curState,
            dataProcessor);
        if (HasError(error)) {
            return error;
        }

        error = ValidateHeaderPositionPatch(
            "WritePos",
            curHeader.GetWritePos(),
            newHeader.GetWritePos(),
            curState,
            dataProcessor);
        if (HasError(error)) {
            return error;
        }

        error = ValidateHeaderRangePatch(
            curState,
            newHeader.GetReadPos(),
            newHeader.GetWritePos(),
            dataProcessor);
        if (HasError(error)) {
            return error;
        }
    }

    TFileRingBufferValidator validator(/* validateChecksums = */ false);
    error = validator.ValidateData(
        dataProcessor,
        curHeader.GetDataCapacity(),
        newHeader.GetReadPos(),
        newHeader.GetWritePos());

    if (HasError(error)) {
        return MakeArgumentError(Sprintf(
            "Invalid ReadPos/WritePos pair (%lu, %lu): %s",
            newHeader.GetReadPos(),
            newHeader.GetWritePos(),
            error.GetMessage().c_str()));
    }

    if (curHeader.GetMetadataChecksum() != newHeader.GetMetadataChecksum() &&
        newHeader.GetMetadataChecksum() != curState.GetActualMetadataChecksum())
    {
        return MakeArgumentError(Sprintf(
            "Changing MetadataChecksum is only allowed to the actual metadata "
            "checksum (actual: %u, new: %u)",
            curState.GetActualMetadataChecksum(),
            newHeader.GetMetadataChecksum()));
    }

    // ReadPos, WritePos and MetadataChecksum are allowed to change subject to
    // the checks above.
    return {};
}

bool HasWriteDataRequestPatch(
    const NProto::TStateFileEntry& curState,
    const NProto::TStateFileEntry& newState)
{
    if (!curState.HasWriteDataRequestMetadata() ||
        !newState.HasWriteDataRequestMetadata())
    {
        return false;
    }

    const auto& curData = curState.GetWriteDataRequestMetadata();
    const auto& newData = newState.GetWriteDataRequestMetadata();
    return curData.GetNodeId() != newData.GetNodeId() ||
           curData.GetHandle() != newData.GetHandle() ||
           curData.GetOffset() != newData.GetOffset();
}

bool HasEntryPatch(
    const NProto::TStateFileEntry& curState,
    const NProto::TStateFileEntry& newState)
{
    return curState.GetDataChecksum() != newState.GetDataChecksum() ||
           curState.GetTag() != newState.GetTag() ||
           curState.GetFreeFlag() != newState.GetFreeFlag() ||
           HasWriteDataRequestPatch(curState, newState);
}

NCloud::NProto::TError ValidateEntryPatch(
    const NProto::TStateFileEntry& curState,
    const NProto::TStateFileEntry& newState,
    const IFileRingBufferDataProcessor& dataProcessor)
{
    const auto capabilities = dataProcessor.GetCapabilities(false);

    if (curState.GetEntryPos() != newState.GetEntryPos()) {
        return MakeInvalidStateError(Sprintf(
            "Entry pos mismatch (cur: %lu, new: %lu)",
            curState.GetEntryPos(),
            newState.GetEntryPos()));
    }

    if (curState.GetActualDataChecksum() != newState.GetActualDataChecksum()) {
        return MakeInvalidStateError(Sprintf(
            "Entry ActualDataChecksum mismatch at pos %lu (cur: %u, new: %u)",
            curState.GetEntryPos(),
            curState.GetActualDataChecksum(),
            newState.GetActualDataChecksum()));
    }

    if (curState.GetDataSize() != newState.GetDataSize()) {
        return MakeArgumentError(Sprintf(
            "Changing entry size at pos %lu is not allowed (cur: %u, new: %u)",
            curState.GetEntryPos(),
            curState.GetDataSize(),
            newState.GetDataSize()));
    }

    bool entryUpdateRequested = false;

    if (curState.GetDataChecksum() != newState.GetDataChecksum()) {
        if (newState.GetDataChecksum() != curState.GetActualDataChecksum()) {
            return MakeArgumentError(Sprintf(
                "Changing entry checksum at pos %lu is only allowed to the "
                "actual data checksum (actual: %u, new: %u)",
                curState.GetEntryPos(),
                curState.GetActualDataChecksum(),
                newState.GetDataChecksum()));
        }
    }

    if (curState.GetTag() != newState.GetTag()) {
        if (newState.GetTag() > capabilities.MaxTag) {
            return MakeArgumentError(Sprintf(
                "Changing entry tag at pos %lu is not possible because it "
                "exceeds the maximal value (cur: %u, new: %u, max: %lu)",
                curState.GetEntryPos(),
                curState.GetTag(),
                newState.GetTag(),
                capabilities.MaxTag));
        }
        entryUpdateRequested = true;
    }

    if (curState.GetFreeFlag() != newState.GetFreeFlag()) {
        entryUpdateRequested = true;
    }

    if (curState.HasWriteDataRequestMetadata() !=
        newState.HasWriteDataRequestMetadata())
    {
        return MakeArgumentError(Sprintf(
            "Changing request data presence for entry at pos %lu is not "
            "allowed (cur: %s, new: %s)",
            curState.GetEntryPos(),
            curState.HasWriteDataRequestMetadata() ? "present" : "absent",
            newState.HasWriteDataRequestMetadata() ? "present" : "absent"));
    }

    if (newState.HasWriteDataRequestMetadata()) {
        const auto& curData = curState.GetWriteDataRequestMetadata();
        const auto& newData = newState.GetWriteDataRequestMetadata();
        if (newData.GetSize() != curData.GetSize()) {
            return MakeArgumentError(Sprintf(
                "Changing request size for entry at pos %lu is not allowed "
                "(cur: %u, new: %u)",
                curState.GetEntryPos(),
                curData.GetSize(),
                newData.GetSize()));
        }

        entryUpdateRequested |= HasWriteDataRequestPatch(curState, newState);
    }

    if (entryUpdateRequested &&
        curState.GetActualDataChecksum() != newState.GetDataChecksum())
    {
        return MakeArgumentError(Sprintf(
            "Changing entry header or data at pos %lu is not allowed because "
            "of checksum mismatch (actual: %u, new: %u)",
            curState.GetEntryPos(),
            curState.GetActualDataChecksum(),
            newState.GetDataChecksum()));
    }

    const bool checksumUpdateRequested =
        curState.GetDataChecksum() != newState.GetDataChecksum();
    if ((checksumUpdateRequested || entryUpdateRequested) &&
        dataProcessor.GetEntryDataPtr(
            curState.GetEntryPos(),
            curState.GetDataSize()) == nullptr)
    {
        return MakeInvalidStateError(Sprintf(
            "Entry data at pos %lu is not accessible",
            curState.GetEntryPos()));
    }

    return {};
}

bool RangesOverlap(
    ui64 firstBegin,
    ui64 firstEnd,
    ui64 secondBegin,
    ui64 secondEnd)
{
    return firstBegin < secondEnd && secondBegin < firstEnd;
}

NCloud::NProto::TError ValidateEntryPatchRanges(
    const NProto::TStateFileDump& curState,
    const NProto::TStateFileDump& newState,
    const IFileRingBufferDataProcessor& dataProcessor,
    const TEntryRange& retainedEntries)
{
    const ui64 entryHeaderSize = dataProcessor.GetEntrySize(0);
    const size_t entryCount = curState.GetEntries().size();

    // A discarded entry from a corrupted wrapped buffer may overlap a retained
    // entry, so repairing it must not overwrite the retained entry.
    for (size_t i = 0; i < entryCount; ++i) {
        const auto& curEntry = curState.GetEntries(i);
        const auto& newEntry = newState.GetEntries(i);
        if (!HasEntryPatch(curEntry, newEntry) || retainedEntries.Contains(i)) {
            continue;
        }

        ui64 writeSize = entryHeaderSize;
        if (HasWriteDataRequestPatch(curEntry, newEntry)) {
            writeSize += sizeof(TSerializedWriteDataRequestHeader);
        }

        const ui64 writeBegin = curEntry.GetEntryPos();
        const ui64 writeEnd = writeBegin + writeSize;

        for (size_t j = retainedEntries.Begin; j < retainedEntries.End; ++j) {
            const auto& retainedEntry = curState.GetEntries(j);
            const ui64 retainedBegin = retainedEntry.GetEntryPos();
            const ui64 retainedEnd =
                retainedBegin +
                dataProcessor.GetEntrySize(retainedEntry.GetDataSize());

            if (RangesOverlap(writeBegin, writeEnd, retainedBegin, retainedEnd))
            {
                return MakeArgumentError(Sprintf(
                    "Changing entry at pos %lu would overwrite retained entry "
                    "at pos %lu",
                    writeBegin,
                    retainedBegin));
            }
        }
    }

    return {};
}

NCloud::NProto::TError ValidatePatch(
    const NProto::TStateFileDump& curState,
    const NProto::TStateFileDump& newState,
    const IFileRingBufferDataProcessor& dataProcessor)
{
    if (curState.GetChecksum() != newState.GetChecksum()) {
        return MakeInvalidStateError(Sprintf(
            "State file checksum mismatch (cur: %u, new: %u)",
            curState.GetChecksum(),
            newState.GetChecksum()));
    }

    if (curState.GetActualMetadataChecksum() !=
        newState.GetActualMetadataChecksum())
    {
        return MakeInvalidStateError(Sprintf(
            "Actual metadata checksum mismatch (cur: %u, new: %u)",
            curState.GetActualMetadataChecksum(),
            newState.GetActualMetadataChecksum()));
    }

    auto error = ValidateHeaderPatch(curState, newState, dataProcessor);
    if (HasError(error)) {
        return error;
    }

    if (curState.GetEntries().size() != newState.GetEntries().size()) {
        return MakeInvalidStateError(Sprintf(
            "Entry count mismatch (cur: %d, new: %d)",
            curState.GetEntries().size(),
            newState.GetEntries().size()));
    }

    const auto retainedEntriesOrError = FindRetainedEntryRange(
        curState,
        newState.GetHeader().GetReadPos(),
        newState.GetHeader().GetWritePos(),
        dataProcessor);

    if (HasError(retainedEntriesOrError)) {
        return retainedEntriesOrError.GetError();
    }

    const auto& retainedEntries = retainedEntriesOrError.GetResult();
    const size_t entryCount = curState.GetEntries().size();

    for (size_t i = 0; i < entryCount; ++i) {
        const auto& newEntry = newState.GetEntries(i);
        error = ValidateEntryPatch(
            curState.GetEntries(i),
            newEntry,
            dataProcessor);
        if (HasError(error)) {
            return error;
        }

        if (retainedEntries.Contains(i) && !newEntry.GetFreeFlag() &&
            newEntry.HasWriteDataRequestMetadata())
        {
            const auto& request = newEntry.GetWriteDataRequestMetadata();
            if (request.GetOffset() > Max<ui64>() - request.GetSize()) {
                return MakeArgumentError(Sprintf(
                    "Request offset and size overflow for entry at pos %lu",
                    newEntry.GetEntryPos()));
            }
        }
    }

    return ValidateEntryPatchRanges(
        curState,
        newState,
        dataProcessor,
        retainedEntries);
}

NCloud::NProto::TError ApplyEntryPatch(
    const NProto::TStateFileEntry& curState,
    const NProto::TStateFileEntry& newState,
    IFileRingBufferDataProcessor& dataProcessor)
{
    const bool checksumChanged =
        curState.GetDataChecksum() != newState.GetDataChecksum();
    const bool headerChanged = curState.GetTag() != newState.GetTag() ||
                               curState.GetFreeFlag() != newState.GetFreeFlag();
    const bool requestChanged = HasWriteDataRequestPatch(curState, newState);

    if (!checksumChanged && !headerChanged && !requestChanged) {
        return {};
    }

    char* dataPtr = dataProcessor.GetEntryDataPtr(
        newState.GetEntryPos(),
        newState.GetDataSize());
    if (dataPtr == nullptr) {
        return MakeInvalidStateError(Sprintf(
            "Entry data at pos %lu is no longer accessible",
            newState.GetEntryPos()));
    }

    if (requestChanged) {
        TSerializedWriteDataRequestHeader requestHeader;
        std::memcpy(&requestHeader, dataPtr, sizeof(requestHeader));

        const auto& newRequestInfo = newState.GetWriteDataRequestMetadata();
        requestHeader.NodeId = newRequestInfo.GetNodeId();
        requestHeader.Handle = newRequestInfo.GetHandle();
        requestHeader.Offset = newRequestInfo.GetOffset();

        std::memcpy(dataPtr, &requestHeader, sizeof(requestHeader));
    }

    ui32 dataChecksum = newState.GetDataChecksum();
    if (headerChanged || requestChanged) {
        dataChecksum = newState.GetFreeFlag()
                           ? 0
                           : Crc32c(dataPtr, newState.GetDataSize());
    }

    const TFileRingBufferEntryHeader entryHeader{
        .DataSize = newState.GetDataSize(),
        .DataChecksum = dataChecksum,
        .Tag = newState.GetTag(),
        .FreeFlag = newState.GetFreeFlag()};

    if (!dataProcessor.WriteEntryHeader(newState.GetEntryPos(), entryHeader)) {
        return MakeInvalidStateError(Sprintf(
            "Entry header at pos %lu is no longer writable",
            newState.GetEntryPos()));
    }

    return {};
}

enum class EDumpEntryResult
{
    Dumped,
    Slack,
    Invalid,
};

EDumpEntryResult DumpEntry(
    NProto::TStateFileDump& state,
    const IFileRingBufferDataProcessor& dataProcessor,
    const TFileRingBufferCapabilities& capabilities,
    ui64& pos,
    NProto::EStateFileType fileType)
{
    const auto entryHeader = dataProcessor.ReadEntryHeader(pos);
    if (IsSlackSpaceMarker(entryHeader, capabilities)) {
        return EDumpEntryResult::Slack;
    }

    const auto* dataPtr =
        entryHeader.DataSize
            ? dataProcessor.GetEntryDataPtr(pos, entryHeader.DataSize)
            : nullptr;

    FillEntryInfo(*state.AddEntries(), entryHeader, pos, dataPtr, fileType);

    if (dataPtr == nullptr) {
        return EDumpEntryResult::Invalid;
    }

    pos += dataProcessor.GetEntrySize(entryHeader.DataSize);
    return EDumpEntryResult::Dumped;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TStateFileDump TStateFileEditor::DumpStateFile(
    TFileRingBufferAccessor& accessor,
    NProto::EStateFileType fileType)
{
    NProto::TStateFileDump result;

    const auto rawData = accessor.GetRawData();
    const auto validationResult = accessor.ValidateAndInitialize();

    // The Checksum field detects file changes between dumping and patching.
    // Checksum calculation skips unused sparse capacity to avoid committing
    // tmpfs pages to RAM.
    const auto headerData =
        rawData.first(Min(rawData.size(), sizeof(TFileRingBufferHeader)));
    result.SetChecksum(Crc32c(headerData.data(), headerData.size()));

    result.SetIsCorrupted(
        validationResult == EFileRingBufferAccessorValidationStatus::Failed);

    if (validationResult ==
        EFileRingBufferAccessorValidationStatus::NotInitialized)
    {
        return result;
    }

    if (result.GetIsCorrupted()) {
        result.SetValidationErrorMessage(
            accessor.GetLastValidationError().GetMessage());
    }

    const auto* headerPtr = accessor.GetHeader();
    if (headerPtr == nullptr) {
        return result;
    }

    const TFileRingBufferHeader header = *headerPtr;
    FillHeader(*result.MutableHeader(), header);

    auto* dataProcessor = accessor.GetDataProcessor();
    if (dataProcessor == nullptr) {
        // Even in Debug mode, no data processor is created when the header
        // version or layout is invalid (e.g. the file is truncated).
        return result;
    }

    const auto metadata =
        accessor.GetRawData(header.MetadataOffset, header.MetadataSize);
    const auto metadataChecksum = Crc32c(metadata.data(), metadata.size());
    result.SetActualMetadataChecksum(metadataChecksum);
    result.SetChecksum(Crc32cCombine(
        result.GetChecksum(),
        metadataChecksum,
        metadata.size()));

    const auto readPos = header.ReadPos;
    const auto writePos = header.WritePos;

    if (readPos <= header.DataCapacity && writePos <= header.DataCapacity) {
        // For a wrapped buffer, assume the entire data area has already been
        // used and include it all in the snapshot checksum.
        ui64 begin = readPos <= writePos ? readPos : 0;
        ui64 end = readPos <= writePos ? writePos : header.DataCapacity;
        auto data = accessor.GetRawData(header.DataOffset + begin, end - begin);

        result.SetChecksum(Crc32cExtend(
            result.GetChecksum(),
            data.data(),
            data.size()));
    }

    const auto capabilities = dataProcessor->GetCapabilities(false);
    auto pos = readPos;
    while (pos > writePos) {
        const auto entryResult =
            DumpEntry(result, *dataProcessor, capabilities, pos, fileType);

        if (entryResult == EDumpEntryResult::Slack) {
            // A slack marker at ReadPos means empty-file normalization or
            // corruption. In either case, there are no more entries to dump.
            if (pos != readPos) {
                pos = 0;
            } else {
                return result;
            }
        }

        if (entryResult != EDumpEntryResult::Dumped) {
            break;
        }
    }

    while (pos < writePos) {
        const auto entryResult =
            DumpEntry(result, *dataProcessor, capabilities, pos, fileType);

        if (entryResult != EDumpEntryResult::Dumped) {
            // A slack marker is not valid in the second, low-address segment.
            break;
        }
    }

    return result;
}

NCloud::NProto::TError TStateFileEditor::PatchStateFile(
    TFileRingBufferAccessor& accessor,
    const NProto::TStateFileDump& newState)
{
    auto curState = DumpStateFile(accessor, NProto::EStateFileType::WriteBackCache);

    auto* header = accessor.GetHeader();
    auto* dataProcessor = accessor.GetDataProcessor();
    if (header == nullptr) {
        return MakeInvalidStateError(
            "State file is not initialized, nothing to patch");
    }

    std::unique_ptr<IFileRingBufferDataProcessor> clearDataProcessor;
    const bool clearBuffer =
        newState.HasHeader() && newState.GetHeader().GetReadPos() == 0 &&
        newState.GetHeader().GetWritePos() == 0;

    if (dataProcessor == nullptr && clearBuffer) {
        const auto rawData = accessor.GetRawData();
        auto error = TFileRingBufferValidator::ValidateHeaderLayout(
            *header,
            rawData.size());

        if (HasError(error)) {
            return MakeInvalidStateError(error.GetMessage());
        }

        clearDataProcessor = CreateFileRingBufferDataProcessor(
            header->Version,
            rawData.subspan(header->DataOffset, header->DataCapacity));
        dataProcessor = clearDataProcessor.get();
    }

    if (dataProcessor == nullptr) {
        return MakeInvalidStateError(
            "State file is not initialized, nothing to patch");
    }

    auto error = ValidatePatch(curState, newState, *dataProcessor);
    if (HasError(error)) {
        return error;
    }

    for (int i = 0; i < curState.GetEntries().size(); ++i) {
        error = ApplyEntryPatch(
            curState.GetEntries(i),
            newState.GetEntries(i),
            *dataProcessor);
        if (HasError(error)) {
            return error;
        }
    }

    const auto& newHeader = newState.GetHeader();
    header->ReadPos = newHeader.GetReadPos();
    header->WritePos = newHeader.GetWritePos();
    header->MetadataChecksum = newHeader.GetMetadataChecksum();

    return {};
}

}   // namespace NCloud::NFileStore::NStateFileTool
