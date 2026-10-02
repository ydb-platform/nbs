#include "state_file_processor.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_data_request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_format.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/ylimits.h>
#include <util/string/printf.h>

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
    const TFileRingBufferEntryHeader& eh,
    ui64 pos,
    const char* dataPtr)
{
    entry.SetDataSize(eh.DataSize);
    entry.SetDataChecksum(eh.DataChecksum);
    entry.SetTag(eh.Tag);
    entry.SetFreeFlag(eh.FreeFlag);
    entry.SetActualDataChecksum(
        dataPtr != nullptr && !eh.FreeFlag ? Crc32c(dataPtr, eh.DataSize) : 0);
    entry.SetEntryPos(pos);

    if (dataPtr != nullptr &&
        sizeof(TSerializedWriteDataRequestHeader) < eh.DataSize)
    {
        // Fill old data contents also for entries with free flag
        const auto* header =
            reinterpret_cast<const TSerializedWriteDataRequestHeader*>(dataPtr);

        auto& requestInfo = *entry.MutableWriteDataRequestInfo();
        requestInfo.SetNodeId(header->NodeId);
        requestInfo.SetHandle(header->Handle);
        requestInfo.SetOffset(header->Offset);
        requestInfo.SetSize(
            eh.DataSize - sizeof(TSerializedWriteDataRequestHeader));
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

bool IsKnownEntryBoundary(
    const NProto::TStateFileDump& curState,
    ui64 pos,
    const IFileRingBufferDataProcessor& dataProcessor)
{
    if (pos == 0 || pos == curState.GetHeader().GetWritePos()) {
        return true;
    }

    for (const auto& entry: curState.GetEntries()) {
        if (pos == entry.GetEntryPos()) {
            return true;
        }
    }

    if (!curState.GetEntries().empty()) {
        const auto& lastEntry =
            curState.GetEntries(curState.GetEntries().size() - 1);
        const ui64 lastEntrySize =
            dataProcessor.GetEntrySize(lastEntry.GetDataSize());

        if (lastEntry.GetEntryPos() <= Max<ui64>() - lastEntrySize &&
            pos == lastEntry.GetEntryPos() + lastEntrySize)
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

    const auto dataCapacity = curState.GetHeader().GetDataCapacity();
    if (newPos > dataCapacity) {
        return MakeArgumentError(Sprintf(
            "%s %lu exceeds data capacity %lu",
            fieldName.data(),
            newPos,
            dataCapacity));
    }

    const auto capabilities = dataProcessor.GetCapabilities(false);
    if (capabilities.Alignment > 1 &&
        newPos % capabilities.Alignment != 0)
    {
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

NCloud::NProto::TError ValidateHeaderPatch(
    const NProto::TStateFileDump& curState,
    const NProto::TStateFileDump& newState,
    const IFileRingBufferDataProcessor& dataProcessor)
{
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

    auto error = ValidateHeaderPositionPatch(
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

    TFileRingBufferValidator validator(/* validateChecksums = */ false);

    error = validator.ValidateData(
        dataProcessor,
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
        newHeader.GetMetadataChecksum() !=
            curState.GetActualMetadataChecksum())
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

NCloud::NProto::TError ValidateEntryPatch(
    const NProto::TStateFileEntry& curState,
    const NProto::TStateFileEntry& newState,
    const TFileRingBufferCapabilities& capabilities)
{
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

    if (newState.HasWriteDataRequestInfo()) {
        if (!curState.HasWriteDataRequestInfo()) {
            return MakeArgumentError(Sprintf(
                "Changing request data for entry at pos %lu is not possible "
                "because it is not present in the current state",
                curState.GetEntryPos()));
        }

        const auto& curData = curState.GetWriteDataRequestInfo();
        const auto& newData = newState.GetWriteDataRequestInfo();

        if (newData.GetNodeId() != curData.GetNodeId()) {
            entryUpdateRequested = true;
        }

        if (newData.GetHandle() != curData.GetHandle()) {
            entryUpdateRequested = true;
        }

        if (newData.GetOffset() != curData.GetOffset()) {
            entryUpdateRequested = true;
        }

        if (newData.GetSize() != curData.GetSize()) {
            return MakeArgumentError(Sprintf(
                "Changing request size for entry at pos %lu is not allowed "
                "(cur: %u, new: %u)",
                curState.GetEntryPos(),
                curData.GetSize(),
                newData.GetSize()));
        }
    }

    if (entryUpdateRequested &&
        (curState.GetActualDataChecksum() != newState.GetDataChecksum()))
    {
        return MakeArgumentError(Sprintf(
            "Changing entry header or data at pos %lu is not allowed because "
            "of checksum mismatch (actual: %u, new: %u)",
            curState.GetEntryPos(),
            curState.GetActualDataChecksum(),
            newState.GetDataChecksum()));
    }

    return {};
}

void ApplyEntryPatch(
    const NProto::TStateFileEntry& entryState,
    IFileRingBufferDataProcessor& dataProcessor)
{
    auto dataChecksum = entryState.GetDataChecksum();

    if (entryState.GetActualDataChecksum() != entryState.GetDataChecksum()) {
        auto entryHeader =
            dataProcessor.ReadEntryHeader(entryState.GetEntryPos());
        entryHeader.DataChecksum = entryState.GetDataChecksum();
        dataProcessor.WriteEntryHeader(entryState.GetEntryPos(), entryHeader);
        return;
    }

    char* dataPtr = dataProcessor.GetEntryDataPtr(
        entryState.GetEntryPos(),
        entryState.GetDataSize());

    if (dataPtr != nullptr &&
        sizeof(TSerializedWriteDataRequestHeader) < entryState.GetDataSize() &&
        entryState.HasWriteDataRequestInfo())
    {
        auto* writeDataRequestHeader =
            reinterpret_cast<TSerializedWriteDataRequestHeader*>(dataPtr);

        const auto& newRequestInfo = entryState.GetWriteDataRequestInfo();

        writeDataRequestHeader->NodeId = newRequestInfo.GetNodeId();
        writeDataRequestHeader->Handle = newRequestInfo.GetHandle();
        writeDataRequestHeader->Offset = newRequestInfo.GetOffset();
    }

    dataChecksum = dataPtr != nullptr && !entryState.GetFreeFlag()
                            ? Crc32c(dataPtr, entryState.GetDataSize())
                            : 0;

    TFileRingBufferEntryHeader entryHeader{
        .DataSize = entryState.GetDataSize(),
        .DataChecksum = dataChecksum,
        .Tag = entryState.GetTag(),
        .FreeFlag = entryState.GetFreeFlag()};

    dataProcessor.WriteEntryHeader(entryState.GetEntryPos(), entryHeader);
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

    auto validateHeaderPatchResult =
        ValidateHeaderPatch(curState, newState, dataProcessor);

    if (HasError(validateHeaderPatchResult)) {
        return validateHeaderPatchResult;
    }

    if (curState.GetEntries().size() != newState.GetEntries().size()) {
        return MakeInvalidStateError(Sprintf(
            "Entry count mismatch (cur: %d, new: %d)",
            curState.GetEntries().size(),
            newState.GetEntries().size()));
    }

    const auto capabilities = dataProcessor.GetCapabilities(false);

    for (int i = 0; i < curState.GetEntries().size(); ++i) {
        auto validateEntryPatchResult = ValidateEntryPatch(
            curState.GetEntries(i),
            newState.GetEntries(i),
            capabilities);

        if (HasError(validateEntryPatchResult)) {
            return validateEntryPatchResult;
        }
    }

    return {};
}

bool DumpEntry(
    NProto::TStateFileDump& state,
    IFileRingBufferDataProcessor& dataProcessor,
    ui64& pos)
{
    const auto eh = dataProcessor.ReadEntryHeader(pos);
    if (eh.DataSize == 0) {
        return false;
    }

    FillEntryInfo(
        *state.AddEntries(),
        eh,
        pos,
        dataProcessor.GetEntryDataPtr(pos, eh.DataSize));

    pos += dataProcessor.GetEntrySize(eh.DataSize);
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TStateFileDump TStateFileProcessor::DumpStateFile(
    TFileRingBufferAccessor& accessor)
{
    NProto::TStateFileDump res;

    auto rawData = accessor.GetRawData();
    auto validationResult = accessor.ValidateAndInitialize();

    res.SetChecksum(Crc32c(rawData.data(), rawData.size()));
    res.SetIsCorrupted(
        validationResult == EFileRingBufferAccessorValidationStatus::Failed);

    auto* header = accessor.GetHeader();
    if (header != nullptr) {
        FillHeader(*res.MutableHeader(), *header);

        const auto rawMetadata = accessor.GetRawMetadata();
        if (header->MetadataSize <= rawMetadata.size()) {
            const auto metadata = rawMetadata.subspan(0, header->MetadataSize);
            res.SetActualMetadataChecksum(
                Crc32c(metadata.data(), metadata.size()));
        }
    }

    auto* dataProcessor = accessor.GetDataProcessor();
    if (dataProcessor == nullptr) {
        return res;
    }

    const auto readPos = header->ReadPos;
    auto pos = readPos;
    while (pos > header->WritePos) {
        if (!DumpEntry(res, *dataProcessor, pos)) {
            // A slack marker at ReadPos is invalid unless WritePos == 0. Do
            // not wrap in that case: doing so would report unrelated entries
            // at the start of a corrupt buffer as if they were live.
            if (pos != readPos) {
                pos = 0;
            }
            break;
        }
    }

    while (pos < header->WritePos) {
        if (!DumpEntry(res, *dataProcessor, pos)) {
            // A slack marker is not valid in the second, low-address segment.
            break;
        }
    }

    return res;
}

NCloud::NProto::TError TStateFileProcessor::PatchStateFile(
    TFileRingBufferAccessor& accessor,
    const NProto::TStateFileDump& newState)
{
    if (accessor.GetHeader() == nullptr ||
        accessor.GetDataProcessor() == nullptr)
    {
        return MakeInvalidStateError(
            "State file is not initialized, nothing to patch");
    }

    auto curState = DumpStateFile(accessor);

    auto validatePatchResult = ValidatePatch(
        curState,
        newState,
        *accessor.GetDataProcessor());

    if (HasError(validatePatchResult)) {
        return validatePatchResult;
    }

    // Persist entry data and entry headers before making the new ring-buffer
    // boundaries visible. If the second flush is interrupted, any visible
    // entry has already reached persistent storage.
    for (int i = 0; i < curState.GetEntries().size(); ++i) {
        ApplyEntryPatch(newState.GetEntries(i), *accessor.GetDataProcessor());
    }

    auto flushError = accessor.Flush();
    if (HasError(flushError)) {
        return flushError;
    }

    auto* header = accessor.GetHeader();
    const auto& newHeader = newState.GetHeader();

    header->ReadPos = newHeader.GetReadPos();
    header->WritePos = newHeader.GetWritePos();
    header->MetadataChecksum = newHeader.GetMetadataChecksum();

    return accessor.Flush();
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
