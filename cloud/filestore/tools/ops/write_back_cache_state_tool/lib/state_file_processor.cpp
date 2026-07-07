#include "state_file_processor.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_format.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/string/printf.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TSerializedWriteDataRequestHeader
{
    ui64 NodeId = 0;
    ui64 Handle = 0;
    ui64 Offset = 0;
};

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

NCloud::NProto::TError ValidateHeaderPatch(
    const NProto::TStateFileHeader& curHeader,
    const NProto::TStateFileHeader& newHeader)
{
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

    // ReadPos, WritePos, MetadataChecksum - allowed to change

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
    const TFileRingBufferCapabilities& capabilities)
{
    if (curState.GetChecksum() != newState.GetChecksum()) {
        return MakeInvalidStateError(Sprintf(
            "State file checksum mismatch (cur: %u, new: %u)",
            curState.GetChecksum(),
            newState.GetChecksum()));
    }

    auto validateHeaderPatchResult =
        ValidateHeaderPatch(curState.GetHeader(), newState.GetHeader());

    if (HasError(validateHeaderPatchResult)) {
        return validateHeaderPatchResult;
    }

    if (curState.GetEntries().size() != newState.GetEntries().size()) {
        return MakeInvalidStateError(Sprintf(
            "Entry count mismatch (cur: %d, new: %d)",
            curState.GetEntries().size(),
            newState.GetEntries().size()));
    }

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TStateFileDump TStateFileProcessor::DumpStateFile(
    TFileRingBufferAccessor& accessor)
{
    NProto::TStateFileDump res;

    auto rawData = accessor.GetRawData();
    auto validationResult = accessor.ValidateAndInitialize();

    res.SetChecksum(Crc32c(rawData.data(), rawData.size()));
    res.SetIsCorrupted(HasError(validationResult));

    auto* header = accessor.GetHeader();
    if (header != nullptr) {
        FillHeader(*res.MutableHeader(), *header);
    }

    auto* dataProcessor = accessor.GetDataProcessor();
    if (dataProcessor == nullptr) {
        return res;
    }

    auto pos = header->ReadPos;
    while (pos > header->WritePos) {
        auto eh = dataProcessor->ReadEntryHeader(pos);
        if (eh.DataSize == 0) {
            pos = 0;
            break;
        }

        FillEntryInfo(
            *res.AddEntries(),
            eh,
            pos,
            dataProcessor->GetEntryDataPtr(pos, eh.DataSize));

        pos += dataProcessor->GetEntrySize(eh.DataSize);
    }

    while (pos < header->WritePos) {
        auto eh = dataProcessor->ReadEntryHeader(pos);
        if (eh.DataSize == 0) {
            pos = 0;
            break;
        }

        FillEntryInfo(
            *res.AddEntries(),
            eh,
            pos,
            dataProcessor->GetEntryDataPtr(pos, eh.DataSize));

        pos += dataProcessor->GetEntrySize(eh.DataSize);
    }

    return res;
}

NCloud::NProto::TError TStateFileProcessor::PatchStateFile(
    TFileRingBufferAccessor& accessor,
    const NProto::TStateFileDump& newState)
{
    if (!accessor.IsInitialized()) {
        return MakeInvalidStateError(
            "State file is not initialized, nothing to patch");
    }

    auto curState = DumpStateFile(accessor);

    auto validatePatchResult = ValidatePatch(
        curState,
        newState,
        accessor.GetDataProcessor()->GetCapabilities(false));

    if (HasError(validatePatchResult)) {
        return validatePatchResult;
    }

    // Patch header
    auto* header = accessor.GetHeader();
    const auto& newHeader = newState.GetHeader();

    header->ReadPos = newHeader.GetReadPos();
    header->WritePos = newHeader.GetWritePos();
    header->MetadataChecksum = newHeader.GetMetadataChecksum();

    // Patch entries
    for (int i = 0; i < curState.GetEntries().size(); ++i) {
        ApplyEntryPatch(newState.GetEntries(i), *accessor.GetDataProcessor());
    }

    return {};
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
