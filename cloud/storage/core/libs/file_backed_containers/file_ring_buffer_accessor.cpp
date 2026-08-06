#include "file_ring_buffer_accessor.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/algorithm.h>
#include <util/string/printf.h>
#include <util/system/align.h>

namespace NCloud {

using EValidationMode = EFileRingBufferAccessorValidationMode;
using EValidationStatus = EFileRingBufferAccessorValidationStatus;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeError(TString message)
{
    return MakeError(E_FAIL, std::move(message));
}

NProto::TError ValidateHeader(
    const TFileRingBufferHeader& header,
    size_t rawDataSize)
{
    if (!IsSupportedFileRingBufferVersion(header.Version)) {
        return MakeError(Sprintf(
            "Unsupported file ring buffer version %u",
            static_cast<ui32>(header.Version)));
    }

    if (header.HeaderSize != sizeof(TFileRingBufferHeader)) {
        return MakeError(Sprintf(
            "Invalid file ring buffer header size %u (expected %lu)",
            header.HeaderSize,
            sizeof(TFileRingBufferHeader)));
    }

    if (header.MetadataOffset < header.HeaderSize) {
        return MakeError(Sprintf(
            "Invalid file ring buffer metadata offset %lu (expected >= %lu)",
            header.MetadataOffset,
            sizeof(TFileRingBufferHeader)));
    }

    if (header.MetadataOffset % sizeof(ui64) != 0) {
        // Note: metadata offset alignment is enforced regardless of whether
        // entries are aligned
        return MakeError(Sprintf(
            "Invalid file ring buffer metadata offset %lu (expected to be "
            "aligned to %lu)",
            header.MetadataOffset,
            sizeof(ui64)));
    }

    if (header.MetadataSize > header.MetadataCapacity) {
        return MakeError(Sprintf(
            "Invalid file ring buffer metadata size %u (expected <= %lu)",
            header.MetadataSize,
            header.MetadataCapacity));
    }

    if (header.DataOffset < header.MetadataOffset) {
        return MakeError(Sprintf(
            "Invalid file ring buffer data offset %lu (expected >= %lu)",
            header.DataOffset,
            header.MetadataOffset));
    }

    if (rawDataSize < header.DataOffset) {
        return MakeError(Sprintf(
            "Invalid file ring buffer data offset %lu (expected <= %lu)",
            header.DataOffset,
            rawDataSize));
    }

    if (header.DataOffset % sizeof(ui64) != 0) {
        // Note: data offset alignment is enforced regardless of whether entries
        // are aligned
        return MakeError(Sprintf(
            "Invalid file ring buffer data offset %lu (expected to be aligned "
            "to %lu)",
            header.DataOffset,
            sizeof(ui64)));
    }

    if (header.MetadataCapacity > header.DataOffset - header.MetadataOffset) {
        return MakeError(Sprintf(
            "Invalid file ring buffer metadata capacity %lu (expected <= %lu)",
            header.MetadataCapacity,
            header.DataOffset - header.MetadataOffset));
    }

    if (header.DataCapacity > rawDataSize - header.DataOffset) {
        return MakeError(Sprintf(
            "Invalid file ring buffer data capacity %lu (expected <= %lu)",
            header.DataCapacity,
            rawDataSize - header.DataOffset));
    }

    if (header.ReadPos > header.DataCapacity) {
        return MakeError(Sprintf(
            "Invalid file ring buffer read position %lu (expected <= %lu)",
            header.ReadPos,
            header.DataCapacity));
    }

    if (header.WritePos > header.DataCapacity) {
        return MakeError(Sprintf(
            "Invalid file ring buffer write position %lu (expected <= %lu)",
            header.WritePos,
            header.DataCapacity));
    }

    return {};
}

TResultOrError<TFileRingBufferEntryHeader> ReadAndValidateEntry(
    IFileRingBufferDataProcessor& dataProcessor,
    ui64 pos,
    const TFileRingBufferCapabilities& capabilities)
{
    if (capabilities.Alignment > 0 && pos % capabilities.Alignment != 0) {
        return MakeError(Sprintf(
            "Invalid file ring buffer entry header position %lu (expected to "
            "be aligned to %lu)",
            pos,
            capabilities.Alignment));
    }

    auto eh = dataProcessor.ReadEntryHeader(pos);

    if (eh.DataSize == 0) {
        if (eh.FreeFlag) {
            return MakeError(Sprintf(
                "Invalid file ring buffer entry header at position %lu "
                "(data size is zero and free flag is set)",
                pos));
        }

        if (eh.Tag != 0) {
            return MakeError(Sprintf(
                "Invalid file ring buffer entry header at position %lu "
                "(data size is zero and tag is non-zero)",
                pos));
        }

        if (capabilities.EntryHeaderIsProcessedAtomically &&
            eh.DataChecksum != 0)
        {
            return MakeError(Sprintf(
                "Invalid file ring buffer entry header at position %lu "
                "(data size is zero and data checksum is non-zero)",
                pos));
        }
    } else {
        const char* payload = dataProcessor.GetEntryDataPtr(pos, eh.DataSize);
        if (payload == nullptr) {
            return MakeError(Sprintf(
                "Invalid file ring buffer entry header at position %lu "
                "(data size %u exceeds data capacity)",
                pos,
                eh.DataSize));
        }

        if (eh.FreeFlag) {
            if (capabilities.EntryHeaderIsProcessedAtomically &&
                eh.DataChecksum != 0)
            {
                return MakeError(Sprintf(
                    "Invalid file ring buffer entry header at position %lu "
                    "(free flag is set and data checksum is non-zero)",
                    pos));
            }
        } else {
            auto actualCrc = Crc32c(payload, eh.DataSize);
            if (actualCrc != eh.DataChecksum) {
                return MakeError(Sprintf(
                    "Checksum mismatch for entry at position %lu "
                    "(header crc %u, actual data crc %u)",
                    pos,
                    eh.DataChecksum,
                    actualCrc));
            }
        }
    }
    return eh;
}

NProto::TError ValidateData(
    IFileRingBufferDataProcessor& dataProcessor,
    ui64 readPos,
    ui64 writePos)
{
    auto capabilities = dataProcessor.GetCapabilities(false);

    if (capabilities.Alignment > 0) {
        if (readPos % capabilities.Alignment != 0) {
            return MakeError(Sprintf(
                "Invalid file ring buffer read position %lu (expected to be "
                "aligned to %lu)",
                readPos,
                capabilities.Alignment));
        }

        if (writePos % capabilities.Alignment != 0) {
            return MakeError(Sprintf(
                "Invalid file ring buffer write position %lu (expected to be "
                "aligned to %lu)",
                writePos,
                capabilities.Alignment));
        }
    }

    // For an empty buffer, one of the following conditions should be met:
    // - readPos == writePos;
    // - writePos == 0 and readPos points to a slack space.
    //
    // For a non-empty buffer, all the following conditions should be met:
    // - readPos points to the first entry;
    // - writePos points to the next byte after the last entry.

    if (readPos == writePos) {
        // Empty buffer
        return {};
    }

    if (writePos == 0) {
        // Valid only when readPos points to a slack space
        // This may happen if Alloc was interrupted
        auto ehResultOrError =
            ReadAndValidateEntry(dataProcessor, readPos, capabilities);

        if (HasError(ehResultOrError)) {
            return ehResultOrError.GetError();
        }

        const auto& eh = ehResultOrError.GetResult();
        if (eh.DataSize == 0) {
            return {};
        }

        return MakeError(Sprintf(
            "Invalid file ring buffer read position %lu (expected to point to "
            "a slack space when WritePos == 0)",
            readPos));
    }

    auto pos = readPos;

    while (pos > writePos) {
        auto ehResultOrError =
            ReadAndValidateEntry(dataProcessor, pos, capabilities);

        if (HasError(ehResultOrError)) {
            return ehResultOrError.GetError();
        }

        const auto& eh = ehResultOrError.GetResult();

        if (eh.DataSize != 0) {
            pos += dataProcessor.GetEntrySize(eh.DataSize);
        } else if (pos != readPos) {
            pos = 0;
        } else {
            return MakeError(Sprintf(
                "Invalid file ring buffer read position %lu (expected to point "
                "to an entry when ReadPos != WritePos and WritePos != 0)",
                pos));
        }
    }

    while (pos < writePos) {
        auto ehResultOrError =
            ReadAndValidateEntry(dataProcessor, pos, capabilities);

        if (HasError(ehResultOrError)) {
            return ehResultOrError.GetError();
        }

        const auto& eh = ehResultOrError.GetResult();

        if (eh.DataSize == 0) {
            return MakeError(Sprintf(
                "Invalid file ring buffer entry header at read position %lu "
                "(unexpected slack space marker)",
                pos));
        }

        pos += dataProcessor.GetEntrySize(eh.DataSize);
    }

    if (pos > writePos) {
        return MakeError(Sprintf(
            "Last entry ends at position %lu (expected to be = write position "
            "%lu)",
            pos,
            writePos));
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFileRingBufferAccessor::TFileRingBufferAccessor(
    EFileRingBufferAccessorValidationMode mode)
    : ValidationMode(mode)
{}

std::span<char> TFileRingBufferAccessor::GetRawData(ui64 offset, ui64 byteCount)
{
    Y_ABORT_UNLESS(offset <= RawData.size());
    Y_ABORT_UNLESS(byteCount <= RawData.size() - offset);

    return RawData.subspan(offset, byteCount);
}

EValidationStatus TFileRingBufferAccessor::ValidateAndInitialize()
{
    auto status = DoValidateAndInitialize();

    if (status == EValidationStatus::Failed &&
        ValidationMode != EValidationMode::Debug)
    {
        // Prevent from accessing internal structures on validation failure
        auto lastError = std::move(LastValidationError);
        ResetValidationState();
        LastValidationError = std::move(lastError);
    }

    return status;
}

void TFileRingBufferAccessor::UpdateRawData(std::span<char> rawData)
{
    RawData = rawData;
    ResetValidationState();
}

EValidationStatus TFileRingBufferAccessor::DoValidateAndInitialize()
{
    ResetValidationState();

    if (RawData.empty()) {
        LastValidationError = MakeError("File is empty");
        return EValidationStatus::NotInitialized;
    }

    if (AlignDown(RawData.data(), sizeof(ui64)) != RawData.data()) {
        // Memory mapping is done in multiples of the page size, which is a
        // multiple of 8 bytes on all supported platforms.
        // Therefore, the raw data size should be aligned to 8 bytes.
        LastValidationError =
            MakeError("Buffer is not aligned to 8 bytes");
        return EValidationStatus::Failed;
    }

    if (RawData.size() < sizeof(TFileRingBufferHeader)) {
        LastValidationError = MakeError(Sprintf(
            "File is too small (%lu bytes) to contain a valid header",
            RawData.size()));
        return EValidationStatus::Failed;
    }

    Header = reinterpret_cast<TFileRingBufferHeader*>(RawData.data());

    const bool allZeros = AllOf(RawData, [](char c) { return c == 0; });
    if (allZeros) {
        LastValidationError = MakeError("File is not initialized");
        return EValidationStatus::NotInitialized;
    }

    LastValidationError = ValidateHeader(*Header, RawData.size());
    if (HasError(LastValidationError)) {
        return EValidationStatus::Failed;
    }

    RawMetadata = GetRawData(Header->MetadataOffset, Header->MetadataCapacity);

    DataProcessor = CreateFileRingBufferDataProcessor(
        Header->Version,
        GetRawData(Header->DataOffset, Header->DataCapacity));

    Capabilities = DataProcessor->GetCapabilities(true);

    LastValidationError =
        ValidateData(*DataProcessor, Header->ReadPos, Header->WritePos);

    if (HasError(LastValidationError)) {
        return EValidationStatus::Failed;
    }

    auto metadata = RawMetadata.subspan(0, Header->MetadataSize);

    auto actualMetadataCrc = Crc32c(metadata.data(), metadata.size());
    if (actualMetadataCrc != Header->MetadataChecksum) {
        LastValidationError = MakeError(Sprintf(
            "Checksum mismatch for metadata "
            "(header crc %u, actual data crc %u)",
            Header->MetadataChecksum,
            actualMetadataCrc));
        return EValidationStatus::Failed;
    }

    return EValidationStatus::Success;
}

void TFileRingBufferAccessor::ResetValidationState()
{
    Header = nullptr;
    DataProcessor.reset();
    RawMetadata = {};
    Capabilities = {};
    LastValidationError = {};
}

////////////////////////////////////////////////////////////////////////////////

TFileMapFileRingBufferAccessor::TFileMapFileRingBufferAccessor(
    TString fileName,
    EFileRingBufferAccessorValidationMode validationMode,
    TMemoryMapCommon::EOpenModeFlag openModeFlags)
    : TFileRingBufferAccessor(validationMode)
    , FileName(std::move(fileName))
    , OpenModeFlags(openModeFlags)
{}

NProto::TError TFileMapFileRingBufferAccessor::Map()
{
    try {
        if (!FileMap) {
            FileMap.emplace(FileName, OpenModeFlags);
        }
        FileMap->Map(0, FileMap->Length());
    } catch (...) {
        UpdateRawData({});
        return MakeError(
            E_IO,
            Sprintf(
                "Failed to map file %s: %s",
                FileName.c_str(),
                CurrentExceptionMessage().c_str()));
    }

    return ProcessMap();
}

NProto::TError TFileMapFileRingBufferAccessor::ResizeAndRemap(size_t newSize)
{
    try {
        if (!FileMap) {
            FileMap.emplace(FileName, OpenModeFlags);
        }
        FileMap->ResizeAndRemap(0, newSize);
    } catch (...) {
        UpdateRawData({});
        return MakeError(
            E_IO,
            Sprintf(
                "Failed to map file %s: %s",
                FileName.c_str(),
                CurrentExceptionMessage().c_str()));
    }

    return ProcessMap();
}

void TFileMapFileRingBufferAccessor::Close()
{
    UpdateRawData({});
    FileMap.reset();
}

NProto::TError TFileMapFileRingBufferAccessor::ProcessMap()
{
    auto fileSize = static_cast<ui64>(FileMap->Length());

    auto rawData = std::span<char>(
        static_cast<char*>(FileMap->Ptr()),
        FileMap->MappedSize());

    if (rawData.size() != fileSize) {
        UpdateRawData({});
        return MakeError(
            E_IO,
            Sprintf(
                "Failed to map the entire file (fileSize=%lu, mappedSize=%zu)",
                fileSize,
                rawData.size()));
    }

    UpdateRawData(rawData);

    return {};
}

}   // namespace NCloud
