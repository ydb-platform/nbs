#pragma once

#include "file_ring_buffer_format.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <util/generic/function_ref.h>
#include <util/system/filemap.h>

#include <optional>
#include <span>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class EFileRingBufferAccessorValidationMode
{
    // Header and DataProcessor will be initialized only after successful
    // validation
    Normal,

    // Header and DataProcessor will be initialized even if validation fails.
    // This mode is intended for repairing corrupted state.
    Debug
};

////////////////////////////////////////////////////////////////////////////////

enum class EFileRingBufferAccessorValidationStatus
{
    // State file is either empty or contains zeroed data:
    // - Header will be accessible if file length >= header size;
    // - DataProcessor, RawMetadata and Capabilities will not be initialized.
    NotInitialized,

    // Successful validation of both header and data:
    // - Header will be accessible;
    // - DataProcessor, RawMetadata and Capabilities will be initialized.
    Success,

    // Validation failed.
    // When EFileRingBufferAccessorValidationMode == Normal:
    // - Header will not be accessible;
    // - DataProcessor, RawMetadata and Capabilities will not be initialized.
    // When EFileRingBufferAccessorValidationMode == Debug:
    // - Header will be accessible if file length >= header size
    // - DataProcessor, RawMetadata, Capabilities will be initialized if header
    //   is successfully validated (but data may be corrupted)
    Failed
};

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferValidator
{
private:
    bool ValidateChecksums = false;

public:
    explicit TFileRingBufferValidator(bool validateChecksums);

    /**
     * Validates whether readPos and writePos describe a structurally valid
     * traversal of dataProcessor. Additionals performs payload checksum
     * validation if ValidateChecksums is set.
     */
    NProto::TError ValidateData(
        const IFileRingBufferDataProcessor& dataProcessor,
        ui64 readPos,
        ui64 writePos) const;

private:
    TResultOrError<TFileRingBufferEntryHeader> ReadAndValidateEntry(
        const IFileRingBufferDataProcessor& dataProcessor,
        ui64 pos,
        const TFileRingBufferCapabilities& capabilities) const;
};

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferAccessor
{
private:
    const EFileRingBufferAccessorValidationMode ValidationMode;

    std::span<char> RawData;
    TFileRingBufferHeader* Header = nullptr;
    std::unique_ptr<IFileRingBufferDataProcessor> DataProcessor;
    std::span<char> RawMetadata;
    TFileRingBufferCapabilities Capabilities = {};
    NProto::TError LastValidationError;

public:
    explicit TFileRingBufferAccessor(
        EFileRingBufferAccessorValidationMode validationMode);

    virtual ~TFileRingBufferAccessor() = default;

    // Flushes changes made through RawData, Header, RawMetadata or
    // DataProcessor to persistent storage. Implementations must complete the
    // flush synchronously and report any observable error.
    virtual NProto::TError Flush() = 0;

    // Validates raw data and initializes Header, DataProcessor, RawMetadata and
    // Capabilities depending on the validation result and mode
    EFileRingBufferAccessorValidationStatus ValidateAndInitialize();

    const NProto::TError& GetLastValidationError() const
    {
        return LastValidationError;
    }

    std::span<char> GetRawData()
    {
        return RawData;
    }

    std::span<const char> GetRawData() const
    {
        return RawData;
    }

    std::span<char> GetRawData(ui64 offset, ui64 byteCount);

    TFileRingBufferHeader* GetHeader()
    {
        return Header;
    }

    const TFileRingBufferHeader* GetHeader() const
    {
        return Header;
    }

    IFileRingBufferDataProcessor* GetDataProcessor()
    {
        return DataProcessor.get();
    }

    const IFileRingBufferDataProcessor* GetDataProcessor() const
    {
        return DataProcessor.get();
    }

    std::span<char> GetRawMetadata()
    {
        return RawMetadata;
    }

    std::span<const char> GetRawMetadata() const
    {
        return RawMetadata;
    }

    const TFileRingBufferCapabilities& GetCapabilities() const
    {
        return Capabilities;
    }

protected:
    // Updating raw data invalidates memory references.
    // The object must be re-initialized by calling ValidateAndInitialize()
    void UpdateRawData(std::span<char> rawData);

private:
    EFileRingBufferAccessorValidationStatus DoValidateAndInitialize();

    void ResetValidationState();
};

////////////////////////////////////////////////////////////////////////////////

class TFileMapFileRingBufferAccessor final: public TFileRingBufferAccessor
{
private:
    const TString FileName;
    const TMemoryMapCommon::EOpenModeFlag OpenModeFlags;
    const std::optional<TFile> BackingFile;

    std::optional<TFileMap> FileMap;

public:
    TFileMapFileRingBufferAccessor(
        TString fileName,
        EFileRingBufferAccessorValidationMode validationMode,
        TMemoryMapCommon::EOpenModeFlag openModeFlags);

    // Maps this exact open file description and never reopens its pathname
    TFileMapFileRingBufferAccessor(
        TFile file,
        EFileRingBufferAccessorValidationMode validationMode,
        TMemoryMapCommon::EOpenModeFlag openModeFlags);

    NProto::TError Map();
    NProto::TError ResizeAndRemap(size_t newSize);
    NProto::TError Flush() override;
    void Close();

private:
    void CreateFileMap();
    NProto::TError ProcessMap();
};

}   // namespace NCloud
