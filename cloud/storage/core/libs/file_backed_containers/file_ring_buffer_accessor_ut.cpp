#include "file_ring_buffer.h"

#include "file_ring_buffer_accessor.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>
#include <util/system/tempfile.h>

namespace NCloud {

using EValidationStatus = EFileRingBufferAccessorValidationStatus;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILE_RING_BUFFER_TEST(name)                                            \
    void TestImpl##name(EFileRingBufferVersion ver);                           \
    Y_UNIT_TEST(name##V5)                                                      \
    {                                                                          \
        TestImpl##name(EFileRingBufferVersion::V5);                            \
    }                                                                          \
    Y_UNIT_TEST(name##V6)                                                      \
    {                                                                          \
        TestImpl##name(EFileRingBufferVersion::V6);                            \
    }                                                                          \
    void TestImpl##name(EFileRingBufferVersion ver)   // FILE_RING_BUFFER_TEST

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    const bool Debug;

    ui64 DataCapacity = 64;
    ui64 MetadataCapacity = 8;

    TTempFileHandle TempFileHandle;
    TFileMapFileRingBufferAccessor Accessor;
    std::span<char> RawData;

    explicit TBootstrap(bool debug = false)
        : Debug(debug)
        , Accessor(
              TempFileHandle.Name(),
              debug ? EFileRingBufferAccessorValidationMode::Debug
                    : EFileRingBufferAccessorValidationMode::Normal,
              TMemoryMapCommon::EOpenModeFlag::oRdWr)
    {
        Remap();
    }

    void Execute(
        const TFunctionRef<void(TFileRingBuffer&)>& fn,
        EFileRingBufferVersion version)
    {
        {
            TFileRingBuffer rb(
                TempFileHandle.Name(),
                DataCapacity,
                MetadataCapacity,
                version);

            fn(rb);
        }

        // TFileRingBuffer may resize the file
        // TFileMap does not update its mapping automatically - need to reopen
        Accessor.Close();
        Remap();
    }

    void AssertValidateSuccess()
    {
        UNIT_ASSERT_VALUES_EQUAL_C(
            EValidationStatus::Success,
            Accessor.ValidateAndInitialize(),
            FormatError(Accessor.GetLastValidationError()));
        UNIT_ASSERT(!HasError(Accessor.GetLastValidationError()));
        UNIT_ASSERT(Accessor.GetHeader() != nullptr);
        UNIT_ASSERT(Accessor.GetDataProcessor() != nullptr);
    }

    void AssertValidateFailed(
        const TString& expectedErrorSubstring,
        EValidationStatus expectedStatus = EValidationStatus::Failed)
    {
        auto actualStatus = Accessor.ValidateAndInitialize();

        UNIT_ASSERT_VALUES_EQUAL(expectedStatus, actualStatus);

        UNIT_ASSERT(HasError(Accessor.GetLastValidationError()));

        UNIT_ASSERT_STRING_CONTAINS(
            Accessor.GetLastValidationError().GetMessage(),
            expectedErrorSubstring);

        if (!Debug && actualStatus == EValidationStatus::Failed) {
            UNIT_ASSERT(Accessor.GetHeader() == nullptr);
            UNIT_ASSERT(Accessor.GetDataProcessor() == nullptr);
            UNIT_ASSERT(Accessor.GetRawMetadata().empty());
        }
    }

    void ResizeAndRemap(size_t size)
    {
        auto error = Accessor.ResizeAndRemap(size);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        RawData = Accessor.GetRawData();
    }

    TFileRingBufferHeader& RawDataHeader()
    {
        UNIT_ASSERT_LE(sizeof(TFileRingBufferHeader), RawData.size());
        return *reinterpret_cast<TFileRingBufferHeader*>(RawData.data());
    }

private:
    void Remap()
    {
        auto error = Accessor.Map();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        RawData = Accessor.GetRawData();
    }
};

struct TTestFileRingBufferAccessor: public TFileRingBufferAccessor
{
public:
    TTestFileRingBufferAccessor()
        : TFileRingBufferAccessor(EFileRingBufferAccessorValidationMode::Debug)
    {}

    void SetRawData(std::span<char> rawData)
    {
        UpdateRawData(rawData);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

static bool operator==(bool lhs, const TResultOrError<bool>& rhs)
{
    return !HasError(rhs) && lhs == rhs.GetResult();
}

Y_UNIT_TEST_SUITE(TFileRingBufferAccessorTest)
{
    Y_UNIT_TEST(ShouldValidateEmptyFile)
    {
        TBootstrap b;

        b.AssertValidateFailed(
            "File is empty",
            EValidationStatus::NotInitialized);

        UNIT_ASSERT(b.Accessor.GetHeader() == nullptr);
        UNIT_ASSERT(b.Accessor.GetDataProcessor() == nullptr);
    }

    Y_UNIT_TEST(ShouldNotValidateNonAlignedBuffer)
    {
        TTestFileRingBufferAccessor accessor;
        ui64 value = 0;
        auto span = std::span(reinterpret_cast<char*>(&value) + 1, 2);

        accessor.SetRawData(span);
        auto status = accessor.ValidateAndInitialize();
        UNIT_ASSERT_VALUES_EQUAL(status, EValidationStatus::Failed);
        UNIT_ASSERT_STRING_CONTAINS(
            accessor.GetLastValidationError().GetMessage(),
            "Buffer is not aligned");
    }

    Y_UNIT_TEST(ShouldValidateFileWithZeroHeader)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader));

        b.AssertValidateFailed(
            "File is not initialized",
            EValidationStatus::NotInitialized);

        UNIT_ASSERT(b.Accessor.GetHeader() != nullptr);
        UNIT_ASSERT(b.Accessor.GetDataProcessor() == nullptr);
    }

    Y_UNIT_TEST(ShouldNotValidateFileWithZeroVersionAndNonZeroContents)
    {
        TBootstrap b(false);
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader));
        b.RawDataHeader().DataCapacity = 1;

        b.AssertValidateFailed("Unsupported file ring buffer version 0");
    }

    Y_UNIT_TEST(ShouldNotValidateTooSmallFile)
    {
        TBootstrap b;
        b.ResizeAndRemap(1);
        b.RawData[0] = 1;

        b.AssertValidateFailed("File is too small");
    }

    Y_UNIT_TEST(ShouldNotValidateUnsupportedVersion)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader));

        auto& header = b.RawDataHeader();
        header.Version = static_cast<EFileRingBufferVersion>(11111);

        b.AssertValidateFailed("Unsupported file ring buffer version");
    }

    FILE_RING_BUFFER_TEST(ShouldValidateCorrectHeader)
    {
        TBootstrap b;
        b.ResizeAndRemap(
            sizeof(TFileRingBufferHeader) + b.MetadataCapacity +
            b.DataCapacity);

        TFileRingBufferHeader header;
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = b.MetadataCapacity;
        header.MetadataSize = 0;
        header.MetadataChecksum = 0;
        header.DataOffset = sizeof(TFileRingBufferHeader) + b.MetadataCapacity;
        header.DataCapacity = b.DataCapacity;
        header.ReadPos = 16;
        header.WritePos = 16;

        b.RawDataHeader() = header;

        b.AssertValidateSuccess();

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(header.Version),
            static_cast<ui32>(b.Accessor.GetHeader()->Version));

        UNIT_ASSERT_VALUES_EQUAL(
            header.HeaderSize,
            b.Accessor.GetHeader()->HeaderSize);

        UNIT_ASSERT_VALUES_EQUAL(
            header.MetadataOffset,
            b.Accessor.GetHeader()->MetadataOffset);

        UNIT_ASSERT_VALUES_EQUAL(
            header.MetadataCapacity,
            b.Accessor.GetHeader()->MetadataCapacity);

        UNIT_ASSERT_VALUES_EQUAL(
            header.MetadataSize,
            b.Accessor.GetHeader()->MetadataSize);

        UNIT_ASSERT_VALUES_EQUAL(
            header.MetadataChecksum,
            b.Accessor.GetHeader()->MetadataChecksum);

        UNIT_ASSERT_VALUES_EQUAL(
            header.DataOffset,
            b.Accessor.GetHeader()->DataOffset);

        UNIT_ASSERT_VALUES_EQUAL(
            header.DataCapacity,
            b.Accessor.GetHeader()->DataCapacity);

        UNIT_ASSERT_VALUES_EQUAL(
            header.ReadPos,
            b.Accessor.GetHeader()->ReadPos);

        UNIT_ASSERT_VALUES_EQUAL(
            header.WritePos,
            b.Accessor.GetHeader()->WritePos);
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateInvalidHeaderSize)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader) + 1;

        b.AssertValidateFailed("Invalid file ring buffer header size");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateInvalidMetadataOffset)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader) - 1;

        b.AssertValidateFailed("Invalid file ring buffer metadata offset");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateUnalignedMetadataOffset)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + sizeof(ui64));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader) + 1;

        b.AssertValidateFailed("Invalid file ring buffer metadata offset");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateInvalidMetadataSize)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + sizeof(ui64));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = sizeof(ui64);
        header.MetadataSize = sizeof(ui64) + 1;

        b.AssertValidateFailed("Invalid file ring buffer metadata size");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateInvalidDataOffset)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + (2 * sizeof(ui64)));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader) + sizeof(ui64);
        header.MetadataCapacity = sizeof(ui64);
        header.MetadataSize = 0;

        // Overlap with metadata
        header.DataOffset = sizeof(TFileRingBufferHeader);
        header.DataCapacity = sizeof(ui64);

        b.AssertValidateFailed("Invalid file ring buffer data offset");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateOutOfRangeDataOffset)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + sizeof(ui64));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = sizeof(ui64);
        header.MetadataSize = 0;

        // Outside file size
        header.DataOffset = sizeof(TFileRingBufferHeader) + (2 * sizeof(ui64));
        header.DataCapacity = sizeof(ui64);

        b.AssertValidateFailed("Invalid file ring buffer data offset");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateUnalignedDataOffset)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + (3 * sizeof(ui64)));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = sizeof(ui64);
        header.MetadataSize = 0;

        // Outside file size
        header.DataOffset = sizeof(TFileRingBufferHeader) + sizeof(ui64) + 1;
        header.DataCapacity = sizeof(ui64);

        b.AssertValidateFailed("Invalid file ring buffer data offset");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateInvalidMetadataCapacity)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + sizeof(ui64));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = sizeof(ui64);
        header.MetadataSize = 0;
        header.DataOffset = sizeof(TFileRingBufferHeader);
        header.DataCapacity = sizeof(ui64);

        b.AssertValidateFailed("Invalid file ring buffer metadata capacity");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateInvalidDataCapacity)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + sizeof(ui64));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = 0;
        header.MetadataSize = 0;
        header.DataOffset = sizeof(TFileRingBufferHeader);
        header.DataCapacity = sizeof(ui64) * 2;

        b.AssertValidateFailed("Invalid file ring buffer data capacity");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateInvalidReadPos)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + sizeof(ui64));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = 0;
        header.MetadataSize = 0;
        header.DataOffset = sizeof(TFileRingBufferHeader);
        header.DataCapacity = sizeof(ui64);
        header.ReadPos = sizeof(ui64) + 1;

        b.AssertValidateFailed("Invalid file ring buffer read position");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateUnalignedReadPos)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + sizeof(ui64));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = 0;
        header.MetadataSize = 0;
        header.DataOffset = sizeof(TFileRingBufferHeader);
        header.DataCapacity = sizeof(ui64);
        header.ReadPos = 0;

        b.AssertValidateSuccess();

        const auto alignment = b.Accessor.GetCapabilities().Alignment;
        if (alignment == 1) {
            return;
        }

        b.RawDataHeader().ReadPos = 1;

        if (alignment > 1) {
            b.AssertValidateFailed(
                "Invalid file ring buffer read position 1 (expected to be "
                "aligned");
        } else {
            b.AssertValidateSuccess();
        }

        b.RawDataHeader().ReadPos = 0;
        b.RawDataHeader().WritePos = 1;

        if (alignment > 1) {
            b.AssertValidateFailed(
                "Invalid file ring buffer write position 1 (expected to be "
                "aligned");
        } else {
            b.AssertValidateSuccess();
        }
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateInvalidWritePos)
    {
        TBootstrap b;
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader) + sizeof(ui64));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader);
        header.MetadataOffset = sizeof(TFileRingBufferHeader);
        header.MetadataCapacity = 0;
        header.MetadataSize = 0;
        header.DataOffset = sizeof(TFileRingBufferHeader);
        header.DataCapacity = sizeof(ui64);
        header.WritePos = sizeof(ui64) + 1;

        b.AssertValidateFailed("Invalid file ring buffer write position");
    }

    FILE_RING_BUFFER_TEST(ShouldValidateNormalFile)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("ABC"));
                UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("123"));
                UNIT_ASSERT_VALUES_EQUAL(true, rb.SetMetadata("meta"));
            },
            ver);

        b.AssertValidateSuccess();

        UNIT_ASSERT(!b.Accessor.GetRawMetadata().empty());
    }

    FILE_RING_BUFFER_TEST(ShouldValidateWrappedFile)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                while (rb.PushBack("ABCD").GetResult()) {
                    // Add elements until the buffer is full
                }

                rb.PopFront();
                rb.PopFront();

                UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("wrap"));
            },
            ver);

        b.AssertValidateSuccess();
    }

    FILE_RING_BUFFER_TEST(ShouldValidateWrappedFileWithAllocWithoutCommit)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                while (rb.PushBack("ABCD").GetResult()) {
                    // Add elements until the buffer is full
                }

                rb.PopFront();
                rb.PopFront();

                rb.Alloc(4);
            },
            ver);

        b.AssertValidateSuccess();
    }

    FILE_RING_BUFFER_TEST(ShouldValidateWrappedFileWithAbortedAlloc)
    {
        TBootstrap b;

        // Initialize empty file
        b.Execute([](TFileRingBuffer&) {}, ver);

        b.AssertValidateSuccess();
        const auto alignment = b.Accessor.GetCapabilities().Alignment;

        auto& header = b.RawDataHeader();
        header.ReadPos = alignment > 0 ? alignment : 1;

        b.Accessor.GetDataProcessor()->WriteEntryHeader(header.ReadPos, {});

        b.AssertValidateSuccess();

        b.Accessor.GetDataProcessor()->WriteEntryHeader(
            header.ReadPos,
            {.DataSize = 4, .FreeFlag = true});

        b.AssertValidateFailed(
            "expected to point to a slack space when WritePos == 0)");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateFileWithZeroDataSizeAndSkipFlag)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            { UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("ABC")); },
            ver);

        b.AssertValidateSuccess();

        b.Accessor.GetDataProcessor()->WriteEntryHeader(
            0,
            {.DataSize = 0, .FreeFlag = true});

        b.AssertValidateFailed("data size is zero and free flag is set");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateFileWithZeroDataSizeAndNonZeroTag)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            { UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("ABC")); },
            ver);

        b.AssertValidateSuccess();

        b.Accessor.GetDataProcessor()->WriteEntryHeader(
            0,
            {.DataSize = 0, .Tag = 1});

        b.AssertValidateFailed("data size is zero and tag is non-zero");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateFileWithDataSizeExceedsCapacity)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            { UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("ABC")); },
            ver);

        b.AssertValidateSuccess();

        b.Accessor.GetDataProcessor()->WriteEntryHeader(0, {.DataSize = 1000});

        b.AssertValidateFailed("data size 1000 exceeds data capacity");
    }

    FILE_RING_BUFFER_TEST(ShouldNotValidateFileWithNonZeroChecksumAndFreeFlag)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            { UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("ABC")); },
            ver);

        b.AssertValidateSuccess();

        b.Accessor.GetDataProcessor()->WriteEntryHeader(
            0,
            {.DataSize = 3, .DataChecksum = 1, .FreeFlag = true});

        if (b.Accessor.GetCapabilities().EntryHeaderIsProcessedAtomically) {
            b.AssertValidateFailed(
                "free flag is set and data checksum is non-zero");
        } else {
            b.AssertValidateSuccess();
        }
    }

    FILE_RING_BUFFER_TEST(
        ShouldNotValidateFileWithNonZeroChecksumAndZeroDataSize)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("01234567"));
                UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("ABC"));
            },
            ver);

        b.AssertValidateSuccess();

        b.Accessor.GetDataProcessor()->WriteEntryHeader(
            16,
            {.DataSize = 0, .DataChecksum = 1});

        if (b.Accessor.GetCapabilities().EntryHeaderIsProcessedAtomically) {
            b.AssertValidateFailed(
                "data size is zero and data checksum is non-zero");
        } else {
            b.AssertValidateFailed("unexpected slack space marker");
        }
    }

    FILE_RING_BUFFER_TEST(ShouldInitializeHeaderOnValidationFailureInDebugMode)
    {
        TBootstrap b(true);
        b.ResizeAndRemap(sizeof(TFileRingBufferHeader));

        auto& header = b.RawDataHeader();
        header.Version = ver;
        header.HeaderSize = sizeof(TFileRingBufferHeader) + 1;

        b.AssertValidateFailed("Invalid file ring buffer header size");

        UNIT_ASSERT(b.Accessor.GetHeader() != nullptr);

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(ver),
            static_cast<ui32>(b.Accessor.GetHeader()->Version));

        UNIT_ASSERT_VALUES_EQUAL(
            sizeof(TFileRingBufferHeader) + 1,
            b.Accessor.GetHeader()->HeaderSize);
    }

    FILE_RING_BUFFER_TEST(ShouldInitializeDataOnValidationFailureInDebugMode)
    {
        TBootstrap b(true);
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                UNIT_ASSERT_VALUES_EQUAL(true, rb.PushBack("ABC"));
                UNIT_ASSERT_VALUES_EQUAL(true, rb.SetMetadata("123"));
            },
            ver);

        b.AssertValidateSuccess();

        *b.Accessor.GetDataProcessor()->GetEntryDataPtr(0, 1) = 'X';

        b.AssertValidateFailed("Checksum mismatch");

        UNIT_ASSERT(b.Accessor.GetHeader() != nullptr);
        UNIT_ASSERT(b.Accessor.GetDataProcessor() != nullptr);
        UNIT_ASSERT(!b.Accessor.GetRawMetadata().empty());
    }
}

}   // namespace NCloud

template <>
void Out<NCloud::EFileRingBufferAccessorValidationStatus>(
    IOutputStream& out,
    NCloud::EFileRingBufferAccessorValidationStatus value)
{
    out << static_cast<ui32>(value);
}
