#include "state_file_processor.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer.h>

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/function_ref.h>
#include <util/generic/mem_copy.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/tempfile.h>

#include <span>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

using EVersion = EFileRingBufferVersion;

#define FILE_RING_BUFFER_TEST(name)                                            \
    void TestImpl##name(EVersion version);                                     \
    Y_UNIT_TEST(name##V5)                                                      \
    {                                                                          \
        TestImpl##name(EVersion::V5);                                          \
    }                                                                          \
    Y_UNIT_TEST(name##V6)                                                      \
    {                                                                          \
        TestImpl##name(EVersion::V6);                                          \
    }                                                                          \
    void TestImpl##name(EVersion version)

// FILE_RING_BUFFER_TEST

const void* Alloc(TFileRingBuffer& ringBuffer, const TString& entry)
{
    const auto result = ringBuffer.Alloc(entry.size());
    UNIT_ASSERT_C(!HasError(result.Error), FormatError(result.Error));
    char* ptr = result.AllocationPtr;
    UNIT_ASSERT(ptr != nullptr);
    MemCopy(ptr, entry.data(), entry.size());
    const auto commitError = ringBuffer.Commit(ptr);
    UNIT_ASSERT_C(!HasError(commitError), FormatError(commitError));
    return ptr;
}

bool PushBack(TFileRingBuffer& ringBuffer, TStringBuf entry)
{
    const auto result = ringBuffer.PushBack(entry);
    UNIT_ASSERT_C(!HasError(result.Error), FormatError(result.Error));
    return result.Pushed;
}

bool PopFront(TFileRingBuffer& ringBuffer)
{
    const auto result = ringBuffer.PopFront();
    UNIT_ASSERT_C(!HasError(result.Error), FormatError(result.Error));
    return result.Removed;
}

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    static constexpr ui64 DataCapacity = 64;
    static constexpr ui64 MetadataCapacity = 8;

    TTempFileHandle TempFileHandle;
    TFileMapFileRingBufferAccessor Accessor;
    std::span<char> RawData;

    TBootstrap()
        : Accessor(
              TempFileHandle.Name(),
              EFileRingBufferAccessorValidationMode::Debug,
              TMemoryMapCommon::EOpenModeFlag::oRdWr)
    {
        Remap();
    }

    void Execute(
        const TFunctionRef<void(TFileRingBuffer&)>& fn,
        EFileRingBufferVersion version)
    {
        TFileRingBuffer ringBuffer(
            TempFileHandle.Name(),
            DataCapacity,
            MetadataCapacity,
            version);

        fn(ringBuffer);

        // TFileRingBuffer may resize the file. TFileMap does not update its
        // mapping automatically, so reopen it before dumping.
        Accessor.Close();
        Remap();
    }

    void ResizeAndRemap(size_t size)
    {
        const auto error = Accessor.ResizeAndRemap(size);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        RawData = Accessor.GetRawData();
    }

    NProto::TStateFileDump Dump()
    {
        return TStateFileProcessor::DumpStateFile(Accessor);
    }

private:
    void Remap()
    {
        const auto error = Accessor.Map();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        RawData = Accessor.GetRawData();
    }
};

TStringBuf AsBytes(const TVector<ui64>& words)
{
    return {
        reinterpret_cast<const char*>(words.data()),
        words.size() * sizeof(ui64)};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStateFileProcessorTest)
{
    Y_UNIT_TEST(ShouldDumpUninitializedAndMalformedStateFiles)
    {
        {
            TBootstrap bootstrap;
            const auto dump = bootstrap.Dump();

            UNIT_ASSERT(!dump.GetIsCorrupted());
            UNIT_ASSERT(!dump.HasHeader());
            UNIT_ASSERT_VALUES_EQUAL(0, dump.GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());
        }

        {
            TBootstrap bootstrap;
            bootstrap.ResizeAndRemap(1);
            bootstrap.RawData[0] = 1;

            const auto dump = bootstrap.Dump();

            UNIT_ASSERT(dump.GetIsCorrupted());
            UNIT_ASSERT(!dump.HasHeader());
            UNIT_ASSERT(dump.HasValidationError());
            UNIT_ASSERT_VALUES_EQUAL(
                Crc32c(bootstrap.RawData.data(), bootstrap.RawData.size()),
                dump.GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());
        }

        {
            TBootstrap bootstrap;
            bootstrap.ResizeAndRemap(sizeof(TFileRingBufferHeader));

            const auto dump = bootstrap.Dump();

            UNIT_ASSERT(!dump.GetIsCorrupted());
            UNIT_ASSERT(dump.HasHeader());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(EVersion::NotInitialized),
                dump.GetHeader().GetVersion());
            UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());
        }
    }

    FILE_RING_BUFFER_TEST(ShouldDumpEmptyFile)
    {
        TBootstrap bootstrap;
        bootstrap.Execute([](TFileRingBuffer&) {}, version);

        const auto dump = bootstrap.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());
        UNIT_ASSERT_VALUES_EQUAL(
            Crc32c(bootstrap.RawData.data(), bootstrap.RawData.size()),
            dump.GetChecksum());

        const auto& header = dump.GetHeader();
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(version),
            header.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(
            sizeof(TFileRingBufferHeader),
            header.GetHeaderSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TBootstrap::DataCapacity,
            header.GetDataCapacity());
        UNIT_ASSERT_VALUES_EQUAL(
            TBootstrap::MetadataCapacity,
            header.GetMetadataCapacity());
        UNIT_ASSERT_VALUES_EQUAL(0, header.GetReadPos());
        UNIT_ASSERT_VALUES_EQUAL(0, header.GetWritePos());
        UNIT_ASSERT(dump.HasActualMetadataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(0, dump.GetActualMetadataChecksum());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpMetadataChecksum)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                const auto result = ringBuffer.SetMetadata("meta");
                UNIT_ASSERT_C(
                    !HasError(result.Error),
                    FormatError(result.Error));
                UNIT_ASSERT(result.Updated);
            },
            version);

        const auto initialDump = bootstrap.Dump();
        UNIT_ASSERT(!initialDump.GetIsCorrupted());
        UNIT_ASSERT(initialDump.HasActualMetadataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(
            initialDump.GetHeader().GetMetadataChecksum(),
            initialDump.GetActualMetadataChecksum());

        bootstrap.Accessor.GetRawMetadata()[0] ^= 1;

        const auto corruptDump = bootstrap.Dump();
        UNIT_ASSERT(corruptDump.GetIsCorrupted());
        UNIT_ASSERT(corruptDump.HasActualMetadataChecksum());
        UNIT_ASSERT(corruptDump.HasValidationError());
        UNIT_ASSERT_VALUES_UNEQUAL(
            corruptDump.GetHeader().GetMetadataChecksum(),
            corruptDump.GetActualMetadataChecksum());
    }

    FILE_RING_BUFFER_TEST(ShouldReportUnavailableMetadataChecksum)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                const auto result = ringBuffer.SetMetadata("meta");
                UNIT_ASSERT_C(
                    !HasError(result.Error),
                    FormatError(result.Error));
                UNIT_ASSERT(result.Updated);
            },
            version);

        bootstrap.Accessor.ValidateAndInitialize();
        bootstrap.Accessor.GetHeader()->MetadataSize =
            TBootstrap::MetadataCapacity + 1;

        const auto dump = bootstrap.Dump();
        UNIT_ASSERT(dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasValidationError());
        UNIT_ASSERT(!dump.HasActualMetadataChecksum());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpFileWithSingleEntry)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(PushBack(ringBuffer, "Hello")); },
            version);

        const auto dump = bootstrap.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());

        const auto& entry = dump.GetEntries(0);
        const ui32 checksum = Crc32c("Hello", 5);

        UNIT_ASSERT_VALUES_EQUAL(0, entry.GetEntryPos());
        UNIT_ASSERT_VALUES_EQUAL(5, entry.GetDataSize());
        UNIT_ASSERT_VALUES_EQUAL(checksum, entry.GetDataChecksum());
        UNIT_ASSERT(entry.HasActualDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(checksum, entry.GetActualDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(0, entry.GetTag());
        UNIT_ASSERT(!entry.GetFreeFlag());
        UNIT_ASSERT(!entry.HasWriteDataRequestInfo());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpFileWithTwoPushedAndOnePoppedEntry)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(PushBack(ringBuffer, "Hello"));
                UNIT_ASSERT(PushBack(ringBuffer, "Bye"));
                UNIT_ASSERT(PopFront(ringBuffer));
            },
            version);

        const auto dump = bootstrap.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());

        const auto& entry = dump.GetEntries(0);
        const ui32 checksum = Crc32c("Bye", 3);

        UNIT_ASSERT_LT(0, entry.GetEntryPos());
        UNIT_ASSERT_VALUES_EQUAL(3, entry.GetDataSize());
        UNIT_ASSERT_VALUES_EQUAL(checksum, entry.GetDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(checksum, entry.GetActualDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(0, entry.GetTag());
        UNIT_ASSERT(!entry.GetFreeFlag());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpFileWithSkippedEntries)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                const void* first = Alloc(ringBuffer, "Hello");
                const void* skipped = Alloc(ringBuffer, "What");
                const void* last = Alloc(ringBuffer, "Bye");

                UNIT_ASSERT(first != nullptr);
                UNIT_ASSERT(skipped != nullptr);
                UNIT_ASSERT(last != nullptr);
                UNIT_ASSERT(!HasError(ringBuffer.Free(skipped)));
            },
            version);

        const auto dump = bootstrap.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(3, dump.GetEntries().size());

        const auto& first = dump.GetEntries(0);
        const auto& skipped = dump.GetEntries(1);
        const auto& last = dump.GetEntries(2);

        UNIT_ASSERT(!first.GetFreeFlag());
        UNIT_ASSERT(skipped.GetFreeFlag());
        UNIT_ASSERT(!last.GetFreeFlag());
        UNIT_ASSERT_VALUES_EQUAL(4, skipped.GetDataSize());
        UNIT_ASSERT(skipped.HasActualDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(0, skipped.GetActualDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(0, skipped.GetDataChecksum());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpFileWithTaggedEntries)
    {
        ui32 expectedFirstTag = 0;
        ui32 expectedSecondTag = 0;

        TBootstrap bootstrap;
        bootstrap.Execute(
            [&](TFileRingBuffer& ringBuffer)
            {
                const void* first = Alloc(ringBuffer, "Hello");
                const void* second = Alloc(ringBuffer, "Bye");

                expectedFirstTag = Min(1U, ringBuffer.GetMaxTag());
                expectedSecondTag = Min(2U, ringBuffer.GetMaxTag());

                UNIT_ASSERT(
                    !HasError(ringBuffer.SetTag(first, expectedFirstTag)));
                UNIT_ASSERT(
                    !HasError(ringBuffer.SetTag(second, expectedSecondTag)));
            },
            version);

        const auto dump = bootstrap.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(2, dump.GetEntries().size());
        UNIT_ASSERT_VALUES_EQUAL(expectedFirstTag, dump.GetEntries(0).GetTag());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedSecondTag,
            dump.GetEntries(1).GetTag());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpWrappedStateFile)
    {
        size_t entryCount = 0;

        TBootstrap bootstrap;
        bootstrap.Execute(
            [&](TFileRingBuffer& ringBuffer)
            {
                while (PushBack(ringBuffer, "123")) {
                    ++entryCount;
                }

                UNIT_ASSERT(PopFront(ringBuffer));
                UNIT_ASSERT(PopFront(ringBuffer));
                UNIT_ASSERT(!ringBuffer.Empty());
                UNIT_ASSERT(PushBack(ringBuffer, "ABCD"));
            },
            version);

        UNIT_ASSERT_LT(2, entryCount);
        --entryCount;

        const auto dump = bootstrap.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(entryCount, dump.GetEntries().size());

        for (size_t i = 0; i < entryCount - 1; ++i) {
            const auto& entry = dump.GetEntries(i);
            UNIT_ASSERT(!entry.GetFreeFlag());
            UNIT_ASSERT_VALUES_EQUAL(3, entry.GetDataSize());
        }

        const auto& lastEntry = dump.GetEntries(entryCount - 1);
        UNIT_ASSERT(!lastEntry.GetFreeFlag());
        UNIT_ASSERT_VALUES_EQUAL(4, lastEntry.GetDataSize());
        UNIT_ASSERT_VALUES_EQUAL(0, lastEntry.GetEntryPos());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpCorruptedStateFile)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(PushBack(ringBuffer, "Hello"));
                UNIT_ASSERT(PushBack(ringBuffer, "Bye"));
            },
            version);

        const auto dump = bootstrap.Dump();
        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(2, dump.GetEntries().size());

        const auto entryPos = dump.GetEntries(1).GetEntryPos();
        const auto entryHeader =
            bootstrap.Accessor.GetDataProcessor()->ReadEntryHeader(entryPos);

        auto headerWithCorruptedChecksum = entryHeader;
        headerWithCorruptedChecksum.DataChecksum ^= 1;

        bootstrap.Accessor.ValidateAndInitialize();
        bootstrap.Accessor.GetDataProcessor()->WriteEntryHeader(
            entryPos,
            headerWithCorruptedChecksum);

        const auto checksumDump = bootstrap.Dump();
        UNIT_ASSERT(checksumDump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(2, checksumDump.GetEntries().size());
        UNIT_ASSERT_VALUES_UNEQUAL(
            checksumDump.GetEntries(1).GetDataChecksum(),
            checksumDump.GetEntries(1).GetActualDataChecksum());

        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(ringBuffer.IsCorrupted()); },
            version);

        auto headerWithCorruptedSize = entryHeader;
        headerWithCorruptedSize.DataSize = 1'000'000;
        headerWithCorruptedSize.DataChecksum = 0;

        bootstrap.Accessor.ValidateAndInitialize();
        bootstrap.Accessor.GetDataProcessor()->WriteEntryHeader(
            entryPos,
            headerWithCorruptedSize);

        const auto sizeDump = bootstrap.Dump();
        UNIT_ASSERT(sizeDump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(2, sizeDump.GetEntries().size());
        UNIT_ASSERT_VALUES_EQUAL(
            headerWithCorruptedSize.DataSize,
            sizeDump.GetEntries(1).GetDataSize());
        UNIT_ASSERT_VALUES_EQUAL(0, sizeDump.GetEntries(1).GetDataChecksum());
        UNIT_ASSERT(!sizeDump.GetEntries(1).HasActualDataChecksum());
        UNIT_ASSERT(sizeDump.HasValidationError());

        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(ringBuffer.IsCorrupted()); },
            version);
    }

    FILE_RING_BUFFER_TEST(ShouldDumpMalformedZeroSizeHeaders)
    {
        TVector<TFileRingBufferEntryHeader> malformedHeaders = {
            {.Tag = 1},
            {.FreeFlag = true},
        };
        if (version == EVersion::V6) {
            malformedHeaders.push_back({.DataChecksum = 1});
        }

        for (const auto& malformedHeader: malformedHeaders) {
            TBootstrap bootstrap;
            bootstrap.Execute(
                [](TFileRingBuffer& ringBuffer)
                { UNIT_ASSERT(PushBack(ringBuffer, "Hello")); },
                version);

            bootstrap.Accessor.ValidateAndInitialize();
            UNIT_ASSERT(bootstrap.Accessor.GetDataProcessor()->WriteEntryHeader(
                0,
                malformedHeader));

            const auto dump = bootstrap.Dump();
            UNIT_ASSERT(dump.GetIsCorrupted());
            UNIT_ASSERT(dump.HasValidationError());
            UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());

            const auto& entry = dump.GetEntries(0);
            UNIT_ASSERT_VALUES_EQUAL(0, entry.GetEntryPos());
            UNIT_ASSERT_VALUES_EQUAL(0, entry.GetDataSize());
            UNIT_ASSERT_VALUES_EQUAL(
                malformedHeader.DataChecksum,
                entry.GetDataChecksum());
            UNIT_ASSERT_VALUES_EQUAL(malformedHeader.Tag, entry.GetTag());
            UNIT_ASSERT_VALUES_EQUAL(
                malformedHeader.FreeFlag,
                entry.GetFreeFlag());
            UNIT_ASSERT_VALUES_EQUAL(
                malformedHeader.FreeFlag,
                entry.HasActualDataChecksum());
        }
    }

    FILE_RING_BUFFER_TEST(ShouldNotWrapAfterMalformedZeroSizeHeader)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                while (PushBack(ringBuffer, "123")) {
                }

                UNIT_ASSERT(PopFront(ringBuffer));
                UNIT_ASSERT(PopFront(ringBuffer));
                UNIT_ASSERT(PushBack(ringBuffer, "ABCD"));
            },
            version);

        const auto validDump = bootstrap.Dump();
        UNIT_ASSERT(!validDump.GetIsCorrupted());
        UNIT_ASSERT_LT(2, validDump.GetEntries().size());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            validDump.GetEntries(validDump.GetEntries().size() - 1)
                .GetEntryPos());

        const auto malformedPos = validDump.GetEntries(1).GetEntryPos();
        UNIT_ASSERT_LT(0, malformedPos);

        bootstrap.Accessor.ValidateAndInitialize();
        UNIT_ASSERT(bootstrap.Accessor.GetDataProcessor()->WriteEntryHeader(
            malformedPos,
            {.Tag = 1}));

        const auto dump = bootstrap.Dump();
        UNIT_ASSERT(dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasValidationError());
        UNIT_ASSERT_VALUES_EQUAL(2, dump.GetEntries().size());
        UNIT_ASSERT_VALUES_EQUAL(
            malformedPos,
            dump.GetEntries(1).GetEntryPos());
        UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries(1).GetDataSize());
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries(1).GetTag());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpHeaderOnlyForTruncatedFile)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(PushBack(ringBuffer, "Hello")); },
            version);

        bootstrap.ResizeAndRemap(bootstrap.RawData.size() - 1);

        const auto dump = bootstrap.Dump();
        UNIT_ASSERT(dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT(dump.HasValidationError());
        UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());
    }

    FILE_RING_BUFFER_TEST(ShouldNotModifyFileThroughReadOnlyMapping)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(PushBack(ringBuffer, "Hello")); },
            version);

        const TString before(
            bootstrap.RawData.data(),
            bootstrap.RawData.size());

        TFileMapFileRingBufferAccessor readOnlyAccessor(
            bootstrap.TempFileHandle.Name(),
            EFileRingBufferAccessorValidationMode::Debug,
            TMemoryMapCommon::EOpenModeFlag::oRdOnly);
        auto error = readOnlyAccessor.Map();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        const auto dump = TStateFileProcessor::DumpStateFile(readOnlyAccessor);
        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());
        UNIT_ASSERT_VALUES_EQUAL(
            before,
            TString(bootstrap.RawData.data(), bootstrap.RawData.size()));
    }

    FILE_RING_BUFFER_TEST(ShouldNotDumpPhantomEntriesAfterSlackAtReadPos)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(PushBack(ringBuffer, "Hello"));
                UNIT_ASSERT(PushBack(ringBuffer, "Bye"));
            },
            version);

        bootstrap.Accessor.ValidateAndInitialize();
        const ui64 slackPos = TBootstrap::DataCapacity - sizeof(ui64);
        UNIT_ASSERT(bootstrap.Accessor.GetDataProcessor()->WriteEntryHeader(
            slackPos,
            {}));
        bootstrap.Accessor.GetHeader()->ReadPos = slackPos;

        const auto dump = bootstrap.Dump();
        UNIT_ASSERT(dump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpWriteDataRequestFields)
    {
        {
            TBootstrap bootstrap;
            bootstrap.Execute(
                [](TFileRingBuffer& ringBuffer)
                {
                    const TVector<ui64> requestHeader{1, 2, 3};
                    UNIT_ASSERT(PushBack(ringBuffer, AsBytes(requestHeader)));
                },
                version);

            const auto dump = bootstrap.Dump();
            UNIT_ASSERT(!dump.GetIsCorrupted());
            UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());
            UNIT_ASSERT(!dump.GetEntries(0).HasWriteDataRequestInfo());
        }

        {
            TBootstrap bootstrap;
            bootstrap.Execute(
                [](TFileRingBuffer& ringBuffer)
                {
                    const TVector<ui64> requestWithPayload{10, 20, 30, 40};
                    UNIT_ASSERT(
                        PushBack(ringBuffer, AsBytes(requestWithPayload)));
                },
                version);

            const auto dump = bootstrap.Dump();
            UNIT_ASSERT(!dump.GetIsCorrupted());
            UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());

            const auto& info = dump.GetEntries(0).GetWriteDataRequestInfo();
            UNIT_ASSERT_VALUES_EQUAL(10, info.GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(20, info.GetHandle());
            UNIT_ASSERT_VALUES_EQUAL(30, info.GetOffset());
            UNIT_ASSERT_VALUES_EQUAL(sizeof(ui64), info.GetSize());
        }
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
