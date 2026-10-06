#include "state_file_processor.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer.h>

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/protobuf/json/config.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/function_ref.h>
#include <util/generic/mem_copy.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>
#include <util/system/tempfile.h>

#include <cstring>
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

TString Dump(TFileRingBuffer& ringBuffer)
{
    TStringBuilder result;
    const auto error = ringBuffer.Visit(
        [&](ui32 checksum, ui32 tag, TStringBuf entry)
        {
            Y_UNUSED(checksum);
            if (!result.empty()) {
                result << ",";
            }
            result << entry << ":" << tag;
        });
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    return result;
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
        EFileRingBufferVersion version,
        ui64 dataCapacity = DataCapacity)
    {
        TFileRingBuffer ringBuffer(
            TempFileHandle.Name(),
            dataCapacity,
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

    void Reopen()
    {
        Accessor.Close();
        Remap();
    }

    NProto::TStateFileDump Dump()
    {
        return TStateFileProcessor::DumpStateFile(Accessor);
    }

    NCloud::NProto::TError Patch(const NProto::TStateFileDump& newState)
    {
        return TStateFileProcessor::PatchStateFile(Accessor, newState);
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

    FILE_RING_BUFFER_TEST(ShouldRepairMetadataChecksum)
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

        UNIT_ASSERT(!bootstrap.Dump().GetIsCorrupted());
        bootstrap.Accessor.GetRawMetadata()[0] ^= 1;

        auto dump = bootstrap.Dump();
        UNIT_ASSERT(dump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_UNEQUAL(
            dump.GetHeader().GetMetadataChecksum(),
            dump.GetActualMetadataChecksum());

        dump.MutableHeader()->SetMetadataChecksum(
            dump.GetActualMetadataChecksum());
        UNIT_ASSERT_C(!HasError(bootstrap.Patch(dump)), "Patch failed");

        const auto repairedDump = bootstrap.Dump();
        UNIT_ASSERT(!repairedDump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(
            repairedDump.GetHeader().GetMetadataChecksum(),
            repairedDump.GetActualMetadataChecksum());
    }

    FILE_RING_BUFFER_TEST(ShouldPatchHeader)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(PushBack(ringBuffer, "Hello"));
                UNIT_ASSERT(PushBack(ringBuffer, "What"));
                UNIT_ASSERT(PushBack(ringBuffer, "Bye"));
            },
            version);

        auto dump = bootstrap.Dump();
        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(3, dump.GetEntries().size());

        dump.MutableHeader()->SetReadPos(dump.GetEntries(1).GetEntryPos());
        dump.MutableHeader()->SetWritePos(dump.GetEntries(2).GetEntryPos());
        UNIT_ASSERT_C(!HasError(bootstrap.Patch(dump)), "Patch failed");

        const auto newDump = bootstrap.Dump();
        UNIT_ASSERT(!newDump.GetIsCorrupted());
        UNIT_ASSERT(newDump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(1, newDump.GetEntries().size());

        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(!ringBuffer.IsCorrupted());
                UNIT_ASSERT_VALUES_EQUAL("What:0", Dump(ringBuffer));
            },
            version);

        auto clearDump = bootstrap.Dump();
        clearDump.MutableHeader()->SetReadPos(0);
        clearDump.MutableHeader()->SetWritePos(0);
        UNIT_ASSERT_C(!HasError(bootstrap.Patch(clearDump)), "Patch failed");

        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(!ringBuffer.IsCorrupted());
                UNIT_ASSERT(ringBuffer.Empty());
            },
            version);
    }

    FILE_RING_BUFFER_TEST(ShouldClearStateFileWithOutOfRangeCursor)
    {
        auto test = [&](auto corruptCursor)
        {
            TBootstrap bootstrap;
            bootstrap.Execute(
                [](TFileRingBuffer& ringBuffer)
                { UNIT_ASSERT(PushBack(ringBuffer, "Hello")); },
                version);

            bootstrap.Accessor.ValidateAndInitialize();
            auto* header = bootstrap.Accessor.GetHeader();
            UNIT_ASSERT(header != nullptr);
            corruptCursor(*header);

            auto patch = bootstrap.Dump();
            UNIT_ASSERT(patch.GetIsCorrupted());
            patch.MutableHeader()->SetReadPos(0);
            patch.MutableHeader()->SetWritePos(0);

            UNIT_ASSERT_C(!HasError(bootstrap.Patch(patch)), "Patch failed");

            bootstrap.Execute(
                [](TFileRingBuffer& ringBuffer)
                {
                    UNIT_ASSERT(!ringBuffer.IsCorrupted());
                    UNIT_ASSERT(ringBuffer.Empty());
                    UNIT_ASSERT(PushBack(ringBuffer, "Recovered"));
                    UNIT_ASSERT_VALUES_EQUAL("Recovered:0", Dump(ringBuffer));
                },
                version);
        };

        test([](auto& header) {
            header.ReadPos = header.DataCapacity + 1;
        });
        test([](auto& header) {
            header.WritePos = header.DataCapacity + 1;
        });
    }

    FILE_RING_BUFFER_TEST(ShouldRejectOverlappingEntryRepairs)
    {
        constexpr ui64 dataCapacity = 128;
        constexpr ui64 highEntryPos = 64;
        constexpr ui32 highEntryDataSize = 32;
        constexpr ui64 slackPos = 104;
        constexpr ui64 lowEntryPos = 0;
        constexpr ui32 lowEntryDataSize = 88;
        constexpr ui64 lowEntryEnd = 96;

        TBootstrap bootstrap;
        bootstrap.Execute([](TFileRingBuffer&) {}, version, dataCapacity);
        bootstrap.Accessor.ValidateAndInitialize();

        auto* dataProcessor = bootstrap.Accessor.GetDataProcessor();
        auto* header = bootstrap.Accessor.GetHeader();
        UNIT_ASSERT(dataProcessor != nullptr);
        UNIT_ASSERT(header != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(
            slackPos,
            highEntryPos + dataProcessor->GetEntrySize(highEntryDataSize));
        UNIT_ASSERT_VALUES_EQUAL(
            lowEntryEnd,
            lowEntryPos + dataProcessor->GetEntrySize(lowEntryDataSize));

        char* lowData =
            dataProcessor->GetEntryDataPtr(lowEntryPos, lowEntryDataSize);
        char* highData =
            dataProcessor->GetEntryDataPtr(highEntryPos, highEntryDataSize);
        UNIT_ASSERT(lowData != nullptr);
        UNIT_ASSERT(highData != nullptr);
        std::memset(lowData, 'B', lowEntryDataSize);
        std::memset(highData, 'A', highEntryDataSize);

        TFileRingBufferEntryHeader highEntryHeader{
            .DataSize = highEntryDataSize,
            .DataChecksum = Crc32c(highData, highEntryDataSize)};
        UNIT_ASSERT(
            dataProcessor->WriteEntryHeader(highEntryPos, highEntryHeader));
        UNIT_ASSERT(dataProcessor->WriteEntryHeader(slackPos, {}));

        const TFileRingBufferEntryHeader lowEntryHeader{
            .DataSize = lowEntryDataSize,
            .DataChecksum = Crc32c(lowData, lowEntryDataSize)};
        UNIT_ASSERT(
            dataProcessor->WriteEntryHeader(lowEntryPos, lowEntryHeader));

        // Corrupting A's checksum also changes bytes in B's overlapping
        // payload, so both entries need a checksum repair.
        highEntryHeader.DataChecksum ^= 1;
        UNIT_ASSERT(
            dataProcessor->WriteEntryHeader(highEntryPos, highEntryHeader));
        header->ReadPos = highEntryPos;
        header->WritePos = 32;

        auto patch = bootstrap.Dump();
        UNIT_ASSERT(patch.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(2, patch.GetEntries().size());
        UNIT_ASSERT_VALUES_EQUAL(
            highEntryPos,
            patch.GetEntries(0).GetEntryPos());
        UNIT_ASSERT_VALUES_EQUAL(
            lowEntryPos,
            patch.GetEntries(1).GetEntryPos());
        UNIT_ASSERT_VALUES_UNEQUAL(
            patch.GetEntries(0).GetDataChecksum(),
            patch.GetEntries(0).GetActualDataChecksum());
        UNIT_ASSERT_VALUES_UNEQUAL(
            patch.GetEntries(1).GetDataChecksum(),
            patch.GetEntries(1).GetActualDataChecksum());

        patch.MutableHeader()->SetReadPos(lowEntryPos);
        patch.MutableHeader()->SetWritePos(lowEntryEnd);
        for (auto& entry: *patch.MutableEntries()) {
            entry.SetDataChecksum(entry.GetActualDataChecksum());
        }

        const TString before(
            bootstrap.RawData.data(),
            bootstrap.RawData.size());
        const auto error = bootstrap.Patch(patch);
        const TString after(bootstrap.RawData.data(), bootstrap.RawData.size());

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("overwrite retained entry"),
            error.GetMessage());
        UNIT_ASSERT_EQUAL_C(
            before,
            after,
            "State file was modified despite patch failure");
    }

    FILE_RING_BUFFER_TEST(ShouldPatchWrappedHighSegmentBoundary)
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

        auto dump = bootstrap.Dump();
        UNIT_ASSERT(!dump.GetIsCorrupted());

        int firstLowEntry = -1;
        for (int i = 0; i < dump.GetEntries().size(); ++i) {
            if (dump.GetEntries(i).GetEntryPos() == 0) {
                firstLowEntry = i;
                break;
            }
        }
        UNIT_ASSERT_LT(0, firstLowEntry);

        const auto& lastHighEntry = dump.GetEntries(firstLowEntry - 1);
        const ui64 highSegmentEnd =
            lastHighEntry.GetEntryPos() +
            bootstrap.Accessor.GetDataProcessor()->GetEntrySize(
                lastHighEntry.GetDataSize());
        dump.MutableHeader()->SetWritePos(highSegmentEnd);

        UNIT_ASSERT_C(!HasError(bootstrap.Patch(dump)), "Patch failed");

        const auto patchedDump = bootstrap.Dump();
        UNIT_ASSERT(!patchedDump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(
            firstLowEntry,
            patchedDump.GetEntries().size());
        for (int i = 0; i < firstLowEntry; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                dump.GetEntries(i).GetEntryPos(),
                patchedDump.GetEntries(i).GetEntryPos());
        }
    }

    FILE_RING_BUFFER_TEST(ShouldPatchEntries)
    {
        ui32 tag = 0;

        TBootstrap bootstrap;
        bootstrap.Execute(
            [&](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(PushBack(ringBuffer, "Hello"));
                UNIT_ASSERT(PushBack(ringBuffer, "What"));
                UNIT_ASSERT(PushBack(ringBuffer, "Bye"));
                tag = Min(2U, ringBuffer.GetMaxTag());
            },
            version);

        bootstrap.Accessor.ValidateAndInitialize();
        auto entryHeader =
            bootstrap.Accessor.GetDataProcessor()->ReadEntryHeader(0);
        entryHeader.DataChecksum ^= 1;
        UNIT_ASSERT(bootstrap.Accessor.GetDataProcessor()->WriteEntryHeader(
            0,
            entryHeader));

        auto dump = bootstrap.Dump();
        UNIT_ASSERT(dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(3, dump.GetEntries().size());

        dump.MutableEntries(0)->SetDataChecksum(
            dump.GetEntries(0).GetActualDataChecksum());
        dump.MutableEntries(1)->SetTag(tag);
        dump.MutableEntries(2)->SetFreeFlag(true);
        UNIT_ASSERT_C(!HasError(bootstrap.Patch(dump)), "Patch failed");

        bootstrap.Execute(
            [&](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(ringBuffer.Validate());
                UNIT_ASSERT_VALUES_EQUAL(
                    TStringBuilder() << "Hello:0,What:" << tag,
                    Dump(ringBuffer));
            },
            version);
    }

    FILE_RING_BUFFER_TEST(ShouldPatchWriteDataRequestFields)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                const TVector<ui64> requestWithPayload{1, 2, 3, 4};
                UNIT_ASSERT(PushBack(ringBuffer, AsBytes(requestWithPayload)));
            },
            version);

        auto dump = bootstrap.Dump();
        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());

        {
            auto overflowingPatch = dump;
            auto* requestInfo = overflowingPatch.MutableEntries(0)
                                    ->MutableWriteDataRequestInfo();
            requestInfo->SetOffset(Max<ui64>() - requestInfo->GetSize() + 1);

            const TString before(
                bootstrap.RawData.data(), bootstrap.RawData.size());
            const auto error = bootstrap.Patch(overflowingPatch);
            const TString after(
                bootstrap.RawData.data(), bootstrap.RawData.size());

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT_C(
                error.GetMessage().Contains("offset and size overflow"),
                error.GetMessage());
            UNIT_ASSERT_EQUAL_C(
                before,
                after,
                "State file was modified despite patch failure");
        }

        auto& requestInfo =
            *dump.MutableEntries(0)->MutableWriteDataRequestInfo();
        requestInfo.SetNodeId(10);
        requestInfo.SetHandle(20);
        requestInfo.SetOffset(Max<ui64>() - requestInfo.GetSize());

        // PatchStateFile must initialize a freshly mapped accessor itself.
        bootstrap.Reopen();
        UNIT_ASSERT_C(!HasError(bootstrap.Patch(dump)), "Patch failed");

        const auto patchedDump = bootstrap.Dump();
        UNIT_ASSERT(!patchedDump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(1, patchedDump.GetEntries().size());

        const auto& patchedInfo =
            patchedDump.GetEntries(0).GetWriteDataRequestInfo();
        UNIT_ASSERT_VALUES_EQUAL(10, patchedInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(20, patchedInfo.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>() - patchedInfo.GetSize(),
            patchedInfo.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(sizeof(ui64), patchedInfo.GetSize());
    }

    FILE_RING_BUFFER_TEST(ShouldAllowFreeingRequestWithOverflowingRange)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                const TVector<ui64> requestWithPayload{
                    1,
                    2,
                    Max<ui64>(),
                    4};
                UNIT_ASSERT(PushBack(ringBuffer, AsBytes(requestWithPayload)));
            },
            version);

        auto patch = bootstrap.Dump();
        UNIT_ASSERT(!patch.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(1, patch.GetEntries().size());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            patch.GetEntries(0).GetWriteDataRequestInfo().GetOffset());
        patch.MutableEntries(0)->SetFreeFlag(true);

        UNIT_ASSERT_C(!HasError(bootstrap.Patch(patch)), "Patch failed");

        const auto dump = bootstrap.Dump();
        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());
        UNIT_ASSERT(dump.GetEntries(0).GetFreeFlag());
    }

    FILE_RING_BUFFER_TEST(ShouldPatchJsonRoundTrip)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(PushBack(ringBuffer, "Hello")); },
            version);

        auto dump = bootstrap.Dump();
        dump.MutableEntries(0)->SetTag(1);

        using EMissingKeyMode =
            NProtobufJson::TProto2JsonConfig::MissingKeyMode;

        NProtobufJson::TProto2JsonConfig config;
        config.SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName)
            .SetFormatOutput(true)
            .SetMissingSingleKeyMode(EMissingKeyMode::MissingKeySkip);

        const auto json = NProtobufJson::Proto2Json(dump, config);
        NProto::TStateFileDump parsedDump;
        NProtobufJson::Json2Proto(TStringBuf(json), parsedDump);

        const auto error = bootstrap.Patch(parsedDump);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT_VALUES_EQUAL("Hello:1", Dump(ringBuffer)); },
            version);
    }

    Y_UNIT_TEST(ShouldRejectPatchForUninitializedStateFile)
    {
        TBootstrap bootstrap;

        const auto error = bootstrap.Patch({});
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("State file is not initialized"),
            error.GetMessage());
    }

    FILE_RING_BUFFER_TEST(ShouldRejectStaleState)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(PushBack(ringBuffer, "Hello")); },
            version);

        const auto dump = bootstrap.Dump();
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(PushBack(ringBuffer, "Bye")); },
            version);

        const auto error = bootstrap.Patch(dump);
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("State file checksum mismatch"),
            error.GetMessage());
    }

    FILE_RING_BUFFER_TEST(ShouldRejectOnFieldsMismatch)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            { UNIT_ASSERT(PushBack(ringBuffer, "Hello")); },
            version);

        const auto dump = bootstrap.Dump();
        auto check = [&](auto mutator, TStringBuf expectedMessage)
        {
            auto newState = dump;
            mutator(newState);

            const TString before(
                bootstrap.RawData.data(),
                bootstrap.RawData.size());
            const auto error = bootstrap.Patch(newState);
            const TString after(
                bootstrap.RawData.data(),
                bootstrap.RawData.size());

            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
            UNIT_ASSERT_C(
                error.GetMessage().Contains(expectedMessage),
                error.GetMessage());
            UNIT_ASSERT_EQUAL_C(
                before,
                after,
                "State file was modified despite patch failure");
        };

        check(
            [](auto& state) { state.SetChecksum(state.GetChecksum() + 1); },
            "State file checksum mismatch");
        check(
            [](auto& state) { state.MutableEntries()->RemoveLast(); },
            "Entry count mismatch");
        check(
            [](auto& state)
            {
                auto* entry = state.MutableEntries(0);
                entry->SetEntryPos(entry->GetEntryPos() + 1);
            },
            "Entry pos mismatch");
        check(
            [](auto& state)
            {
                auto* entry = state.MutableEntries(0);
                entry->SetActualDataChecksum(
                    entry->GetActualDataChecksum() + 1);
            },
            "Entry ActualDataChecksum mismatch");
        check(
            [](auto& state)
            {
                state.SetActualMetadataChecksum(
                    state.GetActualMetadataChecksum() + 1);
            },
            "Actual metadata checksum mismatch");
    }

    FILE_RING_BUFFER_TEST(ShouldRejectPoppedEntryResurrection)
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

        auto dump = bootstrap.Dump();
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());
        UNIT_ASSERT_LT(0, dump.GetEntries(0).GetEntryPos());
        dump.MutableHeader()->SetReadPos(0);

        const TString before(
            bootstrap.RawData.data(),
            bootstrap.RawData.size());
        const auto error = bootstrap.Patch(dump);
        const TString after(bootstrap.RawData.data(), bootstrap.RawData.size());

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("known entry boundary"),
            error.GetMessage());
        UNIT_ASSERT_EQUAL_C(
            before,
            after,
            "State file was modified despite patch failure");
    }

    FILE_RING_BUFFER_TEST(ShouldValidateAllEntriesBeforeMutation)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                UNIT_ASSERT(PushBack(ringBuffer, "Hello"));
                UNIT_ASSERT(PushBack(ringBuffer, "Bye"));
            },
            version);

        const auto initialDump = bootstrap.Dump();
        const ui64 secondEntryPos = initialDump.GetEntries(1).GetEntryPos();
        auto secondHeader =
            bootstrap.Accessor.GetDataProcessor()->ReadEntryHeader(
                secondEntryPos);
        secondHeader.DataSize = 1'000'000;
        UNIT_ASSERT(bootstrap.Accessor.GetDataProcessor()->WriteEntryHeader(
            secondEntryPos,
            secondHeader));

        auto patch = bootstrap.Dump();
        UNIT_ASSERT(patch.GetIsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(2, patch.GetEntries().size());

        patch.MutableHeader()->SetReadPos(0);
        patch.MutableHeader()->SetWritePos(0);
        patch.MutableEntries(0)->SetTag(1);
        patch.MutableEntries(1)->SetDataChecksum(
            patch.GetEntries(1).GetActualDataChecksum());

        const TString before(
            bootstrap.RawData.data(),
            bootstrap.RawData.size());
        const auto error = bootstrap.Patch(patch);
        const TString after(bootstrap.RawData.data(), bootstrap.RawData.size());

        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("not accessible"),
            error.GetMessage());
        UNIT_ASSERT_EQUAL_C(
            before,
            after,
            "State file was modified despite patch failure");
    }

    FILE_RING_BUFFER_TEST(ShouldRejectInvalidChanges)
    {
        TBootstrap bootstrap;
        bootstrap.Execute(
            [](TFileRingBuffer& ringBuffer)
            {
                const TVector<ui64> requestWithPayload{1, 2, 3, 4};
                UNIT_ASSERT(PushBack(ringBuffer, AsBytes(requestWithPayload)));
                UNIT_ASSERT(PushBack(ringBuffer, "Hello"));
            },
            version);

        const auto dump = bootstrap.Dump();
        auto check = [&](auto mutator, TStringBuf expectedMessage)
        {
            auto newState = dump;
            mutator(newState);

            const TString before(
                bootstrap.RawData.data(),
                bootstrap.RawData.size());
            const auto error = bootstrap.Patch(newState);
            const TString after(
                bootstrap.RawData.data(),
                bootstrap.RawData.size());

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT_C(
                error.GetMessage().Contains(expectedMessage),
                error.GetMessage());
            UNIT_ASSERT_EQUAL_C(
                before,
                after,
                "State file was modified despite patch failure");
        };

        check(
            [](auto& state) { state.ClearHeader(); },
            "does not contain a state file header");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetVersion(header->GetVersion() + 1);
            },
            "Version");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetHeaderSize(header->GetHeaderSize() + 1);
            },
            "HeaderSize");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetDataCapacity(header->GetDataCapacity() + 1);
            },
            "DataCapacity");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetDataOffset(header->GetDataOffset() + 1);
            },
            "DataOffset");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetMetadataCapacity(header->GetMetadataCapacity() + 1);
            },
            "MetadataCapacity");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetMetadataOffset(header->GetMetadataOffset() + 1);
            },
            "MetadataOffset");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetMetadataSize(header->GetMetadataSize() + 1);
            },
            "MetadataSize");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetReadPos(header->GetDataCapacity() + 1);
            },
            "ReadPos");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetWritePos(header->GetDataCapacity() + 1);
            },
            "WritePos");
        check(
            [](auto& state) { state.MutableHeader()->SetReadPos(1); },
            version == EVersion::V6 ? "not aligned" : "known entry boundary");
        check(
            [](auto& state) { state.MutableHeader()->SetWritePos(8); },
            "known entry boundary");
        check(
            [](auto& state)
            {
                state.MutableHeader()->SetReadPos(
                    state.GetEntries(1).GetEntryPos());
                state.MutableHeader()->SetWritePos(0);
            },
            "contiguous range of dumped entries");
        check(
            [](auto& state)
            {
                state.MutableHeader()->SetReadPos(
                    state.GetHeader().GetWritePos());
                state.MutableHeader()->SetWritePos(
                    state.GetEntries(1).GetEntryPos());
            },
            "contiguous range of dumped entries");
        check(
            [](auto& state)
            {
                auto* header = state.MutableHeader();
                header->SetMetadataChecksum(header->GetMetadataChecksum() + 1);
            },
            "actual metadata checksum");
        check(
            [](auto& state)
            {
                auto* entry = state.MutableEntries(0);
                entry->SetDataSize(entry->GetDataSize() + 1);
            },
            "Changing entry size");
        check(
            [](auto& state) { state.MutableEntries(0)->SetTag(Max<ui32>()); },
            "exceeds the maximal value");
        check(
            [](auto& state)
            {
                auto* requestInfo =
                    state.MutableEntries(0)->MutableWriteDataRequestInfo();
                requestInfo->SetSize(requestInfo->GetSize() + 1);
            },
            "Changing request size");
        check(
            [](auto& state)
            { state.MutableEntries(1)->MutableWriteDataRequestInfo(); },
            "request data presence");
        check(
            [](auto& state)
            { state.MutableEntries(0)->ClearWriteDataRequestInfo(); },
            "request data presence");
        check(
            [](auto& state)
            {
                auto* entry = state.MutableEntries(0);
                entry->SetDataChecksum(entry->GetDataChecksum() ^ 1);
            },
            "actual data checksum");
        check(
            [](auto& state)
            {
                auto* entry = state.MutableEntries(0);
                entry->SetFreeFlag(true);
                entry->SetDataChecksum(entry->GetDataChecksum() ^ 1);
            },
            "checksum");
        check(
            [](auto& state)
            {
                auto* entry = state.MutableEntries(0);
                entry->MutableWriteDataRequestInfo()->SetHandle(100);
                entry->SetDataChecksum(entry->GetDataChecksum() ^ 1);
            },
            "checksum");
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
