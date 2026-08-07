#include "state_file_processor.h"

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer.h>

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

using EVersion = EFileRingBufferVersion;

#define FILE_RING_BUFFER_TEST(name)    \
    void TestImpl##name(EVersion ver); \
    Y_UNIT_TEST(name##V5)              \
    {                                  \
        TestImpl##name(EVersion::V5);  \
    }                                  \
    Y_UNIT_TEST(name##V6)              \
    {                                  \
        TestImpl##name(EVersion::V6);  \
    }                                  \
    void TestImpl##name(EVersion ver)
// FILE_RING_BUFFER_TEST

const void* Alloc(TFileRingBuffer& rb, const TString& entry)
{
    auto resultOrError = rb.Alloc(entry.size());
    UNIT_ASSERT(!HasError(resultOrError));
    char* ptr = resultOrError.GetResult();
    MemCopy(ptr, entry.data(), entry.size());
    UNIT_ASSERT(rb.Commit());
    return ptr;
}

TString Dump(TFileRingBuffer& rb)
{
    TStringBuilder sb;
    rb.Visit(
        [&](ui32 checksum, ui32 tag, TStringBuf entry)
        {
            Y_UNUSED(checksum);
            if (!sb.empty()) {
                sb << ",";
            }
            sb << entry << ":" << tag;
        });
    return sb;
}

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    TTempFileHandle TempFileHandle;
    TFileMapFileRingBufferAccessor Accessor;
    std::span<char> RawData;

    static constexpr ui64 DataCapacity = 64;
    static constexpr ui64 MetadataCapacity = 8;

    TBootstrap()
        : Accessor(TempFileHandle.Name(), /* readOnly = */ false)
    {
        Remap();
    }

    std::span<char> GetRawData()
    {
        return Accessor.GetRawData();
    }

    void Execute(
        const TFunctionRef<void(TFileRingBuffer&)>& fn,
        EFileRingBufferVersion version)
    {
        TFileRingBuffer rb(
            TempFileHandle.Name(),
            DataCapacity,
            MetadataCapacity,
            version);

        fn(rb);

        // TFileRingBuffer may resize the file
        // TFileMap does not update its mapping automatically - need to reopen
        Accessor.Close();
        Remap();
    }

    void ResizeAndRemap(size_t size)
    {
        auto status = Accessor.ResizeAndRemap(size);
        UNIT_ASSERT(!HasError(status));
        RawData = Accessor.GetRawData();
    }

    NProto::TStateFileDump Dump()
    {
        return TStateFileProcessor::DumpStateFile(Accessor);
    }

    NCloud::NProto::TError Patch(const NProto::TStateFileDump& newState)
    {
        Accessor.ValidateAndInitialize();
        return TStateFileProcessor::PatchStateFile(Accessor, newState);
    }

private:
    void Remap()
    {
        auto status = Accessor.Map();
        UNIT_ASSERT(!HasError(status));
        RawData = Accessor.GetRawData();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStateFileProcessorTest)
{
    // Dump tests

    Y_UNIT_TEST(ShouldDumpUninitializedAndMalformedStateFiles)
    {
        {
            // Empty file
            TBootstrap b;
            auto dump = b.Dump();

            UNIT_ASSERT(!dump.GetIsCorrupted());
            UNIT_ASSERT(!dump.HasHeader());
            UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());
        }

        {
            // Incorrect file
            TBootstrap b;
            b.ResizeAndRemap(1);
            b.RawData[0] = 1;

            auto dump = b.Dump();

            UNIT_ASSERT(dump.GetIsCorrupted());
            UNIT_ASSERT(!dump.HasHeader());
            UNIT_ASSERT_VALUES_EQUAL(
                Crc32c(b.RawData.data(), b.RawData.size()),
                dump.GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());
        }

        {
            // Zero header
            TBootstrap b;
            b.ResizeAndRemap(sizeof(TFileRingBufferHeader));

            auto dump = b.Dump();

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
        TBootstrap b;

        b.Execute([](TFileRingBuffer& rb) { Y_UNUSED(rb); }, ver);

        auto dump = b.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(0, dump.GetEntries().size());

        const auto& header = dump.GetHeader();

        UNIT_ASSERT_VALUES_EQUAL(
            TBootstrap::DataCapacity,
            header.GetDataCapacity());

        UNIT_ASSERT_VALUES_EQUAL(
            TBootstrap::MetadataCapacity,
            header.GetMetadataCapacity());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpFileWithSingleEntry)
    {
        TBootstrap b;

        b.Execute([](TFileRingBuffer& rb) { rb.PushBack("Hello"); }, ver);

        auto dump = b.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());

        const auto& entry = dump.GetEntries(0);

        const ui32 checksum = Crc32c("Hello", 5);

        UNIT_ASSERT_VALUES_EQUAL(0, entry.GetEntryPos());
        UNIT_ASSERT_VALUES_EQUAL(5, entry.GetDataSize());
        UNIT_ASSERT_VALUES_EQUAL(checksum, entry.GetDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(checksum, entry.GetActualDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(0, entry.GetTag());
        UNIT_ASSERT(!entry.GetFreeFlag());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpFileWithTwoPushedAndOnePoppedEntry)
    {
        TBootstrap b;

        b.Execute(
            [](TFileRingBuffer& rb)
            {
                rb.PushBack("Hello");
                rb.PushBack("Bye");
                rb.PopFront();
            },
            ver);

        auto dump = b.Dump();

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
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                const void* p0 = Alloc(rb, "Hello");
                const void* p1 = Alloc(rb, "What");
                const void* p2 = Alloc(rb, "Bye");

                UNIT_ASSERT(p0 != nullptr);
                UNIT_ASSERT(p1 != nullptr);
                UNIT_ASSERT(p2 != nullptr);

                rb.Free(p1);
            },
            ver);

        auto dump = b.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(3, dump.GetEntries().size());

        const auto& entry0 = dump.GetEntries(0);
        const auto& entry1 = dump.GetEntries(1);
        const auto& entry2 = dump.GetEntries(2);

        UNIT_ASSERT(!entry0.GetFreeFlag());
        UNIT_ASSERT(entry1.GetFreeFlag());
        UNIT_ASSERT(!entry2.GetFreeFlag());

        UNIT_ASSERT_VALUES_EQUAL(4, entry1.GetDataSize());
        UNIT_ASSERT_VALUES_EQUAL(0, entry1.GetActualDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(0, entry1.GetDataChecksum());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpFileWithTaggedEntries)
    {
        ui32 expected0 = 0;
        ui32 expected1 = 0;

        TBootstrap b;
        b.Execute(
            [&](TFileRingBuffer& rb)
            {
                const void* p0 = Alloc(rb, "Hello");
                const void* p1 = Alloc(rb, "Bye");

                UNIT_ASSERT(p0 != nullptr);
                UNIT_ASSERT(p1 != nullptr);

                expected0 = Min(1U, rb.GetMaxTag());
                expected1 = Min(2U, rb.GetMaxTag());

                rb.SetTag(p0, expected0);
                rb.SetTag(p1, expected1);
            },
            ver);

        auto dump = b.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(2, dump.GetEntries().size());

        const auto& entry0 = dump.GetEntries(0);
        const auto& entry1 = dump.GetEntries(1);

        UNIT_ASSERT_VALUES_EQUAL(expected0, entry0.GetTag());
        UNIT_ASSERT_VALUES_EQUAL(expected1, entry1.GetTag());
    }

    FILE_RING_BUFFER_TEST(ShouldDumpWrappedStateFile)
    {
        size_t entryCount = 0;

        TBootstrap b;
        b.Execute(
            [&](TFileRingBuffer& rb)
            {
                while (rb.PushBack("123")) {
                    entryCount++;
                }

                rb.PopFront();
                rb.PopFront();

                UNIT_ASSERT(!rb.Empty());

                UNIT_ASSERT(rb.PushBack("ABCD"));
            },
            ver);

        UNIT_ASSERT_LT(2, entryCount);

        entryCount--;

        auto dump = b.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
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
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                rb.PushBack("Hello");
                rb.PushBack("Bye");
            },
            ver);

        auto dump = b.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(2, dump.GetEntries().size());

        auto entryPos = dump.GetEntries(1).GetEntryPos();

        auto header = b.Accessor.GetDataProcessor()->ReadEntryHeader(entryPos);

        // 1. Corrupted checksum

        auto headerWithCorruptedChecksum = header;
        headerWithCorruptedChecksum.DataChecksum ^= 1;

        b.Accessor.ValidateAndInitialize();
        UNIT_ASSERT(b.Accessor.IsInitialized());

        b.Accessor.GetDataProcessor()->WriteEntryHeader(
            entryPos,
            headerWithCorruptedChecksum);

        auto dump1 = b.Dump();

        UNIT_ASSERT(dump1.GetIsCorrupted());
        UNIT_ASSERT(dump1.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(2, dump1.GetEntries().size());

        const auto& entry1 = dump1.GetEntries(1);
        UNIT_ASSERT_VALUES_UNEQUAL(
            entry1.GetDataChecksum(),
            entry1.GetActualDataChecksum());

        b.Execute(
            [](TFileRingBuffer& rb)
            {
                UNIT_ASSERT(rb.IsCorrupted());
            },
            ver);

        // 2. Corrupted entry size

        auto headerWithCorruptedSize = header;
        headerWithCorruptedSize.DataSize = 1000000;

        b.Accessor.ValidateAndInitialize();
        UNIT_ASSERT(b.Accessor.IsInitialized());

        b.Accessor.GetDataProcessor()->WriteEntryHeader(
            entryPos,
            headerWithCorruptedSize);

        auto dump2 = b.Dump();

        UNIT_ASSERT(dump2.GetIsCorrupted());
        UNIT_ASSERT(dump2.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(2, dump2.GetEntries().size());

        b.Execute(
            [](TFileRingBuffer& rb)
            {
                UNIT_ASSERT(rb.IsCorrupted());
            },
            ver);
    }

    // Patch tests

    FILE_RING_BUFFER_TEST(ShouldPatchHeader)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                rb.PushBack("Hello");
                rb.PushBack("What");
                rb.PushBack("Bye");
            },
            ver);

        auto dump = b.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(3, dump.GetEntries().size());

        dump.MutableHeader()->SetReadPos(dump.GetEntries(1).GetEntryPos());
        dump.MutableHeader()->SetWritePos(dump.GetEntries(2).GetEntryPos());

        UNIT_ASSERT(!HasError(b.Patch(dump)));

        auto newDump = b.Dump();

        UNIT_ASSERT(!newDump.GetIsCorrupted());
        UNIT_ASSERT(newDump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(1, newDump.GetEntries().size());

        b.Execute(
            [](TFileRingBuffer& rb)
            {
                UNIT_ASSERT(!rb.IsCorrupted());
                UNIT_ASSERT_VALUES_EQUAL("What:0", Dump(rb));
            },
            ver);
    }

    FILE_RING_BUFFER_TEST(ShouldPatchEntries)
    {
        ui32 tag = 0;

        TBootstrap b;
        b.Execute(
            [&](TFileRingBuffer& rb)
            {
                rb.PushBack("Hello");
                rb.PushBack("What");
                rb.PushBack("Bye");

                tag = Min(2U, rb.GetMaxTag());
            },
            ver);

        // Corrupt checksum for the first entry

        b.Accessor.ValidateAndInitialize();
        UNIT_ASSERT(b.Accessor.IsInitialized());

        auto header = b.Accessor.GetDataProcessor()->ReadEntryHeader(0);
        header.DataChecksum ^= 1;

        b.Accessor.GetDataProcessor()->WriteEntryHeader(0, header);

        // Patch:
        // - fix checksum;
        // - change tag;
        // - delete last entry.

        auto dump = b.Dump();

        UNIT_ASSERT(dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(3, dump.GetEntries().size());

        dump.MutableEntries(0)->SetDataChecksum(header.DataChecksum ^ 1);
        dump.MutableEntries(1)->SetTag(tag);
        dump.MutableEntries(2)->SetFreeFlag(true);

        UNIT_ASSERT(!HasError(b.Patch(dump)));

        b.Execute(
            [&](TFileRingBuffer& rb)
            {
                UNIT_ASSERT(rb.Validate().empty());
                UNIT_ASSERT_VALUES_EQUAL(
                    "Hello:0,What:" + ToString(tag),
                    Dump(rb));
            },
            ver);
    }

    FILE_RING_BUFFER_TEST(ShouldDumpAndPatchWriteDataRequestFields)
    {
        TBootstrap b;
        b.Execute(
            [&](TFileRingBuffer& rb)
            {
                TVector<ui64> payload {1, 2, 3, 4};
                rb.PushBack(
                    {reinterpret_cast<char*>(payload.data()),
                     payload.size() * sizeof(ui64)});
            },
            ver);

        auto dump = b.Dump();

        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(1, dump.GetEntries().size());

        auto& entry = *dump.MutableEntries(0);
        UNIT_ASSERT(entry.HasWriteDataRequestInfo());

        auto& info = *entry.MutableWriteDataRequestInfo();

        UNIT_ASSERT_VALUES_EQUAL(1, info.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(2, info.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(3, info.GetOffset());

        info.SetNodeId(10);
        info.SetHandle(20);
        info.SetOffset(30);

        UNIT_ASSERT(!HasError(b.Patch(dump)));

        auto dump2 = b.Dump();

        UNIT_ASSERT(!dump2.GetIsCorrupted());
        UNIT_ASSERT(dump2.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(1, dump2.GetEntries().size());

        const auto& entry2 = dump2.GetEntries(0);
        UNIT_ASSERT(entry2.HasWriteDataRequestInfo());

        const auto& info2 = entry2.GetWriteDataRequestInfo();

        UNIT_ASSERT_VALUES_EQUAL(10, info2.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(20, info2.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(30, info2.GetOffset());
    }

    Y_UNIT_TEST(ShouldRejectPatchForUninitializedStateFile)
    {
        TBootstrap b;

        const auto error = b.Patch({});

        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        UNIT_ASSERT(
            error.GetMessage().Contains("State file is not initialized"));
    }

    FILE_RING_BUFFER_TEST(ShouldRejectStaleState)
    {
        TBootstrap b;
        b.Execute([&](TFileRingBuffer& rb) { rb.PushBack("Hello"); }, ver);

        auto dump = b.Dump();

        b.Execute([&](TFileRingBuffer& rb) { rb.PushBack("Bye"); }, ver);

        auto error = b.Patch(dump);

        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("State file checksum mismatch"),
            error.GetMessage());
    }

    FILE_RING_BUFFER_TEST(ShouldRejectOnFieldsMismatch)
    {
        TBootstrap b;
        b.Execute([&](TFileRingBuffer& rb) { rb.PushBack("Hello"); }, ver);

        auto dump = b.Dump();

        auto check = [&](auto mutator, TStringBuf expectedMessage)
        {
            auto newState = dump;
            mutator(newState);

            auto prePatch = TString(b.RawData.data(), b.RawData.size());
            auto error = b.Patch(newState);
            auto postPatch = TString(b.RawData.data(), b.RawData.size());

            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
            UNIT_ASSERT_C(
                error.GetMessage().Contains(expectedMessage),
                error.GetMessage());

            UNIT_ASSERT_EQUAL_C(
                prePatch,
                postPatch,
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
    }

    FILE_RING_BUFFER_TEST(ShouldRejectInvalidChanges)
    {
        TBootstrap b;
        b.Execute(
            [](TFileRingBuffer& rb)
            {
                TVector<ui64> payload{1, 2, 3, 4};
                rb.PushBack(
                    {reinterpret_cast<char*>(payload.data()),
                     payload.size() * sizeof(ui64)});

                // This request is too short to be represented as a WriteData
                // request
                rb.PushBack("Hello");
            },
            ver);

        auto dump = b.Dump();

        auto check = [&](auto mutator, TStringBuf expectedMessage)
        {
            auto newState = dump;
            mutator(newState);

            auto prePatch = TString(b.RawData.data(), b.RawData.size());
            auto error = b.Patch(newState);
            auto postPatch = TString(b.RawData.data(), b.RawData.size());

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT_C(
                error.GetMessage().Contains(expectedMessage),
                error.GetMessage());

            UNIT_ASSERT_EQUAL_C(
                prePatch,
                postPatch,
                "State file was modified despite patch failure");
        };

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
            "it is not present in the current state");

        check(
            [](auto& state)
            {
                auto* entry = state.MutableEntries(0);
                entry->SetFreeFlag(true);
                entry->SetDataChecksum(entry->GetDataChecksum() + 1);
            },
            "checksum mismatch");

        check(
            [](auto& state)
            {
                auto* entry = state.MutableEntries(0);
                entry->MutableWriteDataRequestInfo()->SetHandle(100);
                entry->SetDataChecksum(entry->GetDataChecksum() + 1);
            },
            "checksum mismatch");
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
