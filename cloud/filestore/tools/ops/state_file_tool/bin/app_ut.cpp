#include "app.h"

#include <cloud/filestore/tools/ops/state_file_tool/lib/file_lock.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/ylimits.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/subst.h>

#include <cstddef>
#include <optional>

namespace NCloud::NFileStore::NStateFileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString MakeDumpJson(
    std::optional<TStringBuf> readPos = "0",
    std::optional<TStringBuf> writePos = "0",
    TStringBuf tag = "0")
{
    TStringBuilder json;
    json << R"({
        "Checksum": 1,
        "IsCorrupted": false,
        "Header": {
            "Version": 5,
            "HeaderSize": 80,
            "MetadataOffset": "80",
            "MetadataCapacity": "8",
            "MetadataSize": 0,
            "MetadataChecksum": 0,
            "DataOffset": "88",
            "DataCapacity": "64"
)";
    if (readPos) {
        json << ",\n\"ReadPos\": " << *readPos;
    }
    if (writePos) {
        json << ",\n\"WritePos\": " << *writePos;
    }
    json << R"(
        },
        "Entries": [{
            "DataSize": 8,
            "DataChecksum": 0,
            "Tag": )"
         << tag << R"(,
            "FreeFlag": false,
            "EntryPos": "0"
        }]
    })";
    return json;
}

void ReadJson(const TString& json, NProto::TStateFileDump& state)
{
    TStringInput input(json);
    ReadStateFileDumpJson(input, state);
}

void CreateStateFile(const TFsPath& path)
{
    path.Parent().MkDirs();
    TFile(path.GetPath(), CreateNew | RdWr);
    const auto layout = TFileRingBufferLayout::FromCapacity(64, 8);
    UNIT_ASSERT(layout);
    TFileRingBuffer ringBuffer(
        path.GetPath(),
        layout->StateFileSize,
        8,
        EFileRingBufferVersion::V6);
    UNIT_ASSERT(ringBuffer.Validate());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStateFileToolAppTest)
{
    Y_UNIT_TEST(ShouldRequireCursorFields)
    {
        NProto::TStateFileDump state;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ReadJson(MakeDumpJson(std::nullopt, std::nullopt), state),
            yexception,
            "Failed to parse state file dump JSON");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ReadJson(MakeDumpJson("null", "null"), state),
            yexception,
            "Failed to parse state file dump JSON");

        ReadJson(MakeDumpJson("0", "0"), state);
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetHeader().GetReadPos());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetHeader().GetWritePos());
    }

    Y_UNIT_TEST(ShouldRejectOutOfRangeUnsignedIntegers)
    {
        NProto::TStateFileDump state;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ReadJson(MakeDumpJson("0", "0", "4294967297"), state),
            yexception,
            "Failed to parse state file dump JSON");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ReadJson(MakeDumpJson("0", "0", "-4294967295"), state),
            yexception,
            "Failed to parse state file dump JSON");
    }

    Y_UNIT_TEST(ShouldPreserveUnsigned64BitValues)
    {
        NProto::TStateFileDump state;
        state.SetChecksum(1);
        state.SetIsCorrupted(false);
        auto& header = *state.MutableHeader();
        header.SetVersion(5);
        header.SetHeaderSize(80);
        header.SetMetadataOffset(80);
        header.SetMetadataCapacity(8);
        header.SetMetadataSize(0);
        header.SetMetadataChecksum(0);
        header.SetDataOffset(88);
        header.SetDataCapacity(64);
        header.SetReadPos(0);
        header.SetWritePos(0);

        auto& entry = *state.AddEntries();
        entry.SetDataSize(8);
        entry.SetDataChecksum(0);
        entry.SetTag(0);
        entry.SetFreeFlag(false);
        entry.SetEntryPos(0);
        auto& request = *entry.MutableWriteDataRequestMetadata();
        request.SetNodeId(Max<ui64>());
        request.SetHandle(Max<ui64>() - 1);
        request.SetOffset(Max<ui64>() - 2);
        request.SetSize(1);

        TStringStream output;
        WriteStateFileDumpJson(state, output);
        const auto json = output.Str();
        UNIT_ASSERT_STRING_CONTAINS(json, "\"18446744073709551615\"");

        NProto::TStateFileDump parsed;
        ReadJson(json, parsed);
        UNIT_ASSERT(!parsed.HasActualMetadataChecksum());
        UNIT_ASSERT(!parsed.GetEntries(0).HasActualDataChecksum());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            parsed.GetEntries(0).GetWriteDataRequestMetadata().GetNodeId());

        TString invalid = json;
        SubstGlobal(invalid, "18446744073709551615", "18446744073709551616");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ReadJson(invalid, parsed),
            yexception,
            "Failed to parse state file dump JSON");
    }

    Y_UNIT_TEST(ShouldNotOverwriteExistingFileWithListOutput)
    {
        TTempDir tempDir;
        const auto stateFile =
            tempDir.Path() / "state" / "fs" / "session" / "write_back_cache";
        stateFile.Parent().MkDirs();
        TFileOutput(stateFile).Write("state");

        TOptions options;
        options.Command = ECommand::List;
        options.StateDir = (tempDir.Path() / "state").GetPath();
        options.OutputFile = stateFile.GetPath();

        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));
        UNIT_ASSERT_VALUES_EQUAL("state", TFileInput(stateFile).ReadAll());
    }

    Y_UNIT_TEST(ShouldNotOverwriteExistingFileWithDumpOutput)
    {
        TTempDir tempDir;
        const auto stateFile = tempDir.Path() / "write_back_cache";
        CreateStateFile(stateFile);
        const auto state = TFileInput(stateFile).ReadAll();

        TOptions options;
        options.Command = ECommand::Dump;
        options.StateFile = stateFile.GetPath();
        options.FileType = NProto::EStateFileType::WriteBackCache;
        options.OutputFile = stateFile.GetPath();
        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));
        UNIT_ASSERT_VALUES_EQUAL(state, TFileInput(stateFile).ReadAll());
    }

    Y_UNIT_TEST(ShouldReportInvalidPatchInput)
    {
        TTempDir tempDir;
        const auto inputFile = tempDir.Path() / "patch.json";
        const auto stateFile = tempDir.Path() / "write_back_cache";
        CreateStateFile(stateFile);

        TOptions options;
        options.Command = ECommand::Patch;
        options.InputFile = inputFile.GetPath();
        options.StateFile = stateFile.GetPath();
        options.FileType = NProto::EStateFileType::WriteBackCache;

        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));

        TFileOutput(inputFile).Write("{");
        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));
    }

    Y_UNIT_TEST(ShouldDumpLockedStateFileOnlyWithOverride)
    {
        TTempDir tempDir;
        const auto stateFile = tempDir.Path() / "copied-state.bin";
        CreateStateFile(stateFile);
        const auto state = TFileInput(stateFile).ReadAll();

        TFile owner(stateFile.GetPath(), OpenExisting | RdOnly);
        const auto acquired = TryLock(owner);
        UNIT_ASSERT_C(!HasError(acquired), FormatError(acquired.GetError()));
        UNIT_ASSERT(acquired.GetResult());

        const auto outputFile = tempDir.Path() / "dump.json";
        TOptions options;
        options.Command = ECommand::Dump;
        options.StateFile = stateFile.GetPath();
        options.FileType = NProto::EStateFileType::WriteBackCache;
        options.OutputFile = outputFile.GetPath();

        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));
        UNIT_ASSERT(!outputFile.Exists());
        options.UnsafeIgnoreLock = true;
        UNIT_ASSERT_VALUES_EQUAL(0, AppMain(options));
        NProto::TStateFileDump dump;
        ReadJson(TFileInput(outputFile).ReadAll(), dump);
        UNIT_ASSERT(!dump.GetIsCorrupted());
        UNIT_ASSERT(dump.HasHeader());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(EFileRingBufferVersion::V6),
            dump.GetHeader().GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(state, TFileInput(stateFile).ReadAll());
    }

    Y_UNIT_TEST(ShouldRequireSupportedTypeForDirectStateFile)
    {
        TTempDir tempDir;
        const auto stateFile = tempDir.Path() / "handle_ops_queue";
        CreateStateFile(stateFile);
        const auto state = TFileInput(stateFile).ReadAll();
        const auto outputFile = tempDir.Path() / "dump.json";

        TOptions options;
        options.Command = ECommand::Dump;
        options.StateFile = stateFile.GetPath();
        options.OutputFile = outputFile.GetPath();
        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));

        options.FileType = NProto::EStateFileType::HandleOpsQueue;
        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));
        UNIT_ASSERT(!outputFile.Exists());
        UNIT_ASSERT_VALUES_EQUAL(state, TFileInput(stateFile).ReadAll());
    }

    Y_UNIT_TEST(ShouldDumpLocatedStateFile)
    {
        TTempDir tempDir;
        const auto stateDir = tempDir.Path() / "state";
        CreateStateFile(stateDir / "fs" / "session" / "write_back_cache");

        TOptions options;
        options.Command = ECommand::Dump;
        options.StateDir = stateDir.GetPath();
        options.FsId = "fs";
        options.OutputFile = (tempDir.Path() / "dump.json").GetPath();
        UNIT_ASSERT_VALUES_EQUAL(0, AppMain(options));
    }

    Y_UNIT_TEST(ShouldCheckStateFileStatus)
    {
        TTempDir tempDir;
        const auto stateFile = tempDir.Path() / "write_back_cache";
        TFile file(stateFile.GetPath(), CreateNew | RdWr);

        TOptions options;
        options.Command = ECommand::Check;
        options.StateFile = stateFile.GetPath();
        options.FileType = NProto::EStateFileType::WriteBackCache;

        UNIT_ASSERT_VALUES_EQUAL(0, AppMain(options));
        file.Resize(sizeof(TFileRingBufferHeader) + 64);
        UNIT_ASSERT_VALUES_EQUAL(0, AppMain(options));
        file.Close();

        const auto initializedFile = tempDir.Path() / "initialized";
        CreateStateFile(initializedFile);
        options.StateFile = initializedFile.GetPath();
        UNIT_ASSERT_VALUES_EQUAL(0, AppMain(options));

        TFileOutput(initializedFile).Write("broken");
        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));
    }

    Y_UNIT_TEST(ShouldRequireCorruptionOverrideToRepairStateFile)
    {
        TTempDir tempDir;
        const auto stateFile = tempDir.Path() / "write_back_cache";
        CreateStateFile(stateFile);
        {
            TFile file(stateFile.GetPath(), OpenExisting | RdWr);
            const ui32 badChecksum = 1;
            file.Pwrite(
                &badChecksum,
                sizeof(badChecksum),
                offsetof(TFileRingBufferHeader, MetadataChecksum));
        }
        const auto corruptedState = TFileInput(stateFile).ReadAll();

        const auto dumpFile = tempDir.Path() / "dump.json";
        TOptions options;
        options.Command = ECommand::Dump;
        options.StateFile = stateFile.GetPath();
        options.FileType = NProto::EStateFileType::WriteBackCache;
        options.OutputFile = dumpFile.GetPath();
        UNIT_ASSERT_VALUES_EQUAL(0, AppMain(options));

        NProto::TStateFileDump patch;
        ReadJson(TFileInput(dumpFile).ReadAll(), patch);
        UNIT_ASSERT(patch.GetIsCorrupted());
        UNIT_ASSERT(patch.HasActualMetadataChecksum());
        patch.MutableHeader()->SetMetadataChecksum(
            patch.GetActualMetadataChecksum());

        const auto patchFile = tempDir.Path() / "patch.json";
        {
            TFileOutput output(patchFile);
            WriteStateFileDumpJson(patch, output);
        }

        options.Command = ECommand::Patch;
        options.OutputFile.clear();
        options.InputFile = patchFile.GetPath();
        UNIT_ASSERT_VALUES_EQUAL(1, AppMain(options));
        UNIT_ASSERT_VALUES_EQUAL(
            corruptedState,
            TFileInput(stateFile).ReadAll());

        options.UnsafeIgnoreCorruption = true;
        UNIT_ASSERT_VALUES_EQUAL(0, AppMain(options));

        options.Command = ECommand::Check;
        options.InputFile.clear();
        options.UnsafeIgnoreCorruption = false;
        UNIT_ASSERT_VALUES_EQUAL(0, AppMain(options));
    }
}

}   // namespace NCloud::NFileStore::NStateFileTool
