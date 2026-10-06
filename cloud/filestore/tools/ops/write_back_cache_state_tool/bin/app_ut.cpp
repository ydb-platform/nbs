#include "app.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/ylimits.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/subst.h>
#include <util/system/fs.h>

#include <optional>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBackCacheStateToolAppTest)
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
        auto& request = *entry.MutableWriteDataRequestInfo();
        request.SetNodeId(Max<ui64>());
        request.SetHandle(Max<ui64>() - 1);
        request.SetOffset(Max<ui64>() - 2);
        request.SetSize(1);

        TStringStream output;
        WriteStateFileDumpJson(state, output);
        const auto json = output.Str();
        UNIT_ASSERT_STRING_CONTAINS(
            json,
            "\"18446744073709551615\"");

        NProto::TStateFileDump parsed;
        ReadJson(json, parsed);
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            parsed.GetEntries(0).GetWriteDataRequestInfo().GetNodeId());

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

        const auto outputFile = tempDir.Path() / "output";
        UNIT_ASSERT(NFs::HardLink(stateFile.GetPath(), outputFile.GetPath()));

        TOptions options;
        options.Command = ECommand::List;
        options.StateDir = (tempDir.Path() / "state").GetPath();
        options.OutputFile = outputFile.GetPath();

        UNIT_ASSERT_EXCEPTION(AppMain(options), yexception);
        UNIT_ASSERT_VALUES_EQUAL("state", TFileInput(stateFile).ReadAll());
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
