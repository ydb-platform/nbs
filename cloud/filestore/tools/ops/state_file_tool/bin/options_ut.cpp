#include "options.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NCloud::NFileStore::NStateFileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

TOptions ParseOptions(std::initializer_list<TStringBuf> args)
{
    TVector<TString> ownedArgs = {"state-tool"};
    for (const auto arg: args) {
        ownedArgs.emplace_back(arg);
    }

    TVector<char*> argv;
    argv.reserve(ownedArgs.size());
    for (auto& arg: ownedArgs) {
        argv.push_back(arg.begin());
    }

    TOptions options;
    options.Parse(argv.size(), argv.data());
    return options;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOptionsTest)
{
    Y_UNIT_TEST(ShouldAcceptStateFileWithoutRejectingDefaultStateDir)
    {
        const auto options = ParseOptions(
            {"dump", "--state-file", "/tmp/state",
             "--file-type", "write-back-cache"});

        UNIT_ASSERT_EQUAL(ECommand::Dump, options.Command);
        UNIT_ASSERT_VALUES_EQUAL("/tmp/state", options.StateFile);
        UNIT_ASSERT_EQUAL(NProto::EStateFileType::WriteBackCache, options.FileType);
        UNIT_ASSERT_VALUES_EQUAL(
            "/Berkanavt/nfs-vhost/state", options.StateDir);
    }

    Y_UNIT_TEST(ShouldRejectStateFileWithLocatorArguments)
    {
        for (const TStringBuf option:
             {"--fs-id", "--state-dir", "--session-id"})
        {
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                ParseOptions(
                    {"dump", "--state-file", "/tmp/state", option, "value"}),
                yexception, "Cannot specify --state-file");
        }
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedFileType)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions(
                {"dump", "--state-file", "/tmp/state",
                 "--file-type", "handle-ops-queue"}),
            yexception,
            "Unsupported state file type");
    }

    Y_UNIT_TEST(ShouldRequireStateFileOrFileSystemId)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions({"check"}), yexception, "--fs-id is required");

        const auto options = ParseOptions({"check", "--fs-id", "fs-1"});
        UNIT_ASSERT_EQUAL(ECommand::Check, options.Command);
        UNIT_ASSERT_VALUES_EQUAL("fs-1", options.FsId);
    }

    Y_UNIT_TEST(ShouldKeepListDirectoryOriented)
    {
        const auto options =
            ParseOptions({"list", "--state-dir", "/tmp/states"});

        UNIT_ASSERT_EQUAL(ECommand::List, options.Command);
        UNIT_ASSERT_VALUES_EQUAL("/tmp/states", options.StateDir);

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions({"list", "--state-file", "/tmp/state"}),
            yexception, "list command accepts --state-dir only");
    }

    Y_UNIT_TEST(ShouldRejectOptionsForOtherCommands)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions({"list", "--input", ""}),
            yexception,
            "--input is only supported by patch");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions({"dump", "--fs-id", "fs", "--input", "patch.json"}),
            yexception,
            "--input is only supported by patch");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions(
                {"check", "--state-file", "/tmp/state",
                 "--file-type", "write-back-cache", "--output", ""}),
            yexception,
            "--output is only supported by list and dump");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions(
                {"patch", "--fs-id", "fs", "--output", "dump.json"}),
            yexception,
            "--output is only supported by list and dump");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions({"list", "--unsafe-ignore-lock"}),
            yexception,
            "--unsafe-ignore-lock is not supported by list");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions({"dump", "--fs-id", "fs", "--unsafe-ignore-corruption"}),
            yexception,
            "--unsafe-ignore-corruption is only supported by patch");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions({"list", "--file-type", "write-back-cache"}),
            yexception,
            "--file-type is not supported by list");
    }

    Y_UNIT_TEST(ShouldAcceptOptionsForSupportedCommands)
    {
        const auto list = ParseOptions({"list", "--output", "list.json"});
        UNIT_ASSERT_VALUES_EQUAL("list.json", list.OutputFile);

        const auto dump = ParseOptions(
            {"dump", "--state-file", "/tmp/state",
             "--file-type", "write-back-cache", "--output", "dump.json",
             "--unsafe-ignore-lock"});
        UNIT_ASSERT_VALUES_EQUAL("dump.json", dump.OutputFile);
        UNIT_ASSERT(dump.UnsafeIgnoreLock);

        const auto patch = ParseOptions(
            {"patch", "--fs-id", "fs", "--input", "patch.json",
             "--unsafe-ignore-lock", "--unsafe-ignore-corruption"});
        UNIT_ASSERT_VALUES_EQUAL("patch.json", patch.InputFile);
        UNIT_ASSERT(patch.UnsafeIgnoreLock);
        UNIT_ASSERT(patch.UnsafeIgnoreCorruption);

        const auto check =
            ParseOptions({"check", "--fs-id", "fs", "--unsafe-ignore-lock"});
        UNIT_ASSERT(check.UnsafeIgnoreLock);
    }
}

}   // namespace NCloud::NFileStore::NStateFileTool
