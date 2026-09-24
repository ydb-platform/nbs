#include "options.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

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
        const auto options =
            ParseOptions({"dump", "--state-file", "/tmp/state"});

        UNIT_ASSERT_EQUAL(ECommand::Dump, options.Command);
        UNIT_ASSERT_VALUES_EQUAL("/tmp/state", options.StateFile);
        UNIT_ASSERT_VALUES_EQUAL(
            "/Berkanavt/nfs-vhost/state",
            options.StateDir);
    }

    Y_UNIT_TEST(ShouldRejectStateFileWithLocatorArguments)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions(
                {"dump", "--state-file", "/tmp/state", "--fs-id", "fs"}),
            yexception,
            "Cannot specify --state-file");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions(
                {"dump",
                 "--state-file",
                 "/tmp/state",
                 "--state-dir",
                 "/tmp/states"}),
            yexception,
            "Cannot specify --state-file");
    }

    Y_UNIT_TEST(ShouldRequireStateFileOrFileSystemId)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseOptions({"check"}),
            yexception,
            "--fs-id is required");

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
            yexception,
            "list command accepts --state-dir only");
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
