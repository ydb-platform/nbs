#include "app.h"

#include "util/stream/file.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCgroupPudWriterTest)
{
    Y_UNIT_TEST(ShouldCheckThatCgroupFilesInSpecifiedFolder)
    {
        TTempDir tempDir;
        const TFsPath& cgroupRootPath = tempDir.Path();

        (cgroupRootPath / "folder").MkDir();
        (cgroupRootPath / "folder" / "cgroup.procs").Touch();

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            AppMain("some_other_path", {100, {cgroupRootPath / "folder"}}));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            AppMain(cgroupRootPath, {100, {cgroupRootPath / "folder"}}));
    }

    Y_UNIT_TEST(AddToCgroupTest)
    {
        TTempDir tempDir;
        const TFsPath& cgroupRootPath = tempDir.Path();

        TFsPath firstCgroup = cgroupRootPath / "first_cgroup";
        firstCgroup.MkDir();
        TFsPath secondCgroup = cgroupRootPath / "second_cgroup";
        secondCgroup.MkDir();
        UNIT_ASSERT_EQUAL(
            1,
            AppMain(cgroupRootPath, {100, {firstCgroup, secondCgroup}}));
        (firstCgroup / "cgroup.procs").Touch();
        (secondCgroup / "cgroup.procs").Touch();

        UNIT_ASSERT_EQUAL(
            0,
            AppMain(cgroupRootPath, {100, {firstCgroup, secondCgroup}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n",
            TFileInput(firstCgroup / "cgroup.procs").ReadAll());
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n",
            TFileInput(secondCgroup / "cgroup.procs").ReadAll());

        UNIT_ASSERT_EQUAL(
            0,
            AppMain(cgroupRootPath, {150, {firstCgroup, secondCgroup}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n150\n",
            TFileInput(firstCgroup / "cgroup.procs").ReadAll());
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n150\n",
            TFileInput(secondCgroup / "cgroup.procs").ReadAll());
    }

    Y_UNIT_TEST(ShouldParseOptions)
    {
        TTempDir tempDir;
        const TFsPath& cgroupRootPath = tempDir.Path();
        TVector<TFsPath> cgroups{
            cgroupRootPath / "cg1",
            cgroupRootPath / "cg2"};

        for (const auto& cg: cgroups) {
            cg.MkDir();
        }

        TVector<TString> args{"cmd", "--pid", "42"};
        args.insert(args.end(), cgroups.begin(), cgroups.end());

        TVector<char*> rawArgs;
        for (auto& arg: args) {
            rawArgs.push_back(&arg[0]);
        }

        TOptions options;
        options.Parse(args.ysize(), &rawArgs[0]);

        UNIT_ASSERT_VALUES_EQUAL(42, options.Pid);
        UNIT_ASSERT_VALUES_EQUAL(2, options.Cgroups.size());
    }

    // ...
}

}   // namespace NCloud::NBlockStore
