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

        {
            (tempDir.Path() / "folder").MkDir();
            (tempDir.Path() / "folder" / "cgroup.procs").Touch();
        }

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            AppMain({100, {tempDir.Path() / "folder"}, "some_other_path"}));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            AppMain({100, {tempDir.Path() / "folder"}, tempDir.Path()}));
    }

    Y_UNIT_TEST(AddToCgroupTest)
    {
        TTempDir tempDir;

        TFsPath firstCgroup = tempDir.Path() / "first_cgroup";
        firstCgroup.MkDir();
        TFsPath secondCgroup = tempDir.Path() / "second_cgroup";
        secondCgroup.MkDir();
        UNIT_ASSERT_EQUAL(
            1,
            AppMain({100, {firstCgroup, secondCgroup}, tempDir.Path()}));
        (firstCgroup / "cgroup.procs").Touch();
        (secondCgroup / "cgroup.procs").Touch();

        UNIT_ASSERT_EQUAL(
            0,
            AppMain({100, {firstCgroup, secondCgroup}, tempDir.Path()}));
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n",
            TFileInput(firstCgroup / "cgroup.procs").ReadAll());
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n",
            TFileInput(secondCgroup / "cgroup.procs").ReadAll());

        UNIT_ASSERT_EQUAL(
            0,
            AppMain({150, {firstCgroup, secondCgroup}, tempDir.Path()}));
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n150\n",
            TFileInput(firstCgroup / "cgroup.procs").ReadAll());
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n150\n",
            TFileInput(secondCgroup / "cgroup.procs").ReadAll());
    }
}

}   // namespace NCloud::NBlockStore
