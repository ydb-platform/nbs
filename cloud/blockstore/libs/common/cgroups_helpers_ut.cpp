#include "cgroups_helpers.h"

#include "util/stream/file.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCgroupsHelpersTest)
{
    Y_UNIT_TEST(IsPrefixTest)
    {
        TTempDir tempDir;

        {
            (tempDir.Path() / "folder").MkDir();
            TFile f{
                tempDir.Path() / "folder" / "file",
                EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly};
        }

        UNIT_ASSERT(
            IsPrefix(tempDir.Path() / "folder" / "file", tempDir.Path()));

        UNIT_ASSERT(!IsPrefix("some_other_path", tempDir.Path()));
    }

    Y_UNIT_TEST(AddToCgroupTest)
    {
        TTempDir tempDir;

        TFsPath firstCgroup = tempDir.Path() / "first_cgroup";
        firstCgroup.MkDir();
        TFsPath secondCgroup = tempDir.Path() / "second_cgroup";
        secondCgroup.MkDir();
        UNIT_ASSERT_EXCEPTION(
            AddToCGroups(100, {firstCgroup, secondCgroup}),
            std::exception);
        (firstCgroup / "cgroup.procs").Touch();
        (secondCgroup / "cgroup.procs").Touch();

        AddToCGroups(100, {firstCgroup, secondCgroup});
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n",
            TFileInput(firstCgroup / "cgroup.procs").ReadAll());
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n",
            TFileInput(secondCgroup / "cgroup.procs").ReadAll());

        AddToCGroups(150, {firstCgroup, secondCgroup});
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n150\n",
            TFileInput(firstCgroup / "cgroup.procs").ReadAll());
        UNIT_ASSERT_VALUES_EQUAL(
            "100\n150\n",
            TFileInput(secondCgroup / "cgroup.procs").ReadAll());
    }
}

}   // namespace NCloud::NBlockStore
