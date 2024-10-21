#include "utils.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUtilsTest)
{
    Y_UNIT_TEST(ShouldFindFreeNbdDevice)
    {
        TTempDir sysBlockDir;
        TFsPath path = sysBlockDir.Path();

        auto createPidFile = [](auto path) {
            path.Parent().MkDir();
            TOFStream(path.GetPath()).Write("42");
        };

        (path / "sda0").MkDir();
        createPidFile(path / "sda1" / "pid");
        createPidFile(path / "nbd0" / "pid");
        createPidFile(path / "nbd1" / "pid");

        UNIT_ASSERT(FindFreeNbdDevice(path.GetPath()).Empty());

        (path / "nbd2").MkDir();

        UNIT_ASSERT_VALUES_EQUAL(
            FindFreeNbdDevice(path.GetPath()),
            TString("/dev/nbd2"));
    }
}

}   // namespace NCloud::NBlockStore::NBD
