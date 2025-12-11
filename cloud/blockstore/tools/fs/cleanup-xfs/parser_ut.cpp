#include "parser.h"

#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

TString FilePath(TStringBuf fileName)
{
    return JoinFsPaths(
        ArcadiaSourceRoot(),
        "cloud/blockstore/tools/fs/cleanup-xfs/ut/",
        fileName);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TParserTest)
{
    Y_UNIT_TEST(ShouldParseSuperBlock)
    {
        TFileInput file(FilePath("sb.txt"));

        auto sb = ParseSuperBlock(file);

        UNIT_ASSERT_VALUES_EQUAL(4096, sb.BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(4, sb.GroupCount);
        UNIT_ASSERT_VALUES_EQUAL(6094848, sb.BlocksPerGroup);
        UNIT_ASSERT_VALUES_EQUAL(512, sb.SectorSize);
    }

    Y_UNIT_TEST(ShouldParseFreeSpace)
    {
        TFileInput file(FilePath("freesp.txt"));

        auto freeSpace = ParseFreeSpace(file);

        UNIT_ASSERT_VALUES_EQUAL(21, freeSpace.size());

        ui64 freeSpacePerAg[4] = {};

        for (auto& x: freeSpace) {
            freeSpacePerAg[x.GroupNo] += x.Count;
        }

        UNIT_ASSERT_VALUES_EQUAL(1 + 1 + 1 + 1 + 5 + 42736, freeSpacePerAg[0]);
        UNIT_ASSERT_VALUES_EQUAL(1 + 1 + 1 + 1 + 47894, freeSpacePerAg[1]);
        UNIT_ASSERT_VALUES_EQUAL(1 + 1 + 1 + 1 + 2183494, freeSpacePerAg[2]);
        UNIT_ASSERT_VALUES_EQUAL(1 + 1 + 1 + 1 + 6094838, freeSpacePerAg[3]);
    }
}
