#include "follower_disk.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLeaderFollowerLinkTest)
{
    const TLeaderFollowerLink Legacy{
        .LinkUUID = "",
        .LeaderDiskId = "vol-1",
        .LeaderCellId = "",
        .FollowerDiskId = "vol-2",
        .FollowerCellId = ""};
    const TLeaderFollowerLink Local{
        .LinkUUID = "",
        .LeaderDiskId = "vol-1",
        .LeaderCellId = "cell-a",
        .FollowerDiskId = "vol-2",
        .FollowerCellId = "cell-a"};
    const TLeaderFollowerLink Remote{
        .LinkUUID = "",
        .LeaderDiskId = "vol-1",
        .LeaderCellId = "cell-a",
        .FollowerDiskId = "vol-2",
        .FollowerCellId = "cell-b"};

    Y_UNIT_TEST(ShouldMatchByUUIDFirst)
    {
        auto withUUID = Remote;
        withUUID.LinkUUID = "uuid-1";

        auto onlyUUID = TLeaderFollowerLink{.LinkUUID = "uuid-1"};
        UNIT_ASSERT(withUUID.Match(onlyUUID));
        UNIT_ASSERT(onlyUUID.Match(withUUID));

        auto otherUUID = withUUID;
        otherUUID.LinkUUID = "uuid-2";
        UNIT_ASSERT(!withUUID.Match(otherUUID));
    }

    Y_UNIT_TEST(ShouldMatchCellsStrictlyWhenFilled)
    {
        UNIT_ASSERT(Local.Match(Local));
        UNIT_ASSERT(Remote.Match(Remote));
        UNIT_ASSERT(!Local.Match(Remote));
        UNIT_ASSERT(!Remote.Match(Local));

        auto otherFollower = Local;
        otherFollower.FollowerDiskId = "vol-3";
        UNIT_ASSERT(!Local.Match(otherFollower));
    }

    Y_UNIT_TEST(ShouldMatchLegacyLinkOnlyWithinOneCell)
    {
        UNIT_ASSERT(Legacy.Match(Legacy));
        UNIT_ASSERT(Legacy.Match(Local));
        UNIT_ASSERT(Local.Match(Legacy));
        UNIT_ASSERT(!Legacy.Match(Remote));
        UNIT_ASSERT(!Remote.Match(Legacy));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
