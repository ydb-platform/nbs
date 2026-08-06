#include "channel_permissions.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

EChannelPermissions StorageStatusFlags2ChannelPermissions(
    TStorageStatusFlags ssf)
{
    /*
    YellowStop: Tablets switch to read-only mode. Only system writes are
    allowed.

    LightOrange: Alert: "Tablets have not stopped". Compaction writes are not
    allowed if this flag is received.

    PreOrange: VDisk switches to read-only mode.

    Orange: All VDisks in the group switch to read-only mode.

    Red: PDisk stops issuing chunks.

    Black: Reserved for recovery.
    */

    const auto outOfSpaceMask = static_cast<NKikimrBlobStorage::EStatusFlags>(
        NKikimrBlobStorage::StatusDiskSpaceBlack |
        NKikimrBlobStorage::StatusDiskSpaceRed |
        NKikimrBlobStorage::StatusDiskSpaceOrange |
        NKikimrBlobStorage::StatusDiskSpacePreOrange |
        NKikimrBlobStorage::StatusDiskSpaceLightOrange);
    if (ssf.Check(outOfSpaceMask)) {
        return {};
    }

    if (ssf.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
        return EChannelPermission::SystemWritesAllowed;
    }

    return EChannelPermission::SystemWritesAllowed |
           EChannelPermission::UserWritesAllowed;
}

bool IsValid(TStorageStatusFlags ssf)
{
    return ssf.Check(NKikimrBlobStorage::StatusIsValid);
}

bool HasYellowStop(TStorageStatusFlags ssf)
{
    return ssf.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop);
}

bool HasYellowMove(TStorageStatusFlags ssf)
{
    return ssf.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);
}

}   // namespace NCloud::NBlockStore::NStorage
