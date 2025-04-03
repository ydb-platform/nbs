#include "follower_disk.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString TFollowerDiskInfo::GetDiskIdForPrint() const
{
    return ScaleUnitId ? (ScaleUnitId + "/" + FollowerDiskId) : FollowerDiskId;
}

bool TFollowerDiskInfo::operator==(
    const TFollowerDiskInfo& rhs) const = default;

}   // namespace NCloud::NBlockStore::NStorage
