#include "follower_disk.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

bool TFollowerDiskInfo::operator==(const TFollowerDiskInfo& rh) const
{
    auto doTie = [](const TFollowerDiskInfo& x)
    {
        return std::tie(
            x.Id,
            x.FollowerDiskId,
            x.ScaleUnitId,
            x.State,
            x.MigrationBlockIndex);
    };
    return doTie(*this) == doTie(rh);
}

}   // namespace NCloud::NBlockStore::NStorage
