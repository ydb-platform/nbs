#include "follower_disk.h"

#include <cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString TFollowerDiskInfo::GetDiskIdForPrint() const
{
    return ScaleUnitId ? (ScaleUnitId + "/" + FollowerDiskId) : FollowerDiskId;
}

TString TFollowerDiskInfo::Describe() const
{
    auto builder = TStringBuilder();
    builder << "{ State:" << ToString(State) << ", MigratedBytes:";

    if (MigratedBytes) {
        builder << FormatByteSize(*MigratedBytes);
    } else {
        builder << "-";
    }
    builder << "}";
    return builder;
}

bool TFollowerDiskInfo::operator==(
    const TFollowerDiskInfo& rhs) const = default;

}   // namespace NCloud::NBlockStore::NStorage
