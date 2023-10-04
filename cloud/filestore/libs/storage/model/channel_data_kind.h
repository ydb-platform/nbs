#pragma once

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EChannelDataKind
{
    System = 1,
    Index = 2,
    Fresh = 3,

    // Legacy data kind! Do not use it! Kept for backward compatibilty.
    // Must be removed once there are no old filesystems with this data kind.
    Mixed0 = 4,

    Mixed = 5,

    Max
};

}   // namespace NCloud::NFileStore::NStorage
