#pragma once

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <array>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TStartEndpointMode
{
    NProto::EVolumeMountMode MountMode;
    NProto::EVolumeAccessMode AccessMode;
    const char* MountLabel;
    const char* AccessLabel;
};

inline constexpr std::array<TStartEndpointMode, 4> StartEndpointModes = {{
    {NProto::VOLUME_MOUNT_LOCAL,
     NProto::VOLUME_ACCESS_READ_WRITE,
     "local",
     "read_write"},
    {NProto::VOLUME_MOUNT_LOCAL,
     NProto::VOLUME_ACCESS_READ_ONLY,
     "local",
     "read_only"},
    {NProto::VOLUME_MOUNT_REMOTE,
     NProto::VOLUME_ACCESS_READ_WRITE,
     "remote",
     "read_write"},
    {NProto::VOLUME_MOUNT_REMOTE,
     NProto::VOLUME_ACCESS_READ_ONLY,
     "remote",
     "read_only"},
}};

inline constexpr std::array<TStartEndpointMode, 8> AllStartEndpointModes = {{
    StartEndpointModes[0],
    StartEndpointModes[1],
    StartEndpointModes[2],
    StartEndpointModes[3],
    {NProto::VOLUME_MOUNT_LOCAL,
     NProto::VOLUME_ACCESS_USER_READ_ONLY,
     "local",
     "read_only"},
    {NProto::VOLUME_MOUNT_REMOTE,
     NProto::VOLUME_ACCESS_USER_READ_ONLY,
     "remote",
     "read_only"},
    {NProto::VOLUME_MOUNT_LOCAL,
     NProto::VOLUME_ACCESS_REPAIR,
     "local",
     "read_write"},
    {NProto::VOLUME_MOUNT_REMOTE,
     NProto::VOLUME_ACCESS_REPAIR,
     "remote",
     "read_write"},
}};

}   // namespace NCloud::NBlockStore
