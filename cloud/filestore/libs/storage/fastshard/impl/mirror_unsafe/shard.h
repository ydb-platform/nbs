#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

namespace NCloud::NFileStore::NProtoPrivate {

////////////////////////////////////////////////////////////////////////////////

class TPersistentFastShardConfig;

}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateMirrorUnsafeFileSystemShard(
    ui32 shardNo,
    const NProtoPrivate::TPersistentFastShardConfig& config);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
