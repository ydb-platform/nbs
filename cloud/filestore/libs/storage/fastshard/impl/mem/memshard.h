#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

namespace NCloud::NFileStore::NProtoPrivate {

////////////////////////////////////////////////////////////////////////////////

class TMemFastShardConfig;

}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateMemFileSystemShard(
    ui32 shardNo,
    const NProtoPrivate::TMemFastShardConfig& config);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
