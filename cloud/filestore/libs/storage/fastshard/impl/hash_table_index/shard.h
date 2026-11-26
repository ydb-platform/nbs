#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

#include <cloud/fastshard/libs/storage_group/storage_group.h>

namespace NCloud::NFileStore::NProtoPrivate {

////////////////////////////////////////////////////////////////////////////////

class TPersistentFastShardConfig;
class TStorageGroup;

}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateHashTableIndexFileSystemShard(
    TString fileSystemId,
    ui32 shardNo,
    ui64 generation,
    NCloud::NFastShard::IStorageGroupPtr storageGroup,
    const NProtoPrivate::TPersistentFastShardConfig& config);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
