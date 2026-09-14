#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>
#include <cloud/filestore/libs/storage/fastshard/impl/factory/public.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>

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
    IStorageGroupFactoryPtr storageGroupFactory,
    const NProtoPrivate::TPersistentFastShardConfig& config);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
