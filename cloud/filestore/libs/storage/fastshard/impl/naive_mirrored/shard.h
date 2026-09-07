#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>

namespace NCloud::NFileStore::NProtoPrivate {

////////////////////////////////////////////////////////////////////////////////

class TPersistentFastShardConfig;
class TStorageGroup;

}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IStorageGroupFactory
{
    virtual ~IStorageGroupFactory() = default;
    virtual IStorageGroupPtr MakeStorageGroup(
        const NProtoPrivate::TPersistentFastShardConfig& config) = 0;
};

using IStorageGroupFactoryPtr = std::shared_ptr<IStorageGroupFactory>;

/**
 * An unrecognised type falls back to E_SG_MIRROR.
 */
IStorageGroupFactoryPtr CreateStorageGroupFactory();

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateNaiveMirroredFileSystemShard(
    TString fileSystemId,
    ui32 shardNo,
    IStorageGroupFactoryPtr storageGroupFactory,
    const NProtoPrivate::TPersistentFastShardConfig& config);

IFileSystemShardPtr CreateNaiveMirroredFileSystemShard(
    TString fileSystemId,
    ui32 shardNo,
    const NProtoPrivate::TPersistentFastShardConfig& config);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
