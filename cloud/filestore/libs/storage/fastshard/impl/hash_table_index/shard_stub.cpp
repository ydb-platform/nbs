#include "shard.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateHashTableIndexFileSystemShard(
    TString fileSystemId,
    ui32 shardNo,
    ui64 generation,
    IStorageGroupFactoryPtr storageGroupFactory,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    Y_UNUSED(fileSystemId, shardNo, generation, storageGroupFactory, config);

    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
