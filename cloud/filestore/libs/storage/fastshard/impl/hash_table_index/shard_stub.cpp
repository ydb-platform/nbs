#include "shard.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateHashTableIndexFileSystemShard(
    TString fileSystemId,
    ui32 shardNo,
    ui64 generation,
    NCloud::NFastShard::IStorageGroupPtr storageGroup,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    Y_UNUSED(fileSystemId, shardNo, generation, storageGroup, config);

    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
