#include "shard.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateNaiveMirroredFileSystemShard(
    TString fileSystemId,
    ui32 shardNo,
    ui64 generation,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    Y_UNUSED(fileSystemId, shardNo, generation, config);

    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
