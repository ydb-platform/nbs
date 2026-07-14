#include "shard.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateMirrorUnsafeFileSystemShard(
    ui32 shardNo,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    Y_UNUSED(shardNo, config);

    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
