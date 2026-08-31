#include "shard.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateNaiveMirroredFileSystemShard(
    TString fileSystemId,
    ui32 shardNo,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    Y_UNUSED(fileSystemId, shardNo, config);

    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
