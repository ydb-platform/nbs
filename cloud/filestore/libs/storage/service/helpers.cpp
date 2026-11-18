#include "helpers.h"

#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>


namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& config,
    NProto::TFileStore& fileStore)
{
    fileStore.SetFileSystemId(config.GetFileSystemId());
    fileStore.SetProjectId(config.GetProjectId());
    fileStore.SetFolderId(config.GetFolderId());
    fileStore.SetCloudId(config.GetCloudId());
    fileStore.SetBlockSize(config.GetBlockSize());
    fileStore.SetBlocksCount(config.GetBlocksCount());
    fileStore.SetNodesCount(config.GetNodesCount());
    fileStore.SetStorageMediaKind(config.GetStorageMediaKind());
    fileStore.SetConfigVersion(config.GetVersion());
}

NProtoPrivate::TShardTopology GetShardTopology(
    const NProtoPrivate::TGetFileSystemTopologyResponse& response)
{
    // A tablet of the previous version fills only the list.
    if (!response.HasShardTopology()) {
        return BuildOrdinaryShardTopology(response.GetShardFileSystemIds());
    }

    return response.GetShardTopology();
}

}   // namespace NCloud::NFileStore::NStorage
