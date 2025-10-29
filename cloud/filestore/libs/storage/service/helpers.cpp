#include "helpers.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <ydb/core/protos/filestore_config.pb.h>


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

}   // namespace NCloud::NFileStore::NStorage
