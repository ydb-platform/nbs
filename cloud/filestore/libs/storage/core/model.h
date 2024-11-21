#include "config.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TMultiShardFileStoreConfig
{
    NKikimrFileStore::TConfig MainFileSystemConfig;
    TVector<NKikimrFileStore::TConfig> ShardConfigs;
};

TMultiShardFileStoreConfig SetupMultiShardFileStorePerformanceAndChannels(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore,
    const NProto::TFileStorePerformanceProfile& clientProfile);

void SetupFileStorePerformanceAndChannels(
    bool allocateMixed0Channel,
    const TStorageConfig& config,
    NKikimrFileStore::TConfig& fileStore,
    const NProto::TFileStorePerformanceProfile& clientProfile);

}   // namespace NCloud::NFileStore::NStorage
