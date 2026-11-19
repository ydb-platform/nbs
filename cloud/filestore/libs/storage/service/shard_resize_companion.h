#pragma once

#include "shard_request_companion.h"

#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Resizes existing shards: describes them, as a new config is made of the
// current one, and alters them. The owner reports the responses.
class TShardResizeCompanion
{
private:
    const TStorageConfigPtr StorageConfig;
    const NProto::TFileStorePerformanceProfile PerformanceProfile;

    TVector<NKikimrFileStore::TConfig> ShardConfigs;
    TShardRequestCompanion ShardDescription;
    TShardRequestCompanion ShardResize;

public:
    TShardResizeCompanion(
        TStorageConfigPtr storageConfig,
        NProto::TFileStorePerformanceProfile performanceProfile);

    // The shards to resize, a config has the id of a shard and its new size.
    void InitializeShardResize(TVector<NKikimrFileStore::TConfig> shardConfigs);

    void StartShardDescription(const NActors::TActorContext& ctx);
    NProto::TError OnShardDescribed(
        const NActors::TActorContext& ctx,
        ui32 shardIndex,
        const NKikimrFileStore::TConfig& config);
    bool HasShardsToDescribe() const;

    void StartShardResize(const NActors::TActorContext& ctx);
    NProto::TError OnShardResized(
        const NActors::TActorContext& ctx,
        ui32 shardIndex);
    bool HasShardsToResize() const;

private:
    void DescribeShard(const NActors::TActorContext& ctx, ui32 shardIndex);
    void ResizeShard(const NActors::TActorContext& ctx, ui32 shardIndex);
};

}   // namespace NCloud::NFileStore::NStorage
