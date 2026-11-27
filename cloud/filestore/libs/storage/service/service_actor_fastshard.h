#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/libs/storage/disk_registry_proxy/api/service.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NKikimrFileStore {
    class TConfig;
}   // namespace NKikimrFileStore

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Capacity of one fast shard for file data.
ui64 GetFastShardCapacity(const TStorageConfig& config);

// The number of fast shards that hold the given blocks.
ui64 CalculateFastShardCount(
    const TStorageConfig& config,
    ui64 blocksCount,
    ui32 blockSize);

auto MakeAllocateDevicesRequest(
    const TStorageConfig& config,
    const TString& shardId,
    ui64 tabletId,
    const TString& cloudId,
    const TString& folderId,
    NProto::EFastShardStorageKind storageKind)
    -> std::unique_ptr<TEvDiskRegistryProxy::TEvAllocateDevicesRequest>;

TResultOrError<NProtoPrivate::TFastShardConfig> MakeFastShardConfig(
    const TStorageConfig& config,
    const TString& shardId,
    const TEvDiskRegistryProxy::TDeviceLayout& layout,
    NProto::EFastShardStorageKind storageKind,
    ui32 blockSize);

// Destroys the fast shards and then deallocates their devices. A shard without
// devices is skipped, so a retry completes a failed attempt.
NActors::IActorPtr CreateDeallocateFastShardsActor(
    NActors::TActorId owner,
    TString logTag,
    NProtoPrivate::TShardTopology topology);

}   // namespace NCloud::NFileStore::NStorage
