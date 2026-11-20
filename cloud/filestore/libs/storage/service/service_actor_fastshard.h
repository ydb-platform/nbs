#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

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

// Allocates devices for the last fastShardCount shards and replies with their
// topology entries.
NActors::IActorPtr CreateAllocateFastShardsActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TString logTag,
    const NKikimrFileStore::TConfig& mainConfig,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs,
    ui32 fastShardCount,
    NProto::EFastShardStorageKind storageKind);

// Destroys the fast shards and then deallocates their devices. A shard without
// devices is skipped, so a retry completes a failed attempt.
NActors::IActorPtr CreateDeallocateFastShardsActor(
    NActors::TActorId owner,
    TString logTag,
    NProtoPrivate::TShardTopology topology);

}   // namespace NCloud::NFileStore::NStorage
