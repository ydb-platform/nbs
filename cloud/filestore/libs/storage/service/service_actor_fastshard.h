#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrFileStore {
    class TConfig;
}   // namespace NKikimrFileStore

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateFastShardCapacity(
    const TStorageConfig& config,
    ui64 fileSystemSize);

// Allocates devices for the fast shards, they are the last ones of the shards.
NActors::IActorPtr CreateAllocateFastShardsActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TString logTag,
    const NKikimrFileStore::TConfig& mainConfig,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs,
    ui32 fastShardCount,
    NProto::EFastShardStorageKind storageKind);

NActors::IActorPtr CreateDeallocateFastShardsActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TString logTag,
    NProtoPrivate::TShardTopology topology);

}   // namespace NCloud::NFileStore::NStorage
