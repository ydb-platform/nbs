#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TError ValidateFastShardsConfig(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request);

NCloud::NProto::TError ValidateFastShardCapacity(const TStorageConfig& config, ui64 fileSystemSize);

NActors::IActorPtr CreateAllocateFastShardsActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TString logTag,
    TVector<TString> shardIds,
    NProto::EFastShardStorageKind storageKind,
    TString cloudId,
    TString folderId,
    ui64 fileSystemSize);

NActors::IActorPtr CreateDeallocateFastShardsActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TString logTag,
    NProtoPrivate::TFastShardTopology topology);

}   // namespace NCloud::NFileStore::NStorage
