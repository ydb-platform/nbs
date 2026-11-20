#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Resizes a shard that is addressed by its own filesystem id.
NActors::IActorPtr CreateResizeShardActor(
    TStorageConfigPtr storageConfig,
    TRequestInfoPtr requestInfo,
    NProto::TResizeFileStoreRequest request,
    NKikimrFileStore::TConfig config,
    const NProtoPrivate::TGetFileSystemTopologyResponse& topology);

}   // namespace NCloud::NFileStore::NStorage
