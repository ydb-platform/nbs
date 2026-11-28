#pragma once

#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Alters the filesystem, then the shards of the config. Replies
// TEvServicePrivate::TEvMultiShardAlterResponse.
NActors::IActorPtr CreateMultiShardAlterActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TMultiShardFileStoreConfig config);

}   // namespace NCloud::NFileStore::NStorage
