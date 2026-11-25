#pragma once

#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Describes the filesystem and its shards. Replies
// TEvServicePrivate::TEvMultiShardDescribeResponse.
NActors::IActorPtr CreateMultiShardDescribeActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TString fileSystemId);

}   // namespace NCloud::NFileStore::NStorage
