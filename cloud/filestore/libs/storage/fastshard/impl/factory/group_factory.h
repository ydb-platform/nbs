#pragma once

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/fastshard/libs/storage_group/storage_group.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * An unrecognised type falls back to FAST_SHARD_STORAGE_MIRROR.
 */
NCloud::NFastShard::IStorageGroupPtr CreateStorageGroup(
    const TString& fileSystemId,
    const NProtoPrivate::TPersistentFastShardConfig& config);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
