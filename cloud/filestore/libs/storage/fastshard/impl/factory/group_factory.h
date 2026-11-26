#pragma once

#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group_factory.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * An unrecognised type falls back to FAST_SHARD_STORAGE_MIRROR.
 */
IStorageGroupFactoryPtr CreateStorageGroupFactory();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
