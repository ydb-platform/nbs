#pragma once

#include <util/system/defaults.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystemShardFactory;
using IFileSystemShardFactoryPtr = std::shared_ptr<IFileSystemShardFactory>;

struct IStorageGroupFactory;
using IStorageGroupFactoryPtr = std::shared_ptr<IStorageGroupFactory>;

}   // namespace NCloud::NFileStore::NStorage::NFastShard
