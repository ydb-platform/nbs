#pragma once

#include <util/system/defaults.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystemShard;
using IFileSystemShardPtr = std::shared_ptr<IFileSystemShard>;

}   // namespace NCloud::NFileStore::NStorage::NFastShard
