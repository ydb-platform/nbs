#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateMemFileSystemShard();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
