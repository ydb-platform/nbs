#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

// A mem shard for a mem config, the persistent one for a persistent config,
// or a stub answering E_NOT_IMPLEMENTED if the runtime is not enabled here.
IFileSystemShardFactoryPtr CreateFileSystemShardFactory(bool runtimeEnabled);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
