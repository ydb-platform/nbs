#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystemShardFactory
{
    virtual ~IFileSystemShardFactory() = default;

    virtual IFileSystemShardPtr CreateShard(
        const TString& fileSystemId,
        const NProtoPrivate::TFastShardConfig& config,
        ui32 shardNo,
        ui64 generation) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// A mem shard for a mem config, the persistent one for a persistent config,
// or a stub answering E_NOT_IMPLEMENTED if the runtime is not enabled here.
IFileSystemShardFactoryPtr CreateFileSystemShardFactory(bool runtimeEnabled);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
