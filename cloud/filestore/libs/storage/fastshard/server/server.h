#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IServer: public IStartable
{
    // Register a shard under the given filesystem id. Requests with
    // matching FileSystemId will be routed to this shard.
    // Thread-safe; may be called while the server is running.
    virtual void RegisterShard(
        const TString& fileSystemId,
        IFileSystemShardPtr shard) = 0;

    // Unregister a previously registered shard. Subsequent requests
    // for this filesystem id will fail.
    virtual void UnregisterShard(
        const TString& fileSystemId) = 0;
};

using IServerPtr = std::shared_ptr<IServer>;

IServerPtr CreateServer(ui16 port);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
