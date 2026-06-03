#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IServer
{
    virtual ~IServer() = default;

    // Start the server. Blocks until Stop() is called from another
    // thread/fiber. Must be called from a silk fiber context.
    virtual void Start() = 0;

    // Signal the server to stop accepting new connections and return
    // from Start(). Safe to call from any thread.
    virtual void Stop() = 0;

    // Register a shard under the given filesystem id. Requests with
    // matching FileSystemId will be routed to this shard.
    // Thread-safe; may be called while the server is running.
    virtual void RegisterShard(
        const TString& fileSystemId,
        IFileSystemShardPtr shard) = 0;

    // Unregister a previously registered shard. Subsequent requests
    // for this filesystem id will fail.
    virtual void UnregisterShard(const TString& fileSystemId) = 0;
};

using IServerPtr = std::unique_ptr<IServer>;

IServerPtr CreateServer(ui16 port);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
