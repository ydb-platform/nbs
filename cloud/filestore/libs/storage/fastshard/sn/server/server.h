#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * TCP server that decodes TDeviceProtocolRequest frames and dispatches
 * them to a single IStorageNode. Extends IStartable — Start() begins
 * accepting; Stop() shuts the accept loop down, half-closes every open
 * connection and waits for every handler fiber before returning.
 */
struct IServer: public IStartable
{
};

using IServerPtr = std::shared_ptr<IServer>;

/**
 * Constructs a silk-based TCP server that reads length-prefixed
 * TDeviceProtocolRequest frames from the wire, dispatches each to
 * `storage`, and writes the matching TDeviceProtocolResponse frame back.
 *
 * @param port - TCP port to listen on (bound at Start()).
 * @param storage - Backend that services the four request cases.
 *
 * @return - Server instance; call Start() to begin accepting.
 */
IServerPtr CreateServer(ui16 port, IStorageNodePtr storage);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
