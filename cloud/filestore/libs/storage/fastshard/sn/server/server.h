#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IServer: public IStartable
{
};

using IServerPtr = std::shared_ptr<IServer>;

// Creates a silk-based TCP server that reads TDeviceProtocolRequest
// frames from the wire, dispatches them to `storage`, and writes back
// TDeviceProtocolResponse frames.
IServerPtr CreateServer(ui16 port, IStorageNodePtr storage);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
