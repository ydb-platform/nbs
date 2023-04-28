#pragma once

#include <memory>

namespace NCloud::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

typedef int TMemoryCallback(void* addr, size_t len, void* priv);

struct TVhostCallbacks
{
    TMemoryCallback* MapMemory = nullptr;
    TMemoryCallback* UnmapMemory = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

struct TVhostRequest;
using TVhostRequestPtr = std::shared_ptr<TVhostRequest>;

struct IVhostDevice;
using IVhostDevicePtr = std::shared_ptr<IVhostDevice>;

struct IVhostQueue;
using IVhostQueuePtr = std::shared_ptr<IVhostQueue>;

struct IVhostQueueFactory;
using IVhostQueueFactoryPtr = std::shared_ptr<IVhostQueueFactory>;

struct IServer;
using IServerPtr = std::shared_ptr<IServer>;

}   // namespace NCloud::NBlockStore::NVhost
