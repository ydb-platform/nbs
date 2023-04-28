#pragma once

#include <memory>

#include <util/generic/ptr.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct IServer;
using IServerPtr = std::shared_ptr<IServer>;

struct IServerContext;
using IServerContextPtr = TIntrusivePtr<IServerContext>;

struct IServerHandler;
using IServerHandlerPtr = std::shared_ptr<IServerHandler>;

struct IServerHandlerFactory;
using IServerHandlerFactoryPtr = std::shared_ptr<IServerHandlerFactory>;

struct IClient;
using IClientPtr = std::shared_ptr<IClient>;

struct IClientHandler;
using IClientHandlerPtr = std::shared_ptr<IClientHandler>;

struct IDeviceConnection;
using IDeviceConnectionPtr = std::shared_ptr<IDeviceConnection>;

struct ILimiter;
using ILimiterPtr = std::shared_ptr<ILimiter>;

}   // namespace NCloud::NBlockStore::NBD
