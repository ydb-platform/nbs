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

struct IDevice;
using IDevicePtr = std::shared_ptr<IDevice>;

struct IDeviceFactory;
using IDeviceFactoryPtr = std::shared_ptr<IDeviceFactory>;

struct ILimiter;
using ILimiterPtr = std::shared_ptr<ILimiter>;

struct IErrorHandler;
using IErrorHandlerPtr = std::shared_ptr<IErrorHandler>;

struct IErrorHandlerMap;
using IErrorHandlerMapPtr = std::shared_ptr<IErrorHandlerMap>;

}   // namespace NCloud::NBlockStore::NBD
