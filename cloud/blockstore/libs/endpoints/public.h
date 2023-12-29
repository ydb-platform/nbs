#pragma once

#include <util/generic/ptr.h>

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

// the maximal path length allowed for unix-sockets on linux is 107
constexpr size_t UnixSocketPathLengthLimit = 107;

////////////////////////////////////////////////////////////////////////////////

struct ISessionManager;
using ISessionManagerPtr = std::shared_ptr<ISessionManager>;

struct IEndpointManager;
using IEndpointManagerPtr = std::shared_ptr<IEndpointManager>;

struct IEndpointListener;
using IEndpointListenerPtr = std::shared_ptr<IEndpointListener>;

struct IEndpointService;
using IEndpointServicePtr = std::shared_ptr<IEndpointService>;

struct IEndpointEventHandler;
using IEndpointEventHandlerPtr = std::shared_ptr<IEndpointEventHandler>;

struct IEndpointEventProxy;
using IEndpointEventProxyPtr = std::shared_ptr<IEndpointEventProxy>;

}   // namespace NCloud::NBlockStore::NServer
