#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <memory>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

enum class EOobMode
{
    Off,
    Auto,
    Required,
};

enum class ESendZeroCopyMode
{
    Off,
    Auto,
    Required,
};

////////////////////////////////////////////////////////////////////////////////

struct TClientConfig;
using TClientConfigPtr = std::shared_ptr<TClientConfig>;

struct TServerConfig;
using TServerConfigPtr = std::shared_ptr<TServerConfig>;

struct TClientRequest;
using TClientRequestPtr = std::unique_ptr<TClientRequest>;

struct IClientRequestHandler;
using IClientRequestHandlerPtr = std::shared_ptr<IClientRequestHandler>;

struct IClientEndpointHandler;
using IClientEndpointHandlerPtr = std::shared_ptr<IClientEndpointHandler>;

struct IClientEndpoint;
using IClientEndpointPtr = std::shared_ptr<IClientEndpoint>;

struct IClient;
using IClientPtr = std::shared_ptr<IClient>;

struct IServerHandler;
using IServerHandlerPtr = std::shared_ptr<IServerHandler>;

struct IServerEndpoint;
using IServerEndpointPtr = std::shared_ptr<IServerEndpoint>;

struct IServer;
using IServerPtr = std::shared_ptr<IServer>;

}   // namespace NCloud::NBlockStore::NUring
