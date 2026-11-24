#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <util/system/types.h>

#include <memory>

namespace google::protobuf {
    class Message;
}

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

enum class EWaitMode
{
    Poll,
    BusyWait,
    AdaptiveWait,
};

////////////////////////////////////////////////////////////////////////////////

// Application's id of a connection. The client passes it upon connect, the
// server hands it to IServerHandler::HandleConnect.
struct TExternalConnectionId
{
    ui64 High = 0;
    ui64 Low = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IClient;
using IClientPtr = std::shared_ptr<IClient>;

class TNullContext;

struct TClientRequest;
using TClientRequestPtr = std::unique_ptr<TClientRequest>;

struct IClientEndpoint;
using IClientEndpointPtr = std::shared_ptr<IClientEndpoint>;

struct IClientRequestHandler;
using IClientRequestHandlerPtr = std::shared_ptr<IClientRequestHandler>;

struct TClientConfig;
using TClientConfigPtr = std::shared_ptr<TClientConfig>;

struct IServer;
using IServerPtr = std::shared_ptr<IServer>;

struct IServerEndpoint;
using IServerEndpointPtr = std::shared_ptr<IServerEndpoint>;

struct IServerHandler;
using IServerHandlerPtr = std::shared_ptr<IServerHandler>;

struct TServerConfig;
using TServerConfigPtr = std::shared_ptr<TServerConfig>;

using TProtoMessage = google::protobuf::Message;
using TProtoMessagePtr = std::unique_ptr<TProtoMessage>;

class TProtoMessageSerializer;

}   // namespace NCloud::NStorage::NRdma
