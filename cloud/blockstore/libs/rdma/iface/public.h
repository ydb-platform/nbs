#pragma once

#include <memory>

namespace google::protobuf {
    class Message;
}

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct IClient;
using IClientPtr = std::shared_ptr<IClient>;

struct TClientRequest;
using TClientRequestPtr = std::unique_ptr<TClientRequest>;

struct IClientEndpoint;
using IClientEndpointPtr = std::shared_ptr<IClientEndpoint>;

struct IClientHandler;
using IClientHandlerPtr = std::shared_ptr<IClientHandler>;

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

}   // namespace NCloud::NBlockStore::NRdma
