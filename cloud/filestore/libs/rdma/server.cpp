#include "server.h"

#include "protocol.h"
#include "transport.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <google/protobuf/message.h>

#include <util/system/hostname.h>

namespace NCloud::NFileStore::NStorage {

namespace {

using namespace NCloud::NStorage::NRdma;

////////////////////////////////////////////////////////////////////////////////

class TRequestHandler final
    : public IServerHandler
    , public std::enable_shared_from_this<TRequestHandler>
{
private:
    const IRdmaTabletRequestHandlerPtr RequestHandler;
    TLog Log;

    IServerEndpointPtr Endpoint;

public:
    TRequestHandler(IRdmaTabletRequestHandlerPtr requestHandler, TLog log)
        : RequestHandler(std::move(requestHandler))
        , Log(std::move(log))
    {}

    void SetEndpoint(IServerEndpointPtr endpoint)
    {
        Endpoint = std::move(endpoint);
    }

    TCallContextBasePtr CreateCallContext() override
    {
        return MakeIntrusive<TCallContextBase>(ui64{0});
    }

    void HandleRequest(
        void* context,
        TCallContextBasePtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        auto endpoint = Endpoint;
        if (!endpoint) {
            STORAGE_WARN("RDMA request arrived before the endpoint is ready");
            return;
        }

        auto resultOrError = TTabletRdmaProtocol::Serializer()->Parse(in);
        if (HasError(resultOrError)) {
            const auto& error = resultOrError.GetError();
            STORAGE_WARN("failed to parse RDMA request: "
                << FormatError(error));
            endpoint->SendError(context, E_REJECTED, error.GetMessage());
            return;
        }

        auto parsed = resultOrError.ExtractResult();
        if (parsed.MsgId != TTabletRdmaProtocol::TabletRequest) {
            STORAGE_WARN("unexpected RDMA message id: " << parsed.MsgId);
            endpoint->SendError(
                context,
                E_REJECTED,
                "unexpected RDMA message id in request");
            return;
        }

        const auto* envelope =
            static_cast<NProtoPrivate::TTabletRdmaRequest*>(parsed.Proto.get());

        auto self = shared_from_this();
        RequestHandler
            ->HandleRequest(
                envelope->GetTabletId(),
                envelope->GetMethodId(),
                parsed.Data,
                std::move(callContext))
            .Subscribe(
                [self, context, out](
                    const NThreading::TFuture<TTabletTransportResponse>& future)
                {
                    self->CompleteRequest(context, out, future.GetValue());
                });
    }

private:
    void CompleteRequest(
        void* context,
        TStringBuf out,
        const TTabletTransportResponse& response)
    {
        if (HasError(response.Error)) {
            Endpoint->SendError(
                context,
                response.Error.GetCode(),
                response.Error.GetMessage());
            return;
        }

        NProtoPrivate::TTabletRdmaResponse envelope;
        envelope.SetMethodId(response.MethodId);

        const size_t dataLen = response.ResponseProto
            ? response.ResponseProto->ByteSizeLong()
            : response.Response.size();

        const size_t responseBytes =
            TProtoMessageSerializer::MessageByteSize(envelope, dataLen);

        if (responseBytes > out.size()) {
            STORAGE_WARN("RDMA response does not fit the message buffer: "
                << responseBytes << " > " << out.size());
            Endpoint->SendError(
                context,
                E_REJECTED,
                "response does not fit one RDMA message");
            return;
        }

        size_t serialized = 0;
        if (response.ResponseProto) {
            const size_t offset =
                TProtoMessageSerializer::SerializeWithDataLength(
                    out,
                    TTabletRdmaProtocol::TabletResponse,
                    0,   // flags
                    envelope,
                    dataLen);
            const bool ok = response.ResponseProto->SerializeToArray(
                const_cast<char*>(out.data()) + offset,
                dataLen);
            if (!ok) {
                Endpoint->SendError(
                    context,
                    E_REJECTED,
                    "failed to serialize RDMA response body");
                return;
            }
            serialized = offset + dataLen;
        } else {
            TBlockDataRef dataRef;
            TBlockDataRefSpan data;
            if (!response.Response.empty()) {
                dataRef = TBlockDataRef(
                    response.Response.data(),
                    response.Response.size());
                data = TBlockDataRefSpan(&dataRef, 1);
            }

            serialized = TProtoMessageSerializer::SerializeWithData(
                out,
                TTabletRdmaProtocol::TabletResponse,
                0,   // flags
                envelope,
                data);
        }

        Endpoint->SendResponse(context, serialized);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileStoreRdmaServiceTarget final
    : public IStartable
{
private:
    const TFileStoreRdmaServerConfigPtr Config;
    const ILoggingServicePtr Logging;
    const IServerPtr Server;
    const IRdmaTabletRequestHandlerPtr RequestHandler;

    std::shared_ptr<TRequestHandler> Handler;

public:
    TFileStoreRdmaServiceTarget(
            TFileStoreRdmaServerConfigPtr config,
            ILoggingServicePtr logging,
            IServerPtr server,
            IRdmaTabletRequestHandlerPtr requestHandler)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Server(std::move(server))
        , RequestHandler(std::move(requestHandler))
    {}

    void Start() override
    {
        Handler = std::make_shared<TRequestHandler>(
            RequestHandler,
            Logging->CreateLog("NFS_RDMA"));

        const auto host = Config->ListenAddress
            ? Config->ListenAddress
            : FQDNHostName();

        auto endpoint =
            Server->StartEndpoint(host, Config->ListenPort, Handler);

        Handler->SetEndpoint(std::move(endpoint));
    }

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStartablePtr CreateFileStoreRdmaServiceTarget(
    TFileStoreRdmaServerConfigPtr config,
    ILoggingServicePtr logging,
    IServerPtr server,
    IRdmaTabletRequestHandlerPtr requestHandler)
{
    return std::make_shared<TFileStoreRdmaServiceTarget>(
        std::move(config),
        std::move(logging),
        std::move(server),
        std::move(requestHandler));
}

}   // namespace NCloud::NFileStore::NStorage
