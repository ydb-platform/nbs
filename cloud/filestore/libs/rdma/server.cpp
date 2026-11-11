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

#include <library/cpp/threading/future/future.h>

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

    NThreading::TPromise<std::weak_ptr<IServerEndpoint>> Endpoint =
        NThreading::NewPromise<std::weak_ptr<IServerEndpoint>>();

public:
    TRequestHandler(IRdmaTabletRequestHandlerPtr requestHandler, TLog log)
        : RequestHandler(std::move(requestHandler))
        , Log(std::move(log))
    {}

    void SetEndpoint(std::weak_ptr<IServerEndpoint> endpoint)
    {
        Endpoint.SetValue(std::move(endpoint));
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
        auto resultOrError = TTabletRdmaProtocol::Serializer()->Parse(in);
        if (HasError(resultOrError)) {
            const auto& error = resultOrError.GetError();
            STORAGE_WARN("failed to parse RDMA request: "
                << FormatError(error));
            SendError(context, E_REJECTED, error.GetMessage());
            return;
        }

        auto parsed = resultOrError.ExtractResult();
        if (parsed.MsgId != TTabletRdmaProtocol::TabletRequest) {
            STORAGE_WARN("unexpected RDMA message id: " << parsed.MsgId);
            SendError(
                context,
                E_REJECTED,
                "unexpected RDMA message id in request");
            return;
        }

        const auto* envelope =
            static_cast<NProtoPrivate::TTabletRdmaRequest*>(parsed.Proto.get());

        auto self = shared_from_this();
        RequestHandler
            ->HandleRequest(*envelope, parsed.Data, std::move(callContext))
            .Subscribe(
                [self, context, out](
                    const NThreading::TFuture<TTabletTransportResponse>& future)
                {
                    self->CompleteRequest(context, out, future.GetValue());
                });
    }

private:
    IServerEndpointPtr GetEndpoint() const
    {
        return Endpoint.GetFuture().GetValueSync().lock();
    }

    void SendResponse(void* context, size_t responseBytes) const
    {
        if (auto endpoint = GetEndpoint()) {
            endpoint->SendResponse(context, responseBytes);
        }
    }

    void SendError(void* context, ui32 error, TStringBuf message) const
    {
        if (auto endpoint = GetEndpoint()) {
            endpoint->SendError(context, error, message);
        }
    }

    void CompleteRequest(
        void* context,
        TStringBuf out,
        const TTabletTransportResponse& response)
    {
        if (HasError(response.Error)) {
            SendError(
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
            SendError(
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
            response.ResponseProto->SerializeWithCachedSizesToArray(
                reinterpret_cast<ui8*>(const_cast<char*>(out.data())) + offset);
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

        SendResponse(context, serialized);
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

    TLog Log;
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
        Log = Logging->CreateLog("NFS_RDMA");
        Handler = std::make_shared<TRequestHandler>(RequestHandler, Log);

        auto endpoint = Server->StartEndpoint(
            Config->ListenAddress,
            Config->ListenPort,
            Handler);
        if (!endpoint) {
            STORAGE_ERROR("unable to start RDMA endpoint "
                << Config->ListenAddress << ":" << Config->ListenPort);
        }

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
