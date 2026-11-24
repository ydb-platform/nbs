#include "endpoint.h"

#include "protocol.h"
#include "transport.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <library/cpp/threading/future/future.h>

#include <google/protobuf/message.h>

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

    TResultOrError<IServerHandlerPtr> HandleConnect(
        TExternalConnectionId externalConnectionId) override
    {
        Y_UNUSED(externalConnectionId);
        return IServerHandlerPtr();
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
            STORAGE_WARN(
                "failed to parse RDMA request: " << FormatError(error));
            SendError(context, E_ARGUMENT, error.GetMessage());
            return;
        }

        auto parsed = resultOrError.ExtractResult();
        if (parsed.MsgId != TTabletRdmaProtocol::TabletRequest) {
            STORAGE_WARN("unexpected RDMA message id: " << parsed.MsgId);
            SendError(
                context,
                E_NOT_IMPLEMENTED,
                "unexpected RDMA message id in request");
            return;
        }

        const auto* envelope =
            static_cast<NProtoPrivate::TTabletRdmaRequest*>(parsed.Proto.get());

        auto error = SafeExecute<NProto::TError>(
            [&]
            {
                RequestHandler
                    ->HandleRequest(
                        *envelope,
                        parsed.Data,
                        std::move(callContext))
                    .Subscribe(
                        [self = shared_from_this(), context, out](
                            const NThreading::TFuture<TTabletTransportResponse>&
                                future)
                        { self->CompleteRequest(context, out, future); });
                return NProto::TError();
            });

        if (HasError(error)) {
            STORAGE_ERROR(
                "failed to dispatch RDMA request: " << FormatError(error));
            SendError(context, error.GetCode(), error.GetMessage());
        }
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
        const NThreading::TFuture<TTabletTransportResponse>& future)
    {
        auto result = SafeExecute<TResultOrError<size_t>>(
            [&] { return SerializeResponse(out, future.GetValue()); });

        if (HasError(result)) {
            const auto& error = result.GetError();
            SendError(context, error.GetCode(), error.GetMessage());
            return;
        }

        SendResponse(context, result.GetResult());
    }

    TResultOrError<size_t> SerializeResponse(
        TStringBuf out,
        const TTabletTransportResponse& response) const
    {
        if (HasError(response.Error)) {
            return response.Error;
        }

        NProtoPrivate::TTabletRdmaResponse envelope;
        envelope.SetMethodId(response.MethodId);

        const size_t dataLen =
            response.Response ? response.Response->ByteSizeLong() : 0;

        if (dataLen > out.size() ||
            TProtoMessageSerializer::MessageByteSize(envelope, dataLen) >
                out.size())
        {
            STORAGE_WARN(
                "RDMA response does not fit the message buffer: "
                << dataLen << " body bytes, " << out.size() << " buffer bytes");
            return MakeError(
                E_ARGUMENT,
                "response does not fit one RDMA message");
        }

        const size_t offset = TProtoMessageSerializer::SerializeWithDataLength(
            out,
            TTabletRdmaProtocol::TabletResponse,
            0,   // flags
            envelope,
            dataLen);

        if (response.Response) {
            auto* body =
                reinterpret_cast<ui8*>(const_cast<char*>(out.data())) + offset;
            auto* end =
                response.Response->SerializeWithCachedSizesToArray(body);
            Y_DEBUG_ABORT_UNLESS(end == body + dataLen);
        }

        return offset + dataLen;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void StartRdmaTabletEndpoint(
    IServer& server,
    const TString& host,
    ui32 port,
    TLog log,
    IRdmaTabletRequestHandlerPtr requestHandler)
{
    auto handler = std::make_shared<TRequestHandler>(
        std::move(requestHandler),
        std::move(log));

    IServerEndpointPtr endpoint;
    try {
        endpoint = server.StartEndpoint(host, port, handler);
    } catch (...) {
        handler->SetEndpoint({});
        throw;
    }

    if (!endpoint) {
        handler->SetEndpoint({});
        STORAGE_THROW_SERVICE_ERROR(E_FAIL)
            << "unable to start RDMA endpoint " << host << ":" << port;
    }

    handler->SetEndpoint(std::move(endpoint));
}

}   // namespace NCloud::NFileStore::NStorage
