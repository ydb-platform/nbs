#include "rdma_server.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client_rdma/protocol.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NServer {

using namespace NMonitoring;
using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define Y_ENSURE_RETURN(expr, message)                                         \
    if (Y_UNLIKELY(!(expr))) {                                                 \
        return MakeError(E_ARGUMENT, TStringBuilder() << message);             \
    }                                                                          \
// Y_ENSURE_RETURN

////////////////////////////////////////////////////////////////////////////////

class TRdmaEndpoint final
    : public NRdma::IServerHandler
    , public std::enable_shared_from_this<TRdmaEndpoint>
{
private:
    const IServerStatsPtr ServerStats;
    const ITaskQueuePtr TaskQueue;
    const ISessionPtr Session;
    const size_t BlockSize;

    NRdma::IServerEndpointPtr Endpoint;
    TLog Log;

    NRdma::TProtoMessageSerializer* Serializer = TBlockStoreProtocol::Serializer();

public:
    TRdmaEndpoint(
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            ITaskQueuePtr taskQueue,
            ISessionPtr session,
            size_t blockSize)
        : ServerStats(std::move(serverStats))
        , TaskQueue(std::move(taskQueue))
        , Session(std::move(session))
        , BlockSize(blockSize)
    {
        Log = logging->CreateLog("BLOCKSTORE_RDMA");
    }

    void Init(NRdma::IServerEndpointPtr endpoint)
    {
        Endpoint = std::move(endpoint);
    }

    void HandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) override;

private:
    NProto::TError DoHandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out);

    NProto::TError HandleReadBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TReadBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out);

    NProto::TError HandleWriteBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TWriteBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out);

    NProto::TError HandleZeroBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TZeroBlocksRequest* request,
        TStringBuf requestData,
        TStringBuf out);
};

using TRdmaEndpointPtr = std::shared_ptr<TRdmaEndpoint>;

////////////////////////////////////////////////////////////////////////////////

void TRdmaEndpoint::HandleRequest(
    void* context,
    TCallContextPtr callContext,
    TStringBuf in,
    TStringBuf out)
{
    TaskQueue->ExecuteSimple(
        [self = weak_from_this(),
         context,
         callContext = std::move(callContext),
         in,
         out]() mutable
        {
            auto error = SafeExecute<NProto::TError>(
                [self,
                 context,
                 callContext = std::move(callContext),
                 in,
                 out]() mutable
                {
                    if (auto p = self.lock()) {
                        return p->DoHandleRequest(
                            context,
                            std::move(callContext),
                            in,
                            out);
                    }
                    return MakeError(E_CANCELLED);
                });

            if (HasError(error)) {
                if (auto p = self.lock()) {
                    p->Endpoint->SendError(
                        context,
                        error.GetCode(),
                        error.GetMessage());
                }
            }
        });
}

NProto::TError TRdmaEndpoint::DoHandleRequest(
    void* context,
    TCallContextPtr callContext,
    TStringBuf in,
    TStringBuf out)
{
    auto resultOrError = Serializer->Parse(in);
    if (HasError(resultOrError)) {
        return resultOrError.GetError();
    }

    const auto& request = resultOrError.GetResult();
    switch (request.MsgId) {
        case TBlockStoreProtocol::ReadBlocksRequest:
            return HandleReadBlocksRequest(
                context,
                std::move(callContext),
                static_cast<NProto::TReadBlocksRequest&>(*request.Proto),
                request.Data,
                out);

        case TBlockStoreProtocol::WriteBlocksRequest:
            return HandleWriteBlocksRequest(
                context,
                std::move(callContext),
                static_cast<NProto::TWriteBlocksRequest&>(*request.Proto),
                request.Data,
                out);

        case TBlockStoreProtocol::ZeroBlocksRequest:
            return HandleZeroBlocksRequest(
                context,
                std::move(callContext),
                static_cast<NProto::TZeroBlocksRequest*>(&*request.Proto),
                request.Data,
                out);

        default:
            return MakeError(E_NOT_IMPLEMENTED, TStringBuilder()
                << "request message not supported: " << request.MsgId);
    }
}

NProto::TError TRdmaEndpoint::HandleReadBlocksRequest(
    void* context,
    TCallContextPtr callContext,
    NProto::TReadBlocksRequest& request,
    TStringBuf requestData,
    TStringBuf out)
{
    Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

    TGuardedBuffer buffer(TString::Uninitialized(BlockSize * request.GetBlocksCount()));
    auto guardedSgList = buffer.GetGuardedSgList();

    auto req = std::make_shared<NProto::TReadBlocksLocalRequest>();
    req->CopyFrom(request);

    req->Sglist = guardedSgList;

    auto future = Session->ReadBlocksLocal(
        std::move(callContext),
        std::move(req));

    future.Subscribe(
        [self = weak_from_this(),
         taskQueue = TaskQueue,
         guardedSgList = std::move(guardedSgList),
         buffer = std::move(buffer),
         out,
         context](TFuture<NProto::TReadBlocksLocalResponse> future) mutable
        {
            taskQueue->ExecuteSimple(
                [self = std::move(self),
                 guardedSgList = std::move(guardedSgList),
                 buffer = std::move(buffer),
                 out,
                 response = ExtractResponse(future),
                 context]()
                {
                    if (auto p = self.lock()) {
                        auto guard = guardedSgList.Acquire();
                        Y_ENSURE(guard);

                        const auto& sglist = guard.Get();
                        size_t responseBytes =
                            NRdma::TProtoMessageSerializer::SerializeWithData(
                                out,
                                TBlockStoreProtocol::ReadBlocksResponse,
                                0,   // flags
                                response,
                                sglist);

                        p->Endpoint->SendResponse(context, responseBytes);
                    }
                });
        });

    return {};
}

NProto::TError TRdmaEndpoint::HandleWriteBlocksRequest(
    void* context,
    TCallContextPtr callContext,
    NProto::TWriteBlocksRequest& request,
    TStringBuf requestData,
    TStringBuf out)
{
    Y_ENSURE_RETURN(requestData.length() > 0, "invalid request");

    TGuardedSgList guardedSgList({
        { requestData.data(), requestData.length() }
    });

    auto req = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    req->CopyFrom(request);

    req->Sglist = guardedSgList;

    auto future = Session->WriteBlocksLocal(
        std::move(callContext),
        std::move(req));

    future.Subscribe(
        [self = weak_from_this(),
         taskQueue = TaskQueue,
         guardedSgList = std::move(guardedSgList),
         out,
         context](TFuture<NProto::TWriteBlocksLocalResponse> future) mutable
        {
            taskQueue->ExecuteSimple(
                [self = std::move(self),
                 guardedSgList = std::move(guardedSgList),
                 out,
                 response = ExtractResponse(future),
                 context]
                {
                    // enlarge lifetime of guardedSgList
                    Y_UNUSED(guardedSgList);

                    if (auto p = self.lock()) {
                        size_t responseBytes =
                            NRdma::TProtoMessageSerializer::Serialize(
                                out,
                                TBlockStoreProtocol::WriteBlocksResponse,
                                0,   // flags
                                response);
                        p->Endpoint->SendResponse(context, responseBytes);
                    }
                });
        });

    return {};
}

NProto::TError TRdmaEndpoint::HandleZeroBlocksRequest(
    void* context,
    TCallContextPtr callContext,
    NProto::TZeroBlocksRequest* request,
    TStringBuf requestData,
    TStringBuf out)
{
    Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

    auto req = std::make_shared<NProto::TZeroBlocksRequest>(std::move(*request));

    auto future = Session->ZeroBlocks(
        std::move(callContext),
        std::move(req));

    future.Subscribe(
        [self = weak_from_this(), out, context](
            TFuture<NProto::TZeroBlocksResponse> future)
        {
            if (auto p = self.lock()) {
                auto response = ExtractResponse(future);

                size_t responseBytes =
                    NRdma::TProtoMessageSerializer::Serialize(
                        out,
                        TBlockStoreProtocol::ZeroBlocksResponse,
                        0,   // flags
                        response);
                p->Endpoint->SendResponse(context, responseBytes);
            }
        });

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TRdmaEndpointListener final
    : public IEndpointListener
{
private:
    const NRdma::IServerPtr Server;
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const TExecutorPtr Executor;
    const ITaskQueuePtr TaskQueue;
    const TRdmaEndpointConfig Config;

    THashMap<TString, TRdmaEndpointPtr> Endpoints;

public:
    TRdmaEndpointListener(
            NRdma::IServerPtr server,
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            ITaskQueuePtr taskQueue,
            const TRdmaEndpointConfig& config)
        : Server(std::move(server))
        , Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
        , Executor(std::move(executor))
        , TaskQueue(std::move(taskQueue))
        , Config(config)
    {}

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        ISessionPtr session) override
    {
        return Executor->Execute([this, request, volume, session] {
            return DoStartEndpoint(request, volume, session);
        });
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request, volume, session);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override
    {
        return Executor->Execute([this, socketPath]
                                 { return DoStopEndpoint(socketPath); });
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return {};
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }


private:
    NProto::TError DoStartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        ISessionPtr session);

    NProto::TError DoStopEndpoint(const TString& socketPath);
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError TRdmaEndpointListener::DoStartEndpoint(
    const NProto::TStartEndpointRequest& request,
    const NProto::TVolume& volume,
    ISessionPtr session)
{
    // we use socket path for the endpoint key
    auto socketPath = request.GetUnixSocketPath();

    auto it = Endpoints.find(socketPath);
    if (it != Endpoints.end()) {
        return MakeError(S_ALREADY, TStringBuilder()
            << "endpoint already started " << socketPath.Quote());
    }

    auto endpoint = std::make_shared<TRdmaEndpoint>(
        Logging,
        ServerStats,
        TaskQueue,
        std::move(session),
        volume.GetBlockSize());

    endpoint->Init(Server->StartEndpoint(
        Config.ListenAddress,
        Config.ListenPort,    // TODO
        endpoint));

    Endpoints.emplace(socketPath, std::move(endpoint));
    return {};
}

NProto::TError TRdmaEndpointListener::DoStopEndpoint(const TString& socketPath)
{
    auto it = Endpoints.find(socketPath);
    if (it == Endpoints.end()) {
        return MakeError(S_FALSE, TStringBuilder()
            << "endpoint not started " << socketPath.Quote());
    }

    auto& endpoint = it->second;
    // TODO
    Y_UNUSED(endpoint);

    Endpoints.erase(it);
    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateRdmaEndpointListener(
    NRdma::IServerPtr server,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config)
{
    return std::make_shared<TRdmaEndpointListener>(
        std::move(server),
        std::move(logging),
        std::move(serverStats),
        std::move(executor),
        std::move(taskQueue),
        config);
}

}   // namespace NCloud::NBlockStore::NServer
