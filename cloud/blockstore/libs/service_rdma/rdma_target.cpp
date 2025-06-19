#include "rdma_target.h"

#include "rdma_protocol.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define Y_ENSURE_RETURN(expr, message)                                         \
    if (Y_UNLIKELY(!(expr))) {                                                 \
        return MakeError(E_ARGUMENT, TStringBuilder() << message);             \
    }                                                                          \
// Y_ENSURE_RETURN

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                           \
    struct T##name##Method                                             \
    {                                                                  \
        using TRequest = NProto::T##name##Request;                     \
        using TResponse = NProto::T##name##Response;                   \
                                                                       \
        template <typename T, typename... TArgs>                       \
        static TFuture<TResponse> Execute(T& service, TArgs&&... args) \
        {                                                              \
            return service.name(std::forward<TArgs>(args)...);         \
        }                                                              \
    };                                                                 \
    // BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_RETURN_TRUE_CASE(name, ...)         \
    case TBlockStoreServerProtocol::Ev##name##Request: \
        return true;                                   \
                                                       \
        // BLOCKSTORE_RETURN_TRUE_CASE

#undef BLOCKSTORE_RETURN_TRUE_CASE

////////////////////////////////////////////////////////////////////////////////

struct TRequestDetails
{
    void* Context = nullptr;
    TStringBuf Out;
    TStringBuf DataBuffer;
};

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. After Init() public method HandleRequest() can be called
// from any thread.
class TRequestHandler final
    : public NRdma::IServerHandler
    , public std::enable_shared_from_this<TRequestHandler>
{
    IBlockStorePtr Service;
    ITaskQueuePtr TaskQueue;

    TLog Log;
    std::weak_ptr<NRdma::IServerEndpoint> Endpoint;

    const NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreServerProtocol::Serializer();

public:
    TRequestHandler(IBlockStorePtr service, ITaskQueuePtr taskQueue)
        : Service(std::move(service))
        , TaskQueue(std::move(taskQueue))
    {}

    void Init(const NRdma::IServerEndpointPtr& endpoint, TLog log)
    {
        Endpoint = endpoint;
        Log = std::move(log);
    }

private:
#define BLOCKSTORE_HANDLE_REQUEST(name, ...)                                   \
    case TBlockStoreServerProtocol::Ev##name##Request:                         \
        return Handle##name##Request(                                          \
            context,                                                           \
            std::move(callContext),                                            \
            static_cast<NProto::T##name##Request&>(*parseResult.Proto),        \
            parseResult.Data,                                                  \
            out);                                                              \
        // BLOCKSTORE_HANDLE_REQUEST

    NProto::TError DoHandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) const
    {
        auto [parseResult, error] = Serializer->Parse(in);

        if (HasError(error)) {
            STORAGE_ERROR("Can't parse input: %s", FormatError(error).c_str())
            return error;
        }

        STORAGE_TRACE("Processing req with msgId %u", parseResult.MsgId);

        switch (parseResult.MsgId) {
            BLOCKSTORE_HANDLE_REQUEST(ReadBlocks)
            BLOCKSTORE_HANDLE_REQUEST(WriteBlocks)
            BLOCKSTORE_HANDLE_REQUEST(ZeroBlocks)

            default:
                return MakeError(
                    E_NOT_IMPLEMENTED,
                    TStringBuilder()
                        << "Request with msg id "
                        << parseResult.MsgId
                        << " is not supported by blockstore server RDMA target");
        }
    }
#undef BLOCKSTORE_HANDLE_REQUEST

    void HandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        TaskQueue->ExecuteSimple([=, endpoint = Endpoint] {
            auto error = SafeExecute<NProto::TError>([=] {
                return DoHandleRequest(context, callContext, in, out);
            });

            if (HasError(error)) {
                if (auto ep = endpoint.lock()) {
                    ep->SendError(
                        context,
                        error.GetCode(),
                        error.GetMessage());
                }
            }
        });
    }

    NProto::TError HandleReadBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TReadBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");
        Y_ENSURE_RETURN(request.GetBlockSize() != 0, "empty BlockSize");

        TGuardedBuffer buffer(TString::Uninitialized(
            static_cast<size_t>(request.GetBlockSize()) * request.GetBlocksCount()));

        auto [sglist, error] = SgListNormalize(
            TBlockDataRef{buffer.Get().data(), buffer.Get().length()},
            request.GetBlockSize());
        Y_ENSURE_RETURN(error.GetCode() == 0, "cannot create sgList");

        TGuardedSgList guardedSgList(sglist);

        auto req = std::make_shared<NProto::TReadBlocksLocalRequest>();
        req->CopyFrom(request);
        req->BlockSize = request.GetBlockSize();

        req->Sglist = guardedSgList;

        auto future = Service->ReadBlocksLocal(
            std::move(callContext),
            std::move(req));

        future.Subscribe(
            [=,
             buffer = std::move(buffer),
             guardedSgList = std::move(guardedSgList),
             blockSize = request.GetBlockSize(),
             taskQueue = TaskQueue,
             endpoint = Endpoint](auto future)
            {
                auto response = ExtractResponse(future);

                taskQueue->ExecuteSimple(
                    [=,
                     buffer = std::move(buffer),
                     guardedSgList = std::move(guardedSgList)]() mutable
                    {
                        if (SUCCEEDED(response.GetError().GetCode())) {
                            auto guard = guardedSgList.Acquire();
                            Y_ENSURE(guard);

                            ui32 flags = 0;
                            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);

                            size_t responseBytes = NRdma::
                                TProtoMessageSerializer::SerializeWithData(
                                    out,
                                    TBlockStoreServerProtocol::
                                        EvReadBlocksResponse,
                                    flags,   // flags
                                    response,
                                    guard.Get());
                            if (auto ep = endpoint.lock()) {
                                ep->SendResponse(context, responseBytes);
                            }
                        }
                    });
            });

        return {};
    }

    NProto::TError HandleWriteBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TWriteBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        Y_ENSURE_RETURN(requestData.length() > 0, "invalid request");
        auto [sglist, error] = SgListNormalize({ requestData.data(), requestData.length() }, request.GetBlockSize());
        Y_ENSURE_RETURN(error.GetCode() == 0, "cannot create sgList");

        TGuardedSgList guardedSgList(sglist);

        auto req = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        req->CopyFrom(request);

        req->Sglist = guardedSgList;
        req->BlockSize = request.GetBlockSize();
        req->BlocksCount = requestData.length() / req->BlockSize;

        auto future = Service->WriteBlocksLocal(
            std::move(callContext),
            std::move(req));

        future.Subscribe([=, taskQueue = TaskQueue, endpoint = Endpoint] (auto future) {
            auto response = ExtractResponse(future);

            taskQueue->ExecuteSimple([= , response = std::move(response)] {

                ui32 flags = 0;
                SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
                size_t responseBytes = NRdma::TProtoMessageSerializer::Serialize(
                    out,
                    TBlockStoreServerProtocol::EvWriteBlocksResponse,
                    flags,   // flags
                    response);
                if (auto ep = endpoint.lock()) {
                    ep->SendResponse(context, responseBytes);
                }
            });
        });

        return {};
    }

    NProto::TError HandleZeroBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TZeroBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        auto req = std::make_shared<NProto::TZeroBlocksRequest>(std::move(request));

        auto future = Service->ZeroBlocks(
            std::move(callContext),
            std::move(req));

        future.Subscribe([out = out, context = std::move(context), endpoint = Endpoint] (auto future) {
            auto response = ExtractResponse(future);
            ui32 flags = 0;
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
            size_t responseBytes = NRdma::TProtoMessageSerializer::Serialize(
                out,
                TBlockStoreServerProtocol::EvZeroBlocksResponse,
                flags,   // flags
                response);

            if (auto ep = endpoint.lock()) {
                ep->SendResponse(context, responseBytes);
            }
        });

        return {};
    }
};

///////////////////////////////////////////////////////////////////////////////

class TRdmaTarget final: public IStartable
{
    const TBlockstoreServerRdmaTargetConfigPtr Config;

    ILoggingServicePtr Logging;
    NRdma::IServerPtr Server;
    ITaskQueuePtr TaskQueue;

    std::shared_ptr<TRequestHandler> Handler;

    TLog Log;

public:
    TRdmaTarget(
            TBlockstoreServerRdmaTargetConfigPtr rdmaTargetConfig,
            ILoggingServicePtr logging,
            NRdma::IServerPtr server,
            ITaskQueuePtr taskQueue,
            IBlockStorePtr service)
        : Config(std::move(rdmaTargetConfig))
        , Logging(std::move(logging))
        , Server(std::move(server))
        , TaskQueue(std::move(taskQueue))
    {
        Handler =
            std::make_shared<TRequestHandler>(std::move(service), TaskQueue);
    }

    void Start() override
    {
        auto endpoint =
            Server->StartEndpoint(Config->Host, Config->Port, Handler);

        Log = Logging->CreateLog("BLOCKSTORE_SERVER");
        if (endpoint == nullptr) {
            STORAGE_ERROR("unable to set up RDMA endpoint");
            return;
        }

        Handler->Init(endpoint, std::move(Log));
    }

    void Stop() override
    {
        Server->Stop();
        TaskQueue->Stop();
    }
};


}   // namespace

IStartablePtr CreateBlockstoreServerRdmaTarget(
    TBlockstoreServerRdmaTargetConfigPtr rdmaTargetConfig,
    ILoggingServicePtr logging,
    NRdma::IServerPtr server,
    IBlockStorePtr service)
{
    auto threadPool = CreateThreadPool("RDMA", rdmaTargetConfig->WorkerThreads);
    threadPool->Start();

    return std::make_shared<TRdmaTarget>(
        std::move(rdmaTargetConfig),
        std::move(logging),
        std::move(server),
        std::move(threadPool),
        std::move(service));
}

}   // namespace NCloud::NBlockStore::NStorage
