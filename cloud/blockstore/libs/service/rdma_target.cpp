#include "rdma_target.h"

#include "cloud/storage/core/libs/common/thread_pool.h"
#include "rdma_protocol.h"
#include "request_helpers.h"
#include "service.h"

#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace NMonitoring;

namespace {

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

BLOCKSTORE_RDMA_STORAGE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

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
private:
    TLog Log;

    std::weak_ptr<NRdma::IServerEndpoint> Endpoint;
    const NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreServerProtocol::Serializer();

    IBlockStorePtr Service;

    ITaskQueuePtr TaskQueue;

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

    void HandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        auto doHandleRequest = [self = shared_from_this(),
                                context = context,
                                callContext = std::move(callContext),
                                in = in,
                                out = out]() mutable -> NProto::TError
        {
            return self
                ->DoHandleRequest(context, std::move(callContext), in, out);
        };

        auto safeHandleRequest =
            [endpoint = Endpoint,
             context = context,
             doHandleRequest = std::move(doHandleRequest)]() mutable
        {
            auto error =
                SafeExecute<NProto::TError>(std::move(doHandleRequest));

            if (error.GetCode()) {
                if (auto ep = endpoint.lock()) {
                    ep->SendError(context, error.GetCode(), error.GetMessage());
                }
            }
        };

        TaskQueue->ExecuteSimple(std::move(safeHandleRequest));
    }

private:
#define BLOCKSTORE_HANDLE_REQUEST(name, ...)            \
    case TBlockStoreServerProtocol::Ev##name##Request:  \
        return Handle##name##Request(                   \
            context,                                    \
            std::move(callContext),                     \
            std::shared_ptr<NProto::T##name##Request>(  \
                static_cast<NProto::T##name##Request*>( \
                    parseResult.Proto.release())),      \
            parseResult.Data,                           \
            out,                                        \
            parseResult.Flags);                         \
        // BLOCKSTORE_HANDLE_REQUEST

    NProto::TError DoHandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) const
    {
        auto resultOrError = Serializer->Parse(in);

        if (HasError(resultOrError.GetError())) {
            STORAGE_ERROR(
                "Can't parse input: %s",
                FormatError(resultOrError.GetError()).c_str())
            return resultOrError.GetError();
        }
        auto parseResult = resultOrError.ExtractResult();

        STORAGE_DEBUG("Processing req with msgId %d", parseResult.MsgId);

        switch (parseResult.MsgId) {
            BLOCKSTORE_RDMA_STORAGE_SERVICE(BLOCKSTORE_HANDLE_REQUEST)

            default:
                return MakeError(E_NOT_IMPLEMENTED);
        }
    }
#undef BLOCKSTORE_HANDLE_REQUEST

    template <typename TMethod, typename THandleMethod>
    NProto::TError HandleRequest(
        void* context,
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request,
        TStringBuf requestData,
        TStringBuf out,
        ui32 flags,
        THandleMethod handleMethod) const
    {
        Y_UNUSED(flags);
        auto future =
            TMethod::Execute(*Service, callContext, std::move(request));
        SubscribeForResponse(
            std::move(future),
            TRequestDetails{
                .Context = context,
                .Out = out,
                .DataBuffer = requestData,
            },
            std::move(handleMethod));
        return {};
    }

    NProto::TError HandleReadBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        TStringBuf requestData,
        TStringBuf out,
        ui32 flags) const
    {
        return HandleRequest<TReadBlocksMethod>(
            context,
            std::move(callContext),
            std::move(request),
            requestData,
            out,
            flags,
            &TRequestHandler::HandleReadBlocksResponse);
    }

    NProto::TError HandleZeroBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        TStringBuf requestData,
        TStringBuf out,
        ui32 flags) const
    {
        return HandleRequest<TZeroBlocksMethod>(
            context,
            std::move(callContext),
            std::move(request),
            requestData,
            out,
            flags,
            &TRequestHandler::HandleZeroBlocksResponse);
    }

    NProto::TError HandleWriteBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        TStringBuf requestData,
        TStringBuf out,
        ui32 flags) const
    {
        if (!HasProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END)) {
            return HandleRequest<TWriteBlocksMethod>(
                context,
                std::move(callContext),
                std::move(request),
                requestData,
                out,
                flags,
                &TRequestHandler::HandleWriteBlocksResponse);
        }
        size_t blocksCount = request->GetBlocks().BuffersSize();
        if (blocksCount == 0) {
            return MakeError(
                E_ARGUMENT,
                "provide block count with blocks field");
        }
        if (requestData.Size() % blocksCount != 0) {
            return MakeError(
                E_ARGUMENT,
                "request data is not divisible by blocks count");
        }
        auto blockSize = requestData.Size() / blocksCount;

        request->MutableBlocks()->ClearBuffers();
        for (size_t i = 0; i < blocksCount; ++i) {
            request->MutableBlocks()->AddBuffers(
                TString(requestData.Head(blockSize)));

            requestData.Skip(blockSize);
        }

        return HandleRequest<TWriteBlocksMethod>(
            context,
            std::move(callContext),
            std::move(request),
            requestData,
            out,
            flags,
            &TRequestHandler::HandleWriteBlocksResponse);
    }

#define BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION(name, ...) \
    NProto::TError Handle##name##Request(                     \
        void* context,                                        \
        TCallContextPtr callContext,                          \
        std::shared_ptr<NProto::T##name##Request> request,    \
        TStringBuf requestData,                               \
        TStringBuf out,                                       \
        ui32 flags) const                                     \
    {                                                         \
        return HandleRequest<T##name##Method>(                \
            context,                                          \
            std::move(callContext),                           \
            std::move(request),                               \
            requestData,                                      \
            out,                                              \
            flags,                                            \
            &TRequestHandler::Handle##name##Response);        \
    }                                                         \
    // BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION

    BLOCKSTORE_RDMA_STORAGE_SERVICE_CONTROL_PLANE(
        BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION);

#undef BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION

    template <typename TFuture>
    void HandleResponse(
        const TRequestDetails& requestDetails,
        TFuture future,
        uint msgId) const
    {
        const auto& response = future.GetValue();
        STORAGE_DEBUG("sending response with msgId %d", msgId);

        const size_t totalLen =
            NRdma::TProtoMessageSerializer::MessageByteSize(response, 0);
        if (requestDetails.Out.Size() < totalLen) {
            if (auto ep = Endpoint.lock()) {
                ep->SendError(
                    requestDetails.Context,
                    E_ARGUMENT,
                    "Output buffer is too smal, can't response to request");
            }
            return;
        }
        size_t bytes = NRdma::TProtoMessageSerializer::Serialize(
            requestDetails.Out,
            msgId,
            0,
            response);

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(requestDetails.Context, bytes);
        }
    }

#define BLOCKSTORE_DECLARE_HANDLE_RESPONSE_FUNCTION(name, ...) \
    void Handle##name##Response(                               \
        const TRequestDetails& requestDetails,                 \
        TFuture<NProto::T##name##Response> future) const       \
    {                                                          \
        HandleResponse(                                        \
            requestDetails,                                    \
            std::move(future),                                 \
            TBlockStoreServerProtocol::Ev##name##Response);    \
    }                                                          \
    // BLOCKSTORE_DECLARE_HANDLE_RESPONSE_FUNCTION

    BLOCKSTORE_RDMA_STORAGE_SERVICE_CONTROL_PLANE(
        BLOCKSTORE_DECLARE_HANDLE_RESPONSE_FUNCTION);

#undef BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION

    static size_t SerializeReadBlocksResponseToBuffer(
        const NProto::TReadBlocksResponse& response,
        TRequestDetails& requestDetails)
    {
        const auto& blocks = response.GetBlocks();
        const auto& error = response.GetError();

        if (HasError(error)) {
            return NRdma::TProtoMessageSerializer::Serialize(
                requestDetails.Out,
                TBlockStoreServerProtocol::EvReadBlocksResponse,
                0,
                response);
        }

        NProto::TReadBlocksResponse proto;
        proto.MutableTrace()->CopyFrom(response.GetTrace());
        *proto.MutableUnencryptedBlockMask() =
            response.GetUnencryptedBlockMask();
        proto.SetThrottlerDelay(response.GetThrottlerDelay());
        proto.SetAllZeroes(response.GetAllZeroes());

        auto blocksCount = blocks.BuffersSize();
        auto blockSize = blocks.GetBuffers(0).Size();
        auto tailSize = blocksCount * blockSize;
        auto dataBuffer = requestDetails.Out;

        auto totalLen =
            NRdma::TProtoMessageSerializer::MessageByteSize(proto, tailSize);
        if (dataBuffer.size() < totalLen) {
            NProto::TReadBlocksResponse proto;
            *proto.MutableError() = MakeError(
                E_ARGUMENT,
                "Output buffer is too smal, can't response to request");
            return NRdma::TProtoMessageSerializer::Serialize(
                requestDetails.Out,
                TBlockStoreServerProtocol::EvReadBlocksResponse,
                0,
                proto);
        }

        dataBuffer = dataBuffer.RSeek(blocksCount * blockSize);
        for (const auto& buffer: blocks.GetBuffers()) {
            proto.MutableBlocks()->AddBuffers();
            char* dataBufferBegin = const_cast<char*>(dataBuffer.Data());
            MemCopy(dataBufferBegin, buffer.Data(), blockSize);
            dataBuffer = dataBuffer.RSeek(dataBuffer.size() - blockSize);
        }

        ui32 flags = 0;
        SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);

        NRdma::TProtoMessageSerializer::SerializeWithDataLength(
            requestDetails.Out,
            TBlockStoreServerProtocol::EvReadBlocksResponse,
            flags,
            proto,
            tailSize);
        return requestDetails.Out.Size();
    }

    void HandleReadBlocksResponse(
        TRequestDetails& requestDetails,
        TFuture<NProto::TReadBlocksResponse> future) const
    {
        const auto& response = future.GetValue();
        size_t bytes =
            SerializeReadBlocksResponseToBuffer(response, requestDetails);
        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(requestDetails.Context, bytes);
        }
    }

    void HandleZeroBlocksResponse(
        TRequestDetails& requestDetails,
        TFuture<NProto::TZeroBlocksResponse> future) const
    {
        HandleResponse(
            requestDetails,
            std::move(future),
            TBlockStoreServerProtocol::EvZeroBlocksResponse);
    }

    void HandleWriteBlocksResponse(
        TRequestDetails& requestDetails,
        TFuture<NProto::TWriteBlocksResponse> future) const
    {
        HandleResponse(
            requestDetails,
            std::move(future),
            TBlockStoreServerProtocol::EvWriteBlocksResponse);
    }

    template <typename TFuture, typename THandleResponseMethod>
    void SubscribeForResponse(
        TFuture future,
        TRequestDetails requestDetails,
        THandleResponseMethod handleResponseMethod) const
    {
        auto handleResponse = [self = shared_from_this(),
                               requestDetails = requestDetails,
                               handleResponseMethod =
                                   handleResponseMethod](TFuture future) mutable
        {
            self->TaskQueue->ExecuteSimple(
                [self = self,
                 future = std::move(future),
                 requestDetails = requestDetails,
                 handleResponseMethod = handleResponseMethod]() mutable
                {
                    const TRequestHandler* obj = self.get();
                    // Call TRequestHandler::HandleXXXResponse()
                    (obj->*handleResponseMethod)(
                        requestDetails,
                        std::move(future));
                });
        };
        future.Subscribe(std::move(handleResponse));
    }
};

///////////////////////////////////////////////////////////////////////////////

class TRdmaTarget final: public IStartable
{
private:
    const TRdmaTargetConfigPtr Config;

    ILoggingServicePtr Logging;
    NRdma::IServerPtr Server;
    ITaskQueuePtr TaskQueue;

    std::shared_ptr<TRequestHandler> Handler;

    TLog Log;

public:
    TRdmaTarget(
            TRdmaTargetConfigPtr rdmaTargetConfig,
            ILoggingServicePtr logging,
            NRdma::IServerPtr server,
            IBlockStorePtr service,
            ITaskQueuePtr taskQueue)
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
    TRdmaTargetConfigPtr rdmaTargetConfig,
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
        std::move(service),
        std::move(threadPool));
}

}   // namespace NCloud::NBlockStore::NStorage
