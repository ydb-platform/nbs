#include "rdma_target.h"

#include "rdma_protocol.h"

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

bool IsLocalRequest(ui32 msgId)
{
    switch (msgId) {
        BLOCKSTORE_LOCAL_SERVICE(BLOCKSTORE_RETURN_TRUE_CASE)
        default:
            return false;
    }
}

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
        auto [parseResult, error] = Serializer->Parse(in);

        if (HasError(error)) {
            STORAGE_ERROR("Can't parse input: %s", FormatError(error).c_str())
            return error;
        }

        if (IsLocalRequest(parseResult.MsgId)) {
            return MakeError(
                E_NOT_IMPLEMENTED,
                "Local requests are not supported for blockstore server RDMA "
                "target");
        }

        STORAGE_TRACE("Processing req with msgId %u", parseResult.MsgId);

        switch (parseResult.MsgId) {
            BLOCKSTORE_SERVICE(BLOCKSTORE_HANDLE_REQUEST)

            default:
                return MakeError(
                    E_NOT_IMPLEMENTED,
                    "Request with msg id %u are not supported by blockstore "
                    "server RDMA target",
                    parseResult.MsgId);
        }
    }
#undef BLOCKSTORE_HANDLE_REQUEST

    template <typename TMethod, typename THandleMethod>
    NProto::TError GeneralHandleRequest(
        void* context,
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request,
        TStringBuf requestData,
        TStringBuf out,
        ui32 flags,
        THandleMethod handleMethod) const
    {
        Y_UNUSED(flags);
        auto future = TMethod::Execute(
            *Service,
            std::move(callContext),
            std::move(request));
        SubscribeForResponse(
            future,
            TRequestDetails{
                .Context = context,
                .Out = out,
                .DataBuffer = requestData,
            },
            std::move(handleMethod));
        return {};
    }

    template <typename TMethod>
    using THandleMethod = std::function<void(
        TRequestDetails& requestDetails,
        TFuture<typename TMethod::TResponse>& future)>;

    template <typename TMethod>
    NProto::TError HandleRequest(
        void* context,
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request,
        TStringBuf requestData,
        TStringBuf out,
        ui32 flags,
        THandleMethod<TMethod> handleMethod) const
    {
        return GeneralHandleRequest<TMethod>(
            context,
            std::move(callContext),
            std::move(request),
            requestData,
            out,
            flags,
            std::move(handleMethod));
    }

    // TODO: convert request to TWriteBlocksLocalRequest and think about same
    // converting for TReadBlocksRequest
    template <>
    NProto::TError HandleRequest<TWriteBlocksMethod>(
        void* context,
        TCallContextPtr callContext,
        std::shared_ptr<typename TWriteBlocksMethod::TRequest> request,
        TStringBuf requestData,
        TStringBuf out,
        ui32 flags,
        THandleMethod<TWriteBlocksMethod> handleMethod) const
    {
        if (!requestData) {
            return GeneralHandleRequest<TWriteBlocksMethod>(
                context,
                std::move(callContext),
                std::move(request),
                requestData,
                out,
                flags,
                std::move(handleMethod));
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

        return GeneralHandleRequest<TWriteBlocksMethod>(
            context,
            std::move(callContext),
            std::move(request),
            requestData,
            out,
            flags,
            std::move(handleMethod));
    }

#define BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION(name, ...)                \
    NProto::TError Handle##name##Request(                                    \
        void* context,                                                       \
        TCallContextPtr callContext,                                         \
        std::shared_ptr<NProto::T##name##Request> request,                   \
        TStringBuf requestData,                                              \
        TStringBuf out,                                                      \
        ui32 flags) const                                                    \
    {                                                                        \
        auto methodCall =                                                    \
            [self = this](                                                   \
                TRequestDetails& requestDetails,                             \
                TFuture<typename T##name##Method::TResponse> future)         \
        {                                                                    \
            self->Handle##name##Response(requestDetails, std::move(future)); \
        };                                                                   \
        return HandleRequest<T##name##Method>(                               \
            context,                                                         \
            std::move(callContext),                                          \
            std::move(request),                                              \
            requestData,                                                     \
            out,                                                             \
            flags,                                                           \
            std::move(methodCall));                                          \
    }                                                                        \
    // BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION);

#undef BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION

    template <typename TMethod>
    void GeneralHandleResponse(
        const TRequestDetails& requestDetails,
        TFuture<typename TMethod::TResponse>& future,
        uint msgId) const
    {
        const auto& response = future.GetValue();
        STORAGE_TRACE("sending response with msgId %d", msgId);

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

    template <typename TMethod>
    void HandleResponse(
        TRequestDetails& requestDetails,
        TFuture<typename TMethod::TResponse>& future,
        uint msgId) const
    {
        GeneralHandleResponse<TMethod>(requestDetails, future, msgId);
    }

    template <>
    void HandleResponse<TWriteBlocksLocalMethod>(
        TRequestDetails& requestDetails,
        TFuture<typename TWriteBlocksMethod::TResponse>& future,
        uint msgId) const
    {
        Y_UNUSED(msgId);
        GeneralHandleResponse<TWriteBlocksMethod>(
            requestDetails,
            future,
            TBlockStoreServerProtocol::EvWriteBlocksResponse);
    }

    template <>
    void HandleResponse<TReadBlocksMethod>(
        TRequestDetails& requestDetails,
        TFuture<NProto::TReadBlocksResponse>& future,
        uint msgId) const
    {
        const auto& response = future.GetValue();
        if (HasError(response.GetError()) ||
            response.GetBlocks().BuffersSize() == 0)
        {
            GeneralHandleResponse<TReadBlocksMethod>(
                requestDetails,
                future,
                msgId);
            return;
        }

        const auto& resp = future.GetValue();

        NProto::TReadBlocksResponse proto;
        proto.MutableError()->CopyFrom(resp.GetError());
        proto.MutableTrace()->CopyFrom(resp.GetTrace());
        *proto.MutableUnencryptedBlockMask() = resp.GetUnencryptedBlockMask();
        proto.SetThrottlerDelay(resp.GetThrottlerDelay());
        proto.SetAllZeroes(resp.GetAllZeroes());

        bool hasVoidBlocks = false;
        size_t blockSize = 0;
        for (const auto& buf: resp.GetBlocks().GetBuffers()) {
            if (!buf.Data() || !buf.Size()) {
                hasVoidBlocks = true;
            } else {
                blockSize = buf.Size();
            }

            proto.MutableBlocks()
                ->AddBuffers();   // Add empty buffers to pass blocks count.
        }

        if (!blockSize) {
            // we can't do anything with only void blocks
            GeneralHandleResponse<TReadBlocksMethod>(
                requestDetails,
                future,
                msgId);
            return;
        }

        auto zeroBlock = hasVoidBlocks ? TString(blockSize, '\0') : TString();
        TSgList buffers;
        buffers.reserve(resp.GetBlocks().BuffersSize());
        for (const auto& buf: resp.GetBlocks().GetBuffers()) {
            if (!buf.Data() || !buf.Size()) {
                buffers.emplace_back(zeroBlock.Data(), zeroBlock.Size());
            } else {
                buffers.emplace_back(buf.Data(), buf.Size());
            }
        }

        auto totalLen = NRdma::TProtoMessageSerializer::MessageByteSize(
            proto,
            SgListGetSize(buffers));
        if (requestDetails.Out.Size() < totalLen) {
            if (auto ep = Endpoint.lock()) {
                ep->SendError(
                    requestDetails.Context,
                    E_ARGUMENT,
                    "output buffer is too smal to serialize response");
            }
            return;
        }

        size_t responseBytes =
            NRdma::TProtoMessageSerializer::SerializeWithData(
                requestDetails.Out,
                TBlockStoreServerProtocol::EvReadBlocksResponse,
                0,   // flags
                proto,
                buffers);
        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(requestDetails.Context, responseBytes);
        }
    }

#define BLOCKSTORE_DECLARE_HANDLE_RESPONSE_FUNCTION(name, ...) \
    void Handle##name##Response(                               \
        TRequestDetails& requestDetails,                       \
        TFuture<NProto::T##name##Response> future) const       \
    {                                                          \
        HandleResponse<T##name##Method>(                       \
            requestDetails,                                    \
            future,                                            \
            TBlockStoreServerProtocol::Ev##name##Response);    \
    }                                                          \
    // BLOCKSTORE_DECLARE_HANDLE_RESPONSE_FUNCTION

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_HANDLE_RESPONSE_FUNCTION);

#undef BLOCKSTORE_DECLARE_HANDLE_REQUEST_FUNCTION

    template <typename TFuture, typename THandleResponseMethod>
    void SubscribeForResponse(
        TFuture& future,
        TRequestDetails requestDetails,
        THandleResponseMethod handleResponseMethod) const
    {
        auto handleResponse =
            [self = shared_from_this(),
             requestDetails = requestDetails,
             handleResponseMethod =
                 std::move(handleResponseMethod)](TFuture future) mutable
        {
            self->TaskQueue->ExecuteSimple(
                [future = std::move(future),
                 requestDetails = requestDetails,
                 handleResponseMethod =
                     std::move(handleResponseMethod)]() mutable
                { handleResponseMethod(requestDetails, future); });
        };
        future.Subscribe(std::move(handleResponse));
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
