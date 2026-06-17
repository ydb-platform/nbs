#include "rdma_target.h"

#include "rdma_protocol.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/rdma/iface/probes.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace NMonitoring;
using namespace NCloud::NStorage::NRdma;

LWTRACE_USING(STORAGE_RDMA_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MaxRealProtoSize =
    4_KB - NCloud::NStorage::NRdma::RDMA_PROTO_HEADER_SIZE;

////////////////////////////////////////////////////////////////////////////////

#define Y_ENSURE_RETURN(expr, message)                             \
    if (Y_UNLIKELY(!(expr))) {                                     \
        return MakeError(E_ARGUMENT, TStringBuilder() << message); \
    }                                                              \
    // Y_ENSURE_RETURN

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

template <typename TResponse>
void FillResponse(const TCallContextPtr& callContext, TResponse& response)
{
    const ui64 postponeTime =
        callContext->Time(EProcessingStage::Postponed).MicroSeconds();
    response.SetDeprecatedThrottlerDelay(postponeTime);
    response.MutableHeaders()->MutableThrottler()->SetDelay(postponeTime);

    const ui64 shapingTime =
        callContext->Time(EProcessingStage::Shaping).MicroSeconds();
    response.MutableHeaders()->MutableThrottler()->SetShapingDelay(shapingTime);
}

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. After Init() public method HandleRequest() can be called
// from any thread.
class TRequestHandler final
    : public NCloud::NStorage::NRdma::IServerHandler
    , public std::enable_shared_from_this<TRequestHandler>
{
    IBlockStorePtr Service;
    ITraceSerializerPtr TraceSerializer;
    ITaskQueuePtr TaskQueue;

    TLog Log;
    std::weak_ptr<NCloud::NStorage::NRdma::IServerEndpoint> Endpoint;

    const NCloud::NStorage::NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreServerProtocol::Serializer();

public:
    TRequestHandler(
        IBlockStorePtr service,
        ITraceSerializerPtr traceSerializer,
        ITaskQueuePtr taskQueue)
        : Service(std::move(service))
        , TraceSerializer(std::move(traceSerializer))
        , TaskQueue(std::move(taskQueue))
    {}

    void Init(
        const NCloud::NStorage::NRdma::IServerEndpointPtr& endpoint,
        TLog log)
    {
        Endpoint = endpoint;
        Log = std::move(log);
    }

    TCallContextBasePtr CreateCallContext() override
    {
        return NCloud::NBlockStore::CreateCallContext();
    }

private:
#define BLOCKSTORE_HANDLE_REQUEST(name, ...)                             \
    case TBlockStoreServerProtocol::Ev##name##Request:                   \
        return Handle##name##Request(                                    \
            context,                                                     \
            std::move(callContext),                                      \
            static_cast<NProto::T##name##Request*>(&*parseResult.Proto), \
            parseResult.Data,                                            \
            out);

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
            BLOCKSTORE_HANDLE_REQUEST(Ping)
            BLOCKSTORE_HANDLE_REQUEST(MountVolume)
            BLOCKSTORE_HANDLE_REQUEST(UnmountVolume)

            default:
                return MakeError(
                    E_NOT_IMPLEMENTED,
                    TStringBuilder()
                        << "Request with msg id " << parseResult.MsgId
                        << " is not supported by blockstore server RDMA "
                           "target");
        }
    }

#undef BLOCKSTORE_HANDLE_REQUEST

    void HandleRequest(
        void* context,
        TCallContextBasePtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        TaskQueue->ExecuteSimple(
            [=,
             endpoint = Endpoint,
             callContext =
                 ToBlockStoreCallContext(std::move(callContext))]() mutable
            {
                auto error = SafeExecute<NProto::TError>(
                    [=]()
                    { return DoHandleRequest(context, callContext, in, out); });

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

    size_t SerializeReadBlocksResponse(
        const NProto::TReadBlocksResponse& response,
        TStringBuf out,
        ui32 flags,
        TGuardedSgList::TGuard& guard) const
    {
        return SUCCEEDED(response.GetError().GetCode())
                   ? TProtoMessageSerializer::SerializeWithData(
                         out,
                         TBlockStoreServerProtocol::EvReadBlocksResponse,
                         flags,
                         response,
                         guard.Get())
                   : TProtoMessageSerializer::Serialize(
                         out,
                         TBlockStoreServerProtocol::EvReadBlocksResponse,
                         flags,
                         response);
    }

    template <typename TResponse>
    void OnSerializeException(
        TResponse& response,
        TStringBuf responseName) const
    {
        STORAGE_ERROR(
            TStringBuilder()
            << "Unable to serialize " << responseName << " protobuf: ["
            << response.ShortDebugString()
            << "] with exception: " << CurrentExceptionMessage());
    }

    NProto::TError HandleReadBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TReadBlocksRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");
        Y_ENSURE_RETURN(request->GetBlockSize() != 0, "empty BlockSize");

        TGuardedBuffer buffer(
            TString::Uninitialized(
                static_cast<size_t>(request->GetBlockSize()) *
                request->GetBlocksCount()));

        auto [sglist, error] = SgListNormalize(
            TBlockDataRef{buffer.Get().data(), buffer.Get().length()},
            request->GetBlockSize());
        Y_ENSURE_RETURN(error.GetCode() == 0, "cannot create sgList");

        auto guardedSgList = buffer.CreateGuardedSgList(std::move(sglist));

        auto req = std::make_shared<NProto::TReadBlocksLocalRequest>();
        req->CopyFrom(*request);

        req->Sglist = guardedSgList;

        auto future = Service->ReadBlocksLocal(callContext, std::move(req));

        future.Subscribe(
            [=,
             buffer = std::move(buffer),
             guardedSgList = std::move(guardedSgList),
             blockSize = request->GetBlockSize(),
             taskQueue = TaskQueue,
             endpoint = Endpoint,
             weakSelf = weak_from_this()](auto future) mutable
            {
                auto response = ExtractResponse(future);
                FillResponse(callContext, response);

                taskQueue->ExecuteSimple(
                    [=,
                     buffer = std::move(buffer),
                     guardedSgList = std::move(guardedSgList),
                     weakSelf = std::move(weakSelf)]() mutable
                    {
                        if (response.ByteSizeLong() > MaxRealProtoSize) {
                            // TODO: consider variable length proto size
                            // or switch from lwtrace to open telemetry like
                            // solution to avoid sending traces between nodes
                            response.MutableDeprecatedTrace()->Clear();
                            response.MutableHeaders()->ClearTrace();
                        }

                        auto guard = guardedSgList.Acquire();
                        if (!guard) {
                            *response.MutableError() = MakeError(
                                E_CANCELLED,
                                "failed to acquire sglist in Rdma ReadBlocks "
                                "handler");
                        }

                        ui32 flags = 0;
                        SetProtoFlag(
                            flags,
                            NCloud::NStorage::NRdma::
                                RDMA_PROTO_FLAG_DATA_AT_THE_END);

                        size_t responseBytes = 0;
                        try {
                            responseBytes = SerializeReadBlocksResponse(
                                response,
                                out,
                                flags,
                                guard);
                        } catch (...) {
                            if (auto self = weakSelf.lock()) {
                                self->OnSerializeException(
                                    response,
                                    "ReadBlocks");
                            }
                            response = NProto::TReadBlocksLocalResponse{};
                            *response.MutableError() = MakeError(
                                E_REJECTED,
                                "Unable to serialize ReadBlocks response");
                            if (auto ep = endpoint.lock()) {
                                ep->SendError(
                                    context,
                                    E_REJECTED,
                                    "unable to serialize ReadBlocks response");
                            }
                            return;
                        }

                        if (auto ep = endpoint.lock()) {
                            ep->SendResponse(context, responseBytes);
                        }
                    });
            });

        return {};
    }

    NProto::TError HandleWriteBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TWriteBlocksRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() > 0, "invalid request");
        auto [sglist, error] = SgListNormalize(
            {requestData.data(), requestData.length()},
            request->GetBlockSize());
        Y_ENSURE_RETURN(error.GetCode() == 0, "cannot create sgList");

        TGuardedSgList guardedSgList(sglist);

        auto req = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        req->CopyFrom(*request);

        req->Sglist = guardedSgList;
        req->SetBlockSize(request->GetBlockSize());
        req->BlocksCount = requestData.length() / req->GetBlockSize();

        auto future = Service->WriteBlocksLocal(callContext, std::move(req));

        future.Subscribe(
            [=,
             taskQueue = TaskQueue,
             endpoint = Endpoint,
             weakSelf = weak_from_this()](auto future)
            {
                taskQueue->ExecuteSimple(
                    [=, responseFuture = future]() mutable
                    {
                        NProto::TWriteBlocksLocalResponse response;
                        try {
                            response = responseFuture.GetValue();
                        } catch (...) {
                            *response.MutableError() = MakeError(
                                E_REJECTED,
                                TStringBuilder()
                                    << "unable to GetValue from "
                                       "WriteBlocksLocalResponse: "
                                    << CurrentExceptionMessage());
                        }
                        FillResponse(callContext, response);

                        guardedSgList.Close();
                        if (response.ByteSizeLong() > MaxRealProtoSize) {
                            // TODO: consider variable length proto size
                            // or switch from lwtrace to open telemetry like
                            // solution to avoid sending traces between nodes
                            response.MutableDeprecatedTrace()->Clear();
                            response.MutableHeaders()->ClearTrace();
                        }

                        ui32 flags = 0;
                        SetProtoFlag(
                            flags,
                            NCloud::NStorage::NRdma::
                                RDMA_PROTO_FLAG_DATA_AT_THE_END);

                        size_t responseBytes = 0;
                        try {
                            responseBytes = TProtoMessageSerializer::Serialize(
                                out,
                                TBlockStoreServerProtocol::
                                    EvWriteBlocksResponse,
                                flags,   // flags
                                response);
                        } catch (...) {
                            if (auto self = weakSelf.lock()) {
                                self->OnSerializeException(
                                    response,
                                    "WriteBlocks");
                            }
                            response = NProto::TWriteBlocksLocalResponse{};
                            *response.MutableError() = MakeError(
                                E_REJECTED,
                                "Unable to serialize WriteBlocks response");
                            if (auto ep = endpoint.lock()) {
                                ep->SendError(
                                    context,
                                    E_REJECTED,
                                    "unable to serialize WriteBlocks response");
                            }
                            return;
                        }
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
        NProto::TZeroBlocksRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        auto req =
            std::make_shared<NProto::TZeroBlocksRequest>(std::move(*request));

        auto future = Service->ZeroBlocks(callContext, std::move(req));

        future.Subscribe(
            [out = out,
             context = context,
             endpoint = Endpoint,
             callContext = std::move(callContext),
             weakSelf = weak_from_this()](auto future)
            {
                auto response = ExtractResponse(future);
                FillResponse(callContext, response);

                if (response.ByteSizeLong() > MaxRealProtoSize) {
                    // TODO: consider variable length proto size
                    // or switch from lwtrace to open telemetry like
                    // solution to avoid sending traces between nodes
                    response.MutableDeprecatedTrace()->Clear();
                    response.MutableHeaders()->ClearTrace();
                }

                ui32 flags = 0;
                SetProtoFlag(
                    flags,
                    NCloud::NStorage::NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);

                size_t responseBytes = 0;
                try {
                    responseBytes = TProtoMessageSerializer::Serialize(
                        out,
                        TBlockStoreServerProtocol::EvZeroBlocksResponse,
                        flags,   // flags
                        response);
                } catch (...) {
                    if (auto self = weakSelf.lock()) {
                        self->OnSerializeException(
                            response,
                            "ZeroBlocks");
                    }
                    response = NProto::TZeroBlocksResponse{};
                    *response.MutableError() =
                        MakeError(
                            E_REJECTED,
                            "Unable to serialize ZeroBlocks response");
                    if (auto ep = endpoint.lock()) {
                        ep->SendError(
                            context,
                            E_REJECTED,
                            "unable to serialize ZeroBlocks response");
                    }
                    return;
                }
                if (auto ep = endpoint.lock()) {
                    ep->SendResponse(context, responseBytes);
                }
            });

        return {};
    }

    NProto::TError HandlePingRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TPingRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        NProto::TPingResponse response;

        ui32 flags = 0;
        SetProtoFlag(
            flags,
            NCloud::NStorage::NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);

        size_t responseBytes = TProtoMessageSerializer::Serialize(
            out,
            TBlockStoreServerProtocol::EvPingResponse,
            flags,   // flags
            response);

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(context, responseBytes);
        }

        return {};
    }

    NProto::TError HandleMountVolumeRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TMountVolumeRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        auto req =
            std::make_shared<NProto::TMountVolumeRequest>(std::move(*request));

        auto future = Service->MountVolume(callContext, std::move(req));

        future.Subscribe(
            [out = out,
             context = context,
             endpoint = Endpoint,
             callContext = std::move(callContext),
             weakSelf = weak_from_this()](auto future)
            {
                auto response = ExtractResponse(future);

                ui32 flags = 0;
                size_t responseBytes = 0;
                try {
                    responseBytes = TProtoMessageSerializer::Serialize(
                        out,
                        TBlockStoreServerProtocol::EvMountVolumeResponse,
                        flags,   // flags
                        response);
                } catch (...) {
                    if (auto self = weakSelf.lock()) {
                        self->OnSerializeException(
                            response,
                            "MountVolume");
                    }
                    response = NProto::TMountVolumeResponse{};
                    *response.MutableError() =
                        MakeError(
                            E_REJECTED,
                            "Unable to serialize MountVolume response");
                    if (auto ep = endpoint.lock()) {
                        ep->SendError(
                            context,
                            E_REJECTED,
                            "unable to serialize MountVolume response");
                    }
                    return;
                }

                if (auto ep = endpoint.lock()) {
                    ep->SendResponse(context, responseBytes);
                }
            });

        return {};
    }

    NProto::TError HandleUnmountVolumeRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TUnmountVolumeRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        auto req = std::make_shared<NProto::TUnmountVolumeRequest>(
            std::move(*request));

        auto future = Service->UnmountVolume(callContext, std::move(req));

        future.Subscribe(
            [out = out,
             context = context,
             endpoint = Endpoint,
             callContext = std::move(callContext),
             weakSelf = weak_from_this()](auto future)
            {
                auto response = ExtractResponse(future);

                ui32 flags = 0;
                size_t responseBytes = 0;
                try {
                    responseBytes = TProtoMessageSerializer::Serialize(
                        out,
                        TBlockStoreServerProtocol::EvUnmountVolumeResponse,
                        flags,   // flags
                        response);
                } catch (...) {
                    if (auto self = weakSelf.lock()) {
                        self->OnSerializeException(
                            response,
                            "UnmountVolume");
                    }
                    response = NProto::TUnmountVolumeResponse{};
                    *response.MutableError() =
                        MakeError(
                            E_REJECTED,
                            "Unable to serialize UnmountVolume response");
                    if (auto ep = endpoint.lock()) {
                        ep->SendError(
                            context,
                            E_REJECTED,
                            "unable to serialize UnmountVolume response");
                    }
                    return;
                }

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
    ITraceSerializerPtr TraceSerializer;
    NCloud::NStorage::NRdma::IServerPtr Server;
    ITaskQueuePtr TaskQueue;

    std::shared_ptr<TRequestHandler> Handler;

    TLog Log;

public:
    TRdmaTarget(
        TBlockstoreServerRdmaTargetConfigPtr rdmaTargetConfig,
        ILoggingServicePtr logging,
        ITraceSerializerPtr traceSerializer,
        NCloud::NStorage::NRdma::IServerPtr server,
        ITaskQueuePtr taskQueue,
        IBlockStorePtr service)
        : Config(std::move(rdmaTargetConfig))
        , Logging(std::move(logging))
        , TraceSerializer(std::move(traceSerializer))
        , Server(std::move(server))
        , TaskQueue(std::move(taskQueue))
    {
        Handler = std::make_shared<TRequestHandler>(
            std::move(service),
            TraceSerializer,
            TaskQueue);
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
    ITraceSerializerPtr traceSerializer,
    NCloud::NStorage::NRdma::IServerPtr server,
    IBlockStorePtr service)
{
    auto threadPool = CreateThreadPool("RDMA", rdmaTargetConfig->WorkerThreads);
    threadPool->Start();

    return std::make_shared<TRdmaTarget>(
        std::move(rdmaTargetConfig),
        std::move(logging),
        std::move(traceSerializer),
        std::move(server),
        std::move(threadPool),
        std::move(service));
}

}   // namespace NCloud::NBlockStore::NStorage
