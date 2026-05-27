#include "rdma_client.h"

#include "protocol.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <util/datetime/base.h>
#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;
using namespace NCloud::NStorage;

namespace {

///////////////////////////////////////////////////////////////////////////////

constexpr TDuration WAIT_TIMEOUT = TDuration::Seconds(10);

constexpr size_t MAX_PROTO_SIZE = 4*1024;

///////////////////////////////////////////////////////////////////////////////

// Size of the serialized message or error if unable to serialize
using TPrepareResult = TResultOrError<size_t>;

///////////////////////////////////////////////////////////////////////////////

class TEndpointBase: public TBlockStoreImpl<TEndpointBase, IBlockStore>
{
public:
    TEndpointBase() = default;

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeFuture<typename TMethod::TResponse>(TErrorResponse(
            E_NOT_IMPLEMENTED,
            TStringBuilder()
                << "Unsupported request " << TMethod::GetName().Quote()));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IRequestHandler: public NRdma::TNullContext
{
    virtual void HandleResponse(TStringBuf buffer) = 0;
    virtual void HandleError(ui32 error, TStringBuf message) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
void ProcessThrottleTime(
    const TCallContextPtr& callContext,
    TResponse& localResponse)
{
    NProto::TThrottlerInfo& throttler =
        *localResponse.MutableHeaders()->MutableThrottler();
    const ui64 throttlerDelay =
        Max(localResponse.GetDeprecatedThrottlerDelay(), throttler.GetDelay());
    callContext->AddTime(
        EProcessingStage::Postponed,
        TDuration::MicroSeconds(throttlerDelay));
    localResponse.SetDeprecatedThrottlerDelay(0);
    throttler.SetDelay(0);
    callContext->SetPossiblePostponeDuration(TDuration::Zero());

    callContext->AddTime(
        EProcessingStage::Shaping,
        TDuration::MicroSeconds(throttler.GetShapingDelay()));
    throttler.SetShapingDelay(0);
}

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksHandler final
    : public IRequestHandler
{
public:
    using TRequest = NProto::TReadBlocksLocalRequest;
    using TResponse = NProto::TReadBlocksLocalResponse;

private:
    const TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    const ITraceSerializerPtr TraceSerializer;
    const bool IsAlignedDataEnabled;

    ui64 StartTime = 0;

    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

public:
    TReadBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            ITraceSerializerPtr traceSerializer,
            bool isAlignedDataEnabled)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , TraceSerializer(std::move(traceSerializer))
        , IsAlignedDataEnabled(isAlignedDataEnabled)
    {
        TraceSerializer->BuildTraceRequest(
            *Request->MutableHeaders()->MutableInternal()->MutableTrace(),
            CallContext->LWOrbit);

    }

    size_t GetRequestSize() const
    {
        return NRdma::TProtoMessageSerializer::MessageByteSize(*Request, 0);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE + (static_cast<size_t>(Request->GetBlockSize()) *
                                 Request->GetBlocksCount());
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    TPrepareResult PrepareRequest(TStringBuf buffer)
    {
        ui32 flags = 0;
        if (IsAlignedDataEnabled) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

        StartTime = GetCycleCount();

        return NRdma::TProtoMessageSerializer::Serialize(
            buffer,
            TBlockStoreProtocol::ReadBlocksRequest,
            flags,   // flags
            *Request);
    }

    void HandleResponse(TStringBuf buffer) override
    {
        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(TErrorResponse(resultOrError.GetError()));
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TBlockStoreProtocol::ReadBlocksResponse);

        auto& responseMsg = static_cast<NProto::TReadBlocksResponse&>(
            *response.Proto);
        TResponse localResponse;
        localResponse.CopyFrom(responseMsg);

        if (!HasError(responseMsg.GetError())) {
            auto result = CopyData(Request->Sglist, response.Data);
            if (HasError(result)) {
                *localResponse.MutableError() = std::move(result);
                Response.SetValue(std::move(localResponse));
                return;
            }
        }

        if (CallContext->LWOrbit.HasShuttles()) {
            TraceSerializer->HandleTraceInfo(
                responseMsg.GetHeaders().HasTrace()
                    ? responseMsg.GetHeaders().GetTrace()
                    : responseMsg.GetDeprecatedTrace(),
                CallContext->LWOrbit,
                StartTime,
                GetCycleCount());
            responseMsg.ClearDeprecatedTrace();
            responseMsg.MutableHeaders()->ClearTrace();
        }

        ProcessThrottleTime(CallContext, localResponse);

        Response.SetValue(std::move(localResponse));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }

private:
    NProto::TError CopyData(
        TGuardedSgList& guardedSgList,
        TStringBuf data)
    {
        auto guard = guardedSgList.Acquire();
        if (!guard) {
            return MakeError(
                E_CANCELLED,
                "failed to acquire sglist in Rdma ReadBlocks handler");
        }

        size_t expectedSize =
            static_cast<size_t>(Request->GetBlocksCount())
            * Request->GetBlockSize();
        size_t srcSize = data.length();

        if (srcSize != expectedSize) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "invalid response size (expected: " << expectedSize
                << ", actual: " << srcSize << ")");
        }

        const auto& dst = guard.Get();
        auto dstSize = SgListGetSize(dst);

        if (dstSize < srcSize) {
            return TErrorResponse(E_ARGUMENT, TStringBuilder()
                << "invalid buffer size (expected: " << srcSize
                << ", actual: " << dstSize << ")");
        }

        auto bytesRead = SgListCopy(
            TBlockDataRef{data.data(), data.length()},
            dst);

        STORAGE_VERIFY(
            bytesRead == dstSize,
            TWellKnownEntityTypes::DISK,
            GetDiskId(Request->GetDiskId()));

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBlocksHandler final
    : public IRequestHandler
{
public:
    using TRequest = NProto::TWriteBlocksLocalRequest;
    using TResponse = NProto::TWriteBlocksLocalResponse;

private:
    const TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    const ITraceSerializerPtr TraceSerializer;
    const bool IsAlignedDataEnabled;

    ui64 StartTime = 0;

    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

public:
    TWriteBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            ITraceSerializerPtr traceSerializer,
            bool isAlignedDataEnabled)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , TraceSerializer(std::move(traceSerializer))
        , IsAlignedDataEnabled(isAlignedDataEnabled)
    {
        TraceSerializer->BuildTraceRequest(
            *Request->MutableHeaders()->MutableInternal()->MutableTrace(),
            CallContext->LWOrbit);
    }

    size_t GetRequestSize() const
    {
        return NRdma::TProtoMessageSerializer::MessageByteSize(
            *Request,
            static_cast<size_t>(Request->GetBlockSize()) *
                Request->BlocksCount);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE;
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    TPrepareResult PrepareRequest(TStringBuf buffer)
    {
        auto guard = Request->Sglist.Acquire();
        if (!guard) {
            return TErrorResponse(
                E_CANCELLED,
                "failed to acquire sglist in Rdma WriteBlock handler");
        }

        const auto& src = guard.Get();
        auto srcSize = SgListGetSize(src);

        size_t expectedSize =
            static_cast<size_t>(Request->BlocksCount) * Request->GetBlockSize();
        if (srcSize != expectedSize) {
            return TErrorResponse(E_ARGUMENT, TStringBuilder()
                << "invalid buffer size (expected: " << expectedSize
                << ", actual: " << srcSize << ")");
        }

        ui32 flags = 0;
        if (IsAlignedDataEnabled) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

        StartTime = GetCycleCount();

        auto msgSize = NRdma::TProtoMessageSerializer::SerializeWithData(
            buffer,
            TBlockStoreProtocol::WriteBlocksRequest,
            flags,
            *Request,
            src);

        STORAGE_VERIFY(
            msgSize <= buffer.length(),
            TWellKnownEntityTypes::DISK,
            GetDiskId(Request));

        return msgSize;
    }

    void HandleResponse(TStringBuf buffer) override
    {
        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(TErrorResponse(resultOrError.GetError()));
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TBlockStoreProtocol::WriteBlocksResponse);
        Y_ENSURE(response.Data.length() == 0);

        auto& responseMsg = static_cast<TResponse&>(*response.Proto);

        if (CallContext->LWOrbit.HasShuttles()) {
            TraceSerializer->HandleTraceInfo(
                responseMsg.GetHeaders().HasTrace()
                    ? responseMsg.GetHeaders().GetTrace()
                    : responseMsg.GetDeprecatedTrace(),
                CallContext->LWOrbit,
                StartTime,
                GetCycleCount());
            responseMsg.ClearDeprecatedTrace();
            responseMsg.MutableHeaders()->ClearTrace();
        }

        ProcessThrottleTime(CallContext, responseMsg);

        Response.SetValue(std::move(responseMsg));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TZeroBlocksHandler final
    : public IRequestHandler
{
public:
    using TRequest = NProto::TZeroBlocksRequest;
    using TResponse = NProto::TZeroBlocksResponse;

private:
    const TCallContextPtr CallContext;
    const std::shared_ptr<TRequest> Request;
    const ITraceSerializerPtr TraceSerializer;
    const bool IsAlignedDataEnabled;

    ui64 StartTime = 0;

    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

public:
    TZeroBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            ITraceSerializerPtr traceSerializer,
            bool isAlignedDataEnabled)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , TraceSerializer(std::move(traceSerializer))
        , IsAlignedDataEnabled(isAlignedDataEnabled)
    {
        TraceSerializer->BuildTraceRequest(
            *Request->MutableHeaders()->MutableInternal()->MutableTrace(),
            CallContext->LWOrbit);
    }

    size_t GetRequestSize() const
    {
        return NRdma::TProtoMessageSerializer::MessageByteSize(*Request, 0);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE;
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    TPrepareResult PrepareRequest(TStringBuf buffer)
    {
        ui32 flags = 0;
        if (IsAlignedDataEnabled) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

        StartTime = GetCycleCount();

        return NRdma::TProtoMessageSerializer::Serialize(
            buffer,
            TBlockStoreProtocol::ZeroBlocksRequest,
            flags,   // flags
            *Request);
    }

    void HandleResponse(TStringBuf buffer) override
    {
        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(TErrorResponse(resultOrError.GetError()));
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TBlockStoreProtocol::ZeroBlocksResponse);
        Y_ENSURE(response.Data.length() == 0);

        auto& responseMsg = static_cast<TResponse&>(*response.Proto);

        TraceSerializer->HandleTraceInfo(
            responseMsg.GetHeaders().HasTrace()
                ? responseMsg.GetHeaders().GetTrace()
                : responseMsg.GetDeprecatedTrace(),
            CallContext->LWOrbit,
            StartTime,
            GetCycleCount());
        responseMsg.MutableHeaders()->ClearTrace();
        responseMsg.ClearDeprecatedTrace();

        ProcessThrottleTime(CallContext, responseMsg);

        Response.SetValue(std::move(responseMsg));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPingMethod
{
    static constexpr TBlockStoreProtocol::EMessageType ResponseType =
        TBlockStoreProtocol::EMessageType::PingResponse;

    using TRequest = NProto::TPingRequest;
    using TResponse = NProto::TPingResponse;
};

struct TMountVolumeMethod
{
    static constexpr TBlockStoreProtocol::EMessageType ResponseType =
        TBlockStoreProtocol::EMessageType::MountVolumeResponse;

    using TRequest = NProto::TMountVolumeRequest;
    using TResponse = NProto::TMountVolumeResponse;
};

struct TUnmountVolumeMethod
{
    static constexpr TBlockStoreProtocol::EMessageType ResponseType =
        TBlockStoreProtocol::EMessageType::UnmountVolumeResponse;

    using TRequest = NProto::TUnmountVolumeRequest;
    using TResponse = NProto::TUnmountVolumeResponse;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TProtoMessageHandler final
    : public IRequestHandler
{
public:
    using TRequest =  TMethod::TRequest;
    using TResponse = TMethod::TResponse;

private:
    const TCallContextPtr CallContext;
    const std::shared_ptr<TRequest> Request;
    const ITraceSerializerPtr TraceSerializer;

    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

public:
    TProtoMessageHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            ITraceSerializerPtr traceSerializer,
            bool isAlignedDataEnabled)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , TraceSerializer(std::move(traceSerializer))
    {
        Y_UNUSED(isAlignedDataEnabled);
    }

    size_t GetRequestSize() const
    {
        return NRdma::TProtoMessageSerializer::MessageByteSize(*Request, 0);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE;
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    TPrepareResult PrepareRequest(TStringBuf buffer)
    {
        ui32 flags = 0;

        return NRdma::TProtoMessageSerializer::Serialize(
            buffer,
            TBlockStoreProtocol::PingRequest,
            flags,   // flags
            *Request);
    }

    void HandleResponse(TStringBuf buffer) override
    {
        auto resultOrError = Serializer->Parse(buffer);
        if (HasError(resultOrError)) {
            Response.SetValue(TErrorResponse(resultOrError.GetError()));
            return;
        }

        const auto& response = resultOrError.GetResult();
        Y_ENSURE(response.MsgId == TMethod::ResponseType);
        Y_ENSURE(response.Data.length() == 0);

        auto& responseMsg = static_cast<TResponse&>(*response.Proto);

        Response.SetValue(std::move(responseMsg));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }
};


////////////////////////////////////////////////////////////////////////////////

class TRdmaDataEndpoint
    : public TEndpointBase
    , public NRdma::IClientHandler
    , public std::enable_shared_from_this<TRdmaDataEndpoint>
{
    const ITraceSerializerPtr TraceSerializer;
    const ITaskQueuePtr TaskQueue;
    const bool IsAlignedDataEnabled;

    NRdma::IClientEndpointPtr Endpoint;
    TLog Log;

public:
    TRdmaDataEndpoint(
            ILoggingServicePtr logging,
            ITraceSerializerPtr traceSerializer,
            ITaskQueuePtr taskQueue,
            bool isAlignedDataEnabled)
        : TraceSerializer(std::move(traceSerializer))
        , TaskQueue(std::move(taskQueue))
        , IsAlignedDataEnabled(isAlignedDataEnabled)
    {
        Log = logging->CreateLog("BLOCKSTORE_RDMA");
    }

    ~TRdmaDataEndpoint() override
    {
        DoStopEndpoint();
    }

    void Init(NRdma::IClientEndpointPtr endpoint)
    {
        Endpoint = std::move(endpoint);
    }

    void Start() override;
    void Stop() override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        using THandler = TProtoMessageHandler<TMountVolumeMethod>;
        return HandleRequest<THandler>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override
    {
        using THandler = TProtoMessageHandler<TUnmountVolumeMethod>;
        return HandleRequest<THandler>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TPingResponse> Ping(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TPingRequest> request) override
    {
        using THandler =
            TProtoMessageHandler<TPingMethod>;
        return HandleRequest<THandler>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return HandleRequest<TReadBlocksHandler>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return HandleRequest<TWriteBlocksHandler>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return HandleRequest<TZeroBlocksHandler>(
            std::move(callContext),
            std::move(request));
    }

private:
    template <typename T>
    TFuture<typename T::TResponse> HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request);

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override;

    void DoStopEndpoint();
};

////////////////////////////////////////////////////////////////////////////////

class TRdmaEndpoint final
    : public TRdmaDataEndpoint
{
private:
    const IBlockStorePtr VolumeClient;

public:
    static std::shared_ptr<TRdmaEndpoint> Create(
        ILoggingServicePtr logging,
        IBlockStorePtr volumeClient,
        ITraceSerializerPtr traceSerializer,
        ITaskQueuePtr taskQueue,
        bool isAlignedDataEnabled)
    {
        return std::shared_ptr<TRdmaEndpoint>{
            new TRdmaEndpoint(
                std::move(logging),
                std::move(volumeClient),
                std::move(traceSerializer),
                std::move(taskQueue),
                isAlignedDataEnabled)};
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override;

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override;

private:
    TRdmaEndpoint(
            ILoggingServicePtr logging,
            IBlockStorePtr volumeClient,
            ITraceSerializerPtr traceSerializer,
            ITaskQueuePtr taskQueue,
            bool isAlignedDataEnabled)
        : TRdmaDataEndpoint(
            std::move(logging),
            std::move(traceSerializer),
            std::move(taskQueue),
            isAlignedDataEnabled)
        , VolumeClient(std::move(volumeClient))
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

void TRdmaDataEndpoint::Start()
{
    // TODO
}

void TRdmaDataEndpoint::Stop()
{
    DoStopEndpoint();
}

TStorageBuffer TRdmaDataEndpoint::AllocateBuffer(size_t bytesCount)
{
    Y_UNUSED(bytesCount);
    return nullptr;
}

template <typename T>
TFuture<typename T::TResponse> TRdmaDataEndpoint::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<typename T::TRequest> request)
{
    auto handler = std::make_unique<T>(
        callContext,
        std::move(request),
        TraceSerializer,
        IsAlignedDataEnabled);

    auto [req, err] = Endpoint->AllocateRequest(
        shared_from_this(),
        nullptr,
        handler->GetRequestSize(),
        handler->GetResponseSize());

    if (HasError(err)) {
        return MakeFuture<typename T::TResponse>(TErrorResponse(err));
    }

    auto result = handler->PrepareRequest(req->RequestBuffer);
    if (HasError(result.GetError())) {
        return MakeFuture<typename T::TResponse>(
            TErrorResponse(std::move(result.GetError())));
    }

    auto response = handler->GetResponse();
    req->Context = std::move(handler);
    Endpoint->SendRequest(std::move(req), std::move(callContext));

    return response;
}

void TRdmaDataEndpoint::HandleResponse(
    NRdma::TClientRequestPtr req,
    ui32 status,
    size_t responseBytes)
{
    auto* handler = static_cast<IRequestHandler*>(req->Context.get());
    try {
        auto buffer = req->ResponseBuffer.Head(responseBytes);
        if (status == 0) {
            handler->HandleResponse(buffer);
        } else {
            auto error = NRdma::ParseError(buffer);
            handler->HandleError(error.GetCode(), error.GetMessage());
        }
    } catch (...) {
        STORAGE_ERROR("Exception in callback: " << CurrentExceptionMessage());
    }
}

void TRdmaDataEndpoint::DoStopEndpoint()
{
    if (Endpoint) {
        Endpoint->Stop().Wait();
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TMountVolumeResponse> TRdmaEndpoint::MountVolume(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TMountVolumeRequest> request)
{
    // TODO
    return VolumeClient->MountVolume(
        std::move(callContext),
        std::move(request));
}

TFuture<NProto::TUnmountVolumeResponse> TRdmaEndpoint::UnmountVolume(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TUnmountVolumeRequest> request)
{
    // TODO
    return VolumeClient->UnmountVolume(
        std::move(callContext),
        std::move(request));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateRdmaEndpointClient(
    ILoggingServicePtr logging,
    NRdma::IClientPtr client,
    IBlockStorePtr volumeClient,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config)
{
    auto endpoint = TRdmaEndpoint::Create(
        std::move(logging),
        std::move(volumeClient),
        std::move(traceSerializer),
        std::move(taskQueue),
        client->IsAlignedDataEnabled());

    auto startEndpoint = client->StartEndpoint(config.Address, config.Port);

    endpoint->Init(startEndpoint.GetValue(WAIT_TIMEOUT));
    return endpoint;
}

NThreading::TFuture<TResultOrError<IBlockStorePtr>>
CreateRdmaEndpointClientAsync(
    ILoggingServicePtr logging,
    NRdma::IClientPtr client,
    IBlockStorePtr volumeClient,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config)
{
    auto endpoint = TRdmaEndpoint::Create(
        std::move(logging),
        std::move(volumeClient),
        std::move(traceSerializer),
        std::move(taskQueue),
        client->IsAlignedDataEnabled());

    auto future = client->StartEndpoint(config.Address, config.Port);
    return future.Apply([endpoint = std::move(endpoint)] (const auto& future) mutable {
        auto result = SafeExecute<TResultOrError<IBlockStorePtr>>(
            [&] {
                endpoint->Init(future.GetValue());
                return TResultOrError<IBlockStorePtr>(endpoint);
            });
        return result;
    });
}

IBlockStorePtr CreateRdmaDataEndpoint(
    ILoggingServicePtr logging,
    NRdma::IClientPtr client,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config)
{
    auto endpoint = std::make_shared<TRdmaDataEndpoint>(
        std::move(logging),
        std::move(traceSerializer),
        std::move(taskQueue),
        client->IsAlignedDataEnabled());

    auto startEndpoint = client->StartEndpoint(config.Address, config.Port);

    endpoint->Init(startEndpoint.GetValue(WAIT_TIMEOUT));
    return endpoint;
}

NThreading::TFuture<TResultOrError<IBlockStorePtr>> CreateRdmaDataEndpointAsync(
    ILoggingServicePtr logging,
    NRdma::IClientPtr client,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config)
{
    auto endpoint = std::make_shared<TRdmaDataEndpoint>(
        std::move(logging),
        std::move(traceSerializer),
        std::move(taskQueue),
        client->IsAlignedDataEnabled());

    auto future = client->StartEndpoint(config.Address, config.Port);
    return future.Apply([endpoint = std::move(endpoint)] (const auto& future) mutable {
        auto result = SafeExecute<TResultOrError<IBlockStorePtr>>(
            [&] {
                endpoint->Init(future.GetValue());
                return TResultOrError<IBlockStorePtr>(endpoint);
            });
        return result;
    });
}

}   // namespace NCloud::NBlockStore::NClient
