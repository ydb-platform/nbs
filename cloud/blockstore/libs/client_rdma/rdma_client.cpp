#include "rdma_client.h"

#include "protocol.h"

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

///////////////////////////////////////////////////////////////////////////////

constexpr TDuration WAIT_TIMEOUT = TDuration::Seconds(10);

constexpr size_t MAX_PROTO_SIZE = 4*1024;

///////////////////////////////////////////////////////////////////////////////

class TEndpointBase
    : public IBlockStore
{
public:
    TEndpointBase() = default;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        const auto& name = GetBlockStoreRequestName(EBlockStoreRequest::name); \
        return MakeFuture<NProto::T##name##Response>(                          \
            TErrorResponse(E_NOT_IMPLEMENTED, TStringBuilder()                 \
                << "Unsupported request " << name.Quote()));                   \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

struct IRequestHandler: public NRdma::TNullContext
{
    virtual void HandleResponse(TStringBuf buffer) = 0;
    virtual void HandleError(ui32 error, TStringBuf message) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksHandler final
    : public IRequestHandler
{
public:
    using TRequest = NProto::TReadBlocksLocalRequest;
    using TResponse = NProto::TReadBlocksLocalResponse;

private:
    const TCallContextPtr CallContext;
    const std::shared_ptr<TRequest> Request;
    const bool IsAlignedDataEnabled;
    const size_t BlockSize;

    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer = TBlockStoreProtocol::Serializer();

public:
    TReadBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            bool isAlignedDataEnabled,
            size_t blockSize)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , IsAlignedDataEnabled(isAlignedDataEnabled)
        , BlockSize(blockSize)
    {
        Y_UNUSED(isAlignedDataEnabled);
    }

    size_t GetRequestSize() const
    {
        return NRdma::TProtoMessageSerializer::MessageByteSize(*Request, 0);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE + BlockSize * Request->GetBlocksCount();
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    size_t PrepareRequest(TStringBuf buffer)
    {
        ui32 flags = 0;
        if (IsAlignedDataEnabled) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

        Request->SetBlockSize(BlockSize);
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

        auto& responseMsg = static_cast<TResponse&>(*response.Proto);

        if (!HasError(responseMsg.GetError())) {
            CopyData(Request->Sglist, response.Data);
        }

        Response.SetValue(std::move(responseMsg));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }

private:
    static void CopyData(TGuardedSgList& guardedSgList, TStringBuf data)
    {
        auto guard = guardedSgList.Acquire();
        Y_ENSURE(guard);

        const char* ptr = data.data();
        size_t bytesLeft = data.length();

        for (auto buffer: guard.Get()) {
            size_t len = Min(bytesLeft, buffer.Size());
            Y_ENSURE(len);

            memcpy((char*)buffer.Data(), ptr, len);
            ptr += len;
            bytesLeft -= len;
        }

        Y_ENSURE(bytesLeft == 0);
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
    const std::shared_ptr<TRequest> Request;
    const bool IsAlignedDataEnabled;
    const size_t BlockSize;

    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer = TBlockStoreProtocol::Serializer();

public:
    TWriteBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            bool IsAlignedDataEnabled,
            size_t blockSize)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , IsAlignedDataEnabled(IsAlignedDataEnabled)
        , BlockSize(blockSize)
    {}

    size_t GetRequestSize() const
    {
        return NRdma::TProtoMessageSerializer::MessageByteSize(
            *Request,
            BlockSize * Request->BlocksCount);
    }

    size_t GetResponseSize() const
    {
        return MAX_PROTO_SIZE;
    }

    TFuture<TResponse> GetResponse() const
    {
        return Response.GetFuture();
    }

    size_t PrepareRequest(TStringBuf buffer)
    {
        auto guard = Request->Sglist.Acquire();
        Y_ENSURE(guard);

        const auto& sglist = guard.Get();
        Request->SetBlockSize(BlockSize);

        ui32 flags = 0;
        if (IsAlignedDataEnabled) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

        return NRdma::TProtoMessageSerializer::SerializeWithData(
            buffer,
            TBlockStoreProtocol::WriteBlocksRequest,
            flags,
            *Request,
            sglist);
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
    const bool IsAlignedDataEnabled;

    TPromise<TResponse> Response = NewPromise<TResponse>();
    NRdma::TProtoMessageSerializer* Serializer = TBlockStoreProtocol::Serializer();

public:
    TZeroBlocksHandler(
            TCallContextPtr callContext,
            std::shared_ptr<TRequest> request,
            bool isAlignedDataEnabled,
            size_t blockSize)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , IsAlignedDataEnabled(isAlignedDataEnabled)
    {
        Y_UNUSED(blockSize);
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

    size_t PrepareRequest(TStringBuf buffer)
    {
        ui32 flags = 0;
        if (IsAlignedDataEnabled) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

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
        Response.SetValue(std::move(responseMsg));
    }

    void HandleError(ui32 error, TStringBuf message) override
    {
        Response.SetValue(TErrorResponse(error, TString(message)));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRdmaEndpoint final
    : public TEndpointBase
    , public NRdma::IClientHandler
    , public std::enable_shared_from_this<TRdmaEndpoint>
{
private:
    const IBlockStorePtr VolumeClient;
    const bool IsAlignedDataEnabled;

    NRdma::IClientEndpointPtr Endpoint;
    TLog Log;

    // TODO
    size_t BlockSize = 4*1024;

public:
    ~TRdmaEndpoint() override
    {
        Stop();
    }

    static std::shared_ptr<TRdmaEndpoint> Create(
        ILoggingServicePtr logging,
        IBlockStorePtr volumeClient,
        bool IsAlignedDataEnabled)
    {
        return std::shared_ptr<TRdmaEndpoint>{
            new TRdmaEndpoint(
                std::move(logging),
                std::move(volumeClient),
                IsAlignedDataEnabled)};
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
        std::shared_ptr<NProto::TMountVolumeRequest> request) override;

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override;

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
    TRdmaEndpoint(
            ILoggingServicePtr logging,
            IBlockStorePtr volumeClient,
            bool IsAlignedDataEnabled)
        : VolumeClient(std::move(volumeClient))
        , IsAlignedDataEnabled(IsAlignedDataEnabled)
    {
        Log = logging->CreateLog("BLOCKSTORE_RDMA");
    }

    template <typename T>
    TFuture<typename T::TResponse> HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request);

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override;
};

////////////////////////////////////////////////////////////////////////////////

void TRdmaEndpoint::Start()
{
    // TODO
}

void TRdmaEndpoint::Stop()
{
    // TODO
}

TStorageBuffer TRdmaEndpoint::AllocateBuffer(size_t bytesCount)
{
    Y_UNUSED(bytesCount);
    return nullptr;
}

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

template <typename T>
TFuture<typename T::TResponse> TRdmaEndpoint::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<typename T::TRequest> request)
{
    auto handler = std::make_unique<T>(
        callContext,
        std::move(request),
        IsAlignedDataEnabled,
        BlockSize);

    auto [req, err] = Endpoint->AllocateRequest(
        shared_from_this(),
        nullptr,
        handler->GetRequestSize(),
        handler->GetResponseSize());

    if (HasError(err)) {
        return MakeFuture<typename T::TResponse>(TErrorResponse(err));
    }

    handler->PrepareRequest(req->RequestBuffer);
    auto response = handler->GetResponse();
    req->Context = std::move(handler);
    Endpoint->SendRequest(std::move(req), std::move(callContext));

    return response;
}

void TRdmaEndpoint::HandleResponse(
    NRdma::TClientRequestPtr req,
    ui32 status,
    size_t responseBytes)
{
    // TODO: it is much better to process response in different thread

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateRdmaEndpointClient(
    ILoggingServicePtr logging,
    NRdma::IClientPtr client,
    IBlockStorePtr volumeClient,
    const TRdmaEndpointConfig& config)
{
    auto endpoint =
        TRdmaEndpoint::Create(
            std::move(logging),
            std::move(volumeClient),
            client->IsAlignedDataEnabled());

    auto startEndpoint = client->StartEndpoint(config.Address, config.Port);

    endpoint->Init(startEndpoint.GetValue(WAIT_TIMEOUT));
    return endpoint;
}

NThreading::TFuture<IBlockStorePtr> CreateRdmaEndpointClientAsync(
    ILoggingServicePtr logging,
    NRdma::IClientPtr client,
    IBlockStorePtr volumeClient,
    const TRdmaEndpointConfig& config)
{
    auto endpoint =
        TRdmaEndpoint::Create(
            std::move(logging),
            std::move(volumeClient),
            client->IsAlignedDataEnabled());

    auto promise = NewPromise<IBlockStorePtr>();

    auto future = client->StartEndpoint(config.Address, config.Port);
    future.Subscribe([endpoint = std::move(endpoint), promise = promise] (const auto& future) mutable {
        endpoint->Init(future.GetValue());
        promise.SetValue(endpoint);
    });
    return promise.GetFuture();
}

}   // namespace NCloud::NBlockStore::NClient
