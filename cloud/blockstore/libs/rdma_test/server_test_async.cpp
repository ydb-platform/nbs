#include "server_test_async.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/sglist.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/string/printf.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString MakeKey(const TString& host, ui32 port)
{
    return Sprintf("%s:%u", host.c_str(), port);
}

////////////////////////////////////////////////////////////////////////////////

class IRequestWrapper
{
public:
    virtual ~IRequestWrapper() = default;
    virtual void SendResponse(size_t responseBytes) = 0;
    virtual void SendError(ui32 error, TStringBuf message) = 0;
};

template <typename TRequestResponse>
class TRequestWrapper: public IRequestWrapper
{
public:
    TRequestWrapper()
        : Promise(NThreading::NewPromise<TRequestResponse>())
    {}

    void SendResponse(size_t responseBytes) override
    {
        UNIT_ASSERT(OutBuffer.size() >= responseBytes);

        auto parsedResponse =
            TBlockStoreProtocol::Serializer()->Parse(OutBuffer).ExtractResult();

        auto* concreteResponse =
            static_cast<TRequestResponse*>(parsedResponse.Proto.get());

        constexpr bool hasOutputBuffers =
            requires { concreteResponse->MutableBlocks(); };
        if constexpr (hasOutputBuffers) {
            UNIT_ASSERT(4_KB * BlocksCount <= parsedResponse.Data.size());
            for (ui32 i = 0; i < BlocksCount; ++i) {
                *concreteResponse->MutableBlocks()->AddBuffers() =
                    TString(parsedResponse.Data.substr(4_KB * i, 4_KB));
            }
        }

        Promise.SetValue(*concreteResponse);
    }

    void SendError(ui32 error, TStringBuf message) override
    {
        TRequestResponse errorResponse;
        errorResponse.MutableError()->SetCode(error);
        errorResponse.MutableError()->SetMessage(TString(message));
        Promise.SetValue(std::move(errorResponse));
    }

    TString Serialized;
    TString OutBuffer;
    ui32 BlocksCount = 0;
    NThreading::TPromise<TRequestResponse> Promise;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

class TRdmaAsyncTestEndpoint: public NRdma::IServerEndpoint
{
private:
    NRdma::IServerHandlerPtr Handler;

    const NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

public:
    TRdmaAsyncTestEndpoint(NRdma::IServerHandlerPtr handler)
        : Handler(std::move(handler))
    {}

    void SendResponse(void* context, size_t responseBytes) override
    {
        std::unique_ptr<IRequestWrapper> wrapper(
            static_cast<IRequestWrapper*>(context));
        wrapper->SendResponse(responseBytes);
    }

    void SendError(void* context, ui32 error, TStringBuf message) override
    {
        std::unique_ptr<IRequestWrapper> wrapper(
            static_cast<IRequestWrapper*>(context));
        wrapper->SendError(error, message);
    }

    template <typename TResponse, typename TRequest>
    NThreading::TFuture<TResponse> Run(
        TBlockStoreProtocol::EMessageType messageType,
        TRequest request)
    {
        auto wrapper = std::make_unique<TRequestWrapper<TResponse>>();

        // Serialize request
        TSgList sglist;
        NBlockStore::NProto::TIOVector tempBlocks;
        constexpr bool hasMutableBlocks = requires { request.MutableBlocks(); };
        if constexpr (hasMutableBlocks) {
            tempBlocks.Swap(request.MutableBlocks());
            request.ClearBlocks();

            for (const auto& b: tempBlocks.GetBuffers()) {
                sglist.push_back({b.data(), b.size()});
            }
        }

        const size_t expectedSerializedSize =
            NRdma::TProtoMessageSerializer::MessageByteSize(
                request,
                SgListGetSize(sglist));
        wrapper->Serialized = TString(expectedSerializedSize, 0);
        const size_t serializedSize =
            NRdma::TProtoMessageSerializer::SerializeWithData(
                wrapper->Serialized,
                messageType,
                0,   // flags
                request,
                sglist);
        UNIT_ASSERT(expectedSerializedSize >= serializedSize);

        // Prepare buffer for response message
        size_t expectedOutputSize = 4_KB;
        constexpr bool isReadRequest =
            std::is_same_v<TRequest, NProto::TReadDeviceBlocksRequest>;
        if constexpr (isReadRequest) {
            expectedOutputSize +=
                CalculateBytesCount(request, request.GetBlockSize());
        }
        wrapper->OutBuffer = TString(expectedOutputSize, 0);

        // Set blocks count
        constexpr bool hasBlocksCount = requires { request.GetBlocksCount(); };
        if constexpr (hasBlocksCount) {
            wrapper->BlocksCount = request.GetBlocksCount();
        }

        // Send request
        auto rawWrapper = wrapper.get();
        auto result = wrapper->Promise.GetFuture();
        Handler->HandleRequest(
            wrapper.release(),
            MakeIntrusive<TCallContext>(),
            rawWrapper->Serialized,
            rawWrapper->OutBuffer);

        return std::move(result);
    }
};

////////////////////////////////////////////////////////////////////////////////

NRdma::IServerEndpointPtr TRdmaAsyncTestServer::StartEndpoint(
    TString host,
    ui32 port,
    NRdma::IServerHandlerPtr handler)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    if (!ep) {
        ep = std::make_shared<TRdmaAsyncTestEndpoint>(handler);
    }
    return ep;
}

NRdma::IServerEndpointPtr TRdmaAsyncTestServer::StartEndpointOnInterface(
    TString interface,
    ui32 port,
    NRdma::IServerHandlerPtr handler)
{
    auto& ep = Endpoints[MakeKey(interface, port)];
    if (!ep) {
        ep = std::make_shared<TRdmaAsyncTestEndpoint>(handler);
    }
    return ep;
}

NThreading::TFuture<NProto::TWriteDeviceBlocksResponse> TRdmaAsyncTestServer::
    Run(TString host, ui32 port, NProto::TWriteDeviceBlocksRequest request)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    UNIT_ASSERT(ep.get());
    return ep->Run<NProto::TWriteDeviceBlocksResponse>(
        TBlockStoreProtocol::WriteDeviceBlocksRequest,
        std::move(request));
}

NThreading::TFuture<NProto::TReadDeviceBlocksResponse> TRdmaAsyncTestServer::
    Run(TString host, ui32 port, NProto::TReadDeviceBlocksRequest request)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    UNIT_ASSERT(ep.get());
    return ep->Run<NProto::TReadDeviceBlocksResponse>(
        TBlockStoreProtocol::ReadDeviceBlocksRequest,
        std::move(request));
}

NThreading::TFuture<NProto::TChecksumDeviceBlocksResponse>
TRdmaAsyncTestServer::Run(
    TString host,
    ui32 port,
    NProto::TChecksumDeviceBlocksRequest request)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    UNIT_ASSERT(ep.get());
    return ep->Run<NProto::TChecksumDeviceBlocksResponse>(
        TBlockStoreProtocol::ChecksumDeviceBlocksRequest,
        std::move(request));
}

NThreading::TFuture<NProto::TZeroDeviceBlocksResponse> TRdmaAsyncTestServer::
    Run(TString host, ui32 port, NProto::TZeroDeviceBlocksRequest request)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    UNIT_ASSERT(ep.get());
    return ep->Run<NProto::TZeroDeviceBlocksResponse>(
        TBlockStoreProtocol::ZeroDeviceBlocksRequest,
        std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
