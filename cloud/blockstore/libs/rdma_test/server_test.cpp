#include "server_test.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/sglist.h>
#include <cloud/blockstore/libs/rdma/protobuf.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/string/printf.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaEndpointImpl
    : NRdma::IServerEndpoint
{
    NRdma::IServerHandlerPtr Handler;

    size_t CurrentRequestId = 0;
    THashMap<void*, NThreading::TPromise<void>> Requests;

    NRdma::TProtoMessageSerializer* Serializer
        = TBlockStoreProtocol::Serializer();

    TRdmaEndpointImpl(NRdma::IServerHandlerPtr handler)
        : Handler(std::move(handler))
    {}

    template <typename TRequest>
    auto HandleRequest(TString& buffer, TString& out)
    {
        void* context = reinterpret_cast<void*>(++CurrentRequestId);
        auto& p = Requests[context] = NThreading::NewPromise();
        auto f = p.GetFuture();

        Handler->HandleRequest(
            context,
            MakeIntrusive<TCallContext>(),
            buffer,
            out);
        f.GetValueSync();

        return Serializer->Parse(out).ExtractResult();
    }

    template <typename TRequest, typename TResponse>
    TResponse HandleRequestAndExtractResponse(TString& buffer, size_t outSize)
    {
        TString out(outSize, 0);
        auto parsedResponse = HandleRequest<TRequest>(buffer, out).Proto;
        auto* concreteResponse = dynamic_cast<TResponse*>(parsedResponse.get());

        UNIT_ASSERT(concreteResponse);
        return *concreteResponse;
    }

    auto ProcessRequest(NProto::TChecksumDeviceBlocksRequest request)
    {
        TString buffer(4_KB, 0);

        Serializer->Serialize(
            buffer,
            TBlockStoreProtocol::ChecksumDeviceBlocksRequest,
            request,
            TContIOVector(nullptr, 0));

        return HandleRequestAndExtractResponse<
            NProto::TChecksumDeviceBlocksRequest,
            NProto::TChecksumDeviceBlocksResponse>(buffer, 4_KB);
    }

    auto ProcessRequest(NProto::TReadDeviceBlocksRequest request)
    {
        TString buffer(4_KB, 0);

        Serializer->Serialize(
            buffer,
            TBlockStoreProtocol::ReadDeviceBlocksRequest,
            request,
            TContIOVector(nullptr, 0));

        TString out(4_MB + 4_KB, 0);

        auto result = HandleRequest<NProto::TReadDeviceBlocksRequest>(
            buffer,
            out);

        using TResponse = NProto::TReadDeviceBlocksResponse;
        auto* concreteResponse = dynamic_cast<TResponse*>(result.Proto.get());

        UNIT_ASSERT(concreteResponse);

        for (ui32 i = 0; i < request.GetBlocksCount(); ++i) {
            *concreteResponse->MutableBlocks()->AddBuffers() =
                TString(result.Data.substr(4_KB * i, 4_KB));
        }

        return *concreteResponse;

    }

    auto ProcessRequest(NProto::TWriteDeviceBlocksRequest request)
    {
        TString dataBuffer(4_MB, 0);
        TVector<IOutputStream::TPart> parts;
        size_t offset = 0;
        for (auto& b: request.GetBlocks().GetBuffers()) {
            memcpy(dataBuffer.begin() + offset, b.data(), 4_KB);
            parts.push_back({dataBuffer.data() + offset, 4_KB});
            offset += 4_KB;
        }
        request.ClearBlocks();

        TString buffer(4_MB + 4_KB, 0);
        Serializer->Serialize(
            buffer,
            TBlockStoreProtocol::WriteDeviceBlocksRequest,
            request,
            TContIOVector(parts.data(), parts.size()));

        return HandleRequestAndExtractResponse<
            NProto::TWriteDeviceBlocksRequest,
            NProto::TWriteDeviceBlocksResponse>(buffer, 4_KB);
    }

    auto ProcessRequest(NProto::TZeroDeviceBlocksRequest request)
    {
        TString buffer(4_KB, 0);

        Serializer->Serialize(
            buffer,
            TBlockStoreProtocol::ZeroDeviceBlocksRequest,
            request,
            TContIOVector(nullptr, 0));

        return HandleRequestAndExtractResponse<
            NProto::TZeroDeviceBlocksRequest,
            NProto::TZeroDeviceBlocksResponse>(buffer, 4_KB);
    }

    void SendResponse(void* context, size_t responseBytes) override
    {
        Y_UNUSED(responseBytes);

        auto it = Requests.find(context);
        UNIT_ASSERT_C(it != Requests.end(), "request not found");
        auto p = it->second;
        Requests.erase(it);

        p.SetValue();
    }

    void SendError(void* context, ui32 error, TStringBuf message) override
    {
        Y_UNUSED(context);
        Y_UNUSED(error);

        UNIT_ASSERT_C(
            0,
            TStringBuilder() << "not implemented, errmsg: " << message);
    }
};

////////////////////////////////////////////////////////////////////////////////

TString MakeKey(const TString& host, ui32 port)
{
    return Sprintf("%s:%u", host.c_str(), port);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NRdma::IServerEndpointPtr TRdmaServerTest::StartEndpoint(
    TString host,
    ui32 port,
    NRdma::IServerHandlerPtr handler)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    if (!ep.Endpoint) {
        ep.Endpoint = std::make_shared<TRdmaEndpointImpl>(handler);
    }
    return ep.Endpoint;
}

NProto::TChecksumDeviceBlocksResponse TRdmaServerTest::ProcessRequest(
    TString host,
    ui32 port,
    NProto::TChecksumDeviceBlocksRequest request)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    UNIT_ASSERT(ep.Endpoint);
    auto& impl = static_cast<TRdmaEndpointImpl&>(*ep.Endpoint);
    return impl.ProcessRequest(std::move(request));
}

NProto::TReadDeviceBlocksResponse TRdmaServerTest::ProcessRequest(
    TString host,
    ui32 port,
    NProto::TReadDeviceBlocksRequest request)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    UNIT_ASSERT(ep.Endpoint);
    auto& impl = static_cast<TRdmaEndpointImpl&>(*ep.Endpoint);
    return impl.ProcessRequest(std::move(request));
}

NProto::TWriteDeviceBlocksResponse TRdmaServerTest::ProcessRequest(
    TString host,
    ui32 port,
    NProto::TWriteDeviceBlocksRequest request)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    UNIT_ASSERT(ep.Endpoint);
    auto& impl = static_cast<TRdmaEndpointImpl&>(*ep.Endpoint);
    return impl.ProcessRequest(std::move(request));
}

NProto::TZeroDeviceBlocksResponse TRdmaServerTest::ProcessRequest(
    TString host,
    ui32 port,
    NProto::TZeroDeviceBlocksRequest request)
{
    auto& ep = Endpoints[MakeKey(host, port)];
    UNIT_ASSERT(ep.Endpoint);
    auto& impl = static_cast<TRdmaEndpointImpl&>(*ep.Endpoint);
    return impl.ProcessRequest(std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
