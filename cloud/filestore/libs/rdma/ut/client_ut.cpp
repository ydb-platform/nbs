#include <cloud/filestore/libs/rdma/client.h>
#include <cloud/filestore/libs/rdma/endpoint.h>
#include <cloud/filestore/libs/rdma/protocol.h>
#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/rdma_test/test_server.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

using namespace NCloud::NStorage::NRdma;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestClientRequest final
    : public TClientRequest
{
    TString In;
    TString Out;

    TTestClientRequest(
            IClientRequestHandlerPtr handler,
            std::unique_ptr<TNullContext> context,
            size_t requestBytes,
            size_t responseBytes)
        : TClientRequest(std::move(handler), std::move(context))
        , In(requestBytes, 0)
        , Out(responseBytes, 0)
    {
        RequestBuffer = In;
        ResponseBuffer = Out;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestClientEndpoint final
    : public IClientEndpoint
{
    NProto::TError AllocationError;

    TResultOrError<TClientRequestPtr> AllocateRequest(
        IClientRequestHandlerPtr handler,
        std::unique_ptr<TNullContext> context,
        size_t requestBytes,
        size_t responseBytes) override
    {
        if (HasError(AllocationError)) {
            return AllocationError;
        }

        return TClientRequestPtr(std::make_unique<TTestClientRequest>(
            std::move(handler),
            std::move(context),
            requestBytes,
            responseBytes));
    }

    ui64 SendRequest(
        TClientRequestPtr req,
        TCallContextBasePtr callContext) override
    {
        Y_UNUSED(req);
        Y_UNUSED(callContext);
        return 0;
    }

    void CancelRequest(ui64 clientRequestId) override
    {
        Y_UNUSED(clientRequestId);
    }

    TFuture<void> Stop() override
    {
        return MakeFuture();
    }

    void TryForceReconnect() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TTestResponseHandler final
    : public IClientRequestHandler
{
    void HandleResponse(
        TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        Y_UNUSED(req);
        Y_UNUSED(status);
        Y_UNUSED(responseBytes);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestRequestHandler final
    : public IRdmaTabletRequestHandler
{
    NProtoPrivate::TTabletRdmaRequest Envelope;
    TString Body;

    TPromise<TTabletTransportResponse> Response =
        NewPromise<TTabletTransportResponse>();

    TFuture<TTabletTransportResponse> HandleRequest(
        const NProtoPrivate::TTabletRdmaRequest& envelope,
        TStringBuf requestBody,
        TCallContextBasePtr callContext) override
    {
        Y_UNUSED(callContext);

        Envelope = envelope;
        Body = TString(requestBody);
        return Response.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeRecord(TString message)
{
    return MakeError(E_IO, std::move(message));
}

NProtoPrivate::TTabletRdmaRequest MakeEnvelope(ui32 methodId)
{
    NProtoPrivate::TTabletRdmaRequest envelope;
    envelope.SetMethodId(methodId);
    envelope.MutablePipeServerId()->SetRawX1(7);
    envelope.MutablePipeServerId()->SetRawX2(8);
    return envelope;
}

TClientRequestPtr Allocate(
    IClientEndpoint& endpoint,
    ui32 methodId,
    const NProto::TError& record,
    size_t maxResponseBytes)
{
    auto [request, error] = AllocateRdmaTabletRequest(
        endpoint,
        std::make_shared<TTestResponseHandler>(),
        MakeEnvelope(methodId),
        record,
        maxResponseBytes);
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    UNIT_ASSERT(request);
    return std::move(request);
}

size_t WriteResponse(
    TStringBuf out,
    ui32 methodId,
    const NProto::TError& record)
{
    NProtoPrivate::TTabletRdmaResponse envelope;
    envelope.SetMethodId(methodId);

    const TString body = record.SerializeAsString();
    const TBlockDataRef dataRef(body.data(), body.size());

    return TProtoMessageSerializer::SerializeWithData(
        out,
        TTabletRdmaProtocol::TabletResponse,
        0,   // flags
        envelope,
        TBlockDataRefSpan(&dataRef, 1));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaTabletClientTest)
{
    Y_UNIT_TEST(ShouldEncodeRequest)
    {
        TTestClientEndpoint endpoint;
        auto handler = std::make_shared<TTestResponseHandler>();

        auto [request, error] = AllocateRdmaTabletRequest(
            endpoint,
            handler,
            MakeEnvelope(100),
            MakeRecord("request"),
            1_KB);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_EQUAL(handler, request->Handler);
        UNIT_ASSERT(!request->Context);

        auto result =
            TTabletRdmaProtocol::Serializer()->Parse(request->RequestBuffer);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
        auto parsed = result.ExtractResult();

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TTabletRdmaProtocol::TabletRequest),
            parsed.MsgId);
        const auto* envelope =
            static_cast<const NProtoPrivate::TTabletRdmaRequest*>(
                parsed.Proto.get());
        UNIT_ASSERT_VALUES_EQUAL(100, envelope->GetMethodId());
        UNIT_ASSERT_VALUES_EQUAL(7, envelope->GetPipeServerId().GetRawX1());
        UNIT_ASSERT_VALUES_EQUAL(8, envelope->GetPipeServerId().GetRawX2());

        NProto::TError record;
        UNIT_ASSERT(
            record.ParseFromArray(parsed.Data.data(), parsed.Data.size()));
        UNIT_ASSERT_VALUES_EQUAL("request", record.GetMessage());
    }

    Y_UNIT_TEST(ShouldParseTypedResponse)
    {
        TTestClientEndpoint endpoint;
        auto request = Allocate(endpoint, 100, MakeRecord("request"), 1_KB);

        const size_t responseBytes = WriteResponse(
            request->ResponseBuffer,
            101,
            MakeRecord("response"));

        NProto::TError record;
        auto [methodId, error] = ParseRdmaTabletResponse(
            *request,
            RDMA_PROTO_OK,
            responseBytes,
            &record);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(101, methodId);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_IO), record.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("response", record.GetMessage());
    }

    Y_UNIT_TEST(ShouldExchangeRequestsWithServerEndpoint)
    {
        TTestClientEndpoint endpoint;

        ui32 status = RDMA_PROTO_OK;
        size_t responseBytes = 0;
        TClientRequestPtr request;

        auto server = std::make_shared<TTestRdmaServer>(
            [&](void* context, size_t bytes)
            {
                UNIT_ASSERT_EQUAL(context, request.get());
                status = RDMA_PROTO_OK;
                responseBytes = bytes;
            },
            [&](void* context, ui32 error, TStringBuf message)
            {
                UNIT_ASSERT_EQUAL(context, request.get());
                status = RDMA_PROTO_FAIL;
                responseBytes =
                    SerializeError(error, message, request->ResponseBuffer);
            });

        auto exchange = [&](
            TTabletTransportResponse serverResponse,
            NProto::TError& record)
        {
            auto requestHandler = std::make_shared<TTestRequestHandler>();
            requestHandler->Response.SetValue(std::move(serverResponse));

            StartRdmaTabletEndpoint(
                *server,
                "::1",
                10021,
                CreateLoggingService("console")->CreateLog("NFS_RDMA"),
                requestHandler);

            request = Allocate(endpoint, 100, MakeRecord("request"), 1_KB);

            auto handler = server->GetHandler();
            handler->HandleRequest(
                request.get(),
                handler->CreateCallContext(),
                request->RequestBuffer,
                request->ResponseBuffer);

            UNIT_ASSERT_VALUES_EQUAL(
                100,
                requestHandler->Envelope.GetMethodId());
            UNIT_ASSERT_VALUES_EQUAL(
                7,
                requestHandler->Envelope.GetPipeServerId().GetRawX1());
            UNIT_ASSERT_VALUES_EQUAL(
                8,
                requestHandler->Envelope.GetPipeServerId().GetRawX2());

            NProto::TError requestRecord;
            UNIT_ASSERT(requestRecord.ParseFromString(requestHandler->Body));
            UNIT_ASSERT_VALUES_EQUAL("request", requestRecord.GetMessage());

            return ParseRdmaTabletResponse(
                *request,
                status,
                responseBytes,
                &record);
        };

        {
            NProto::TError record;
            auto [methodId, error] = exchange(
                TTabletTransportResponse(
                    101,
                    std::make_unique<NProto::TError>(MakeRecord("response"))),
                record);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(101, methodId);
            UNIT_ASSERT_VALUES_EQUAL("response", record.GetMessage());
        }

        {
            NProto::TError record;
            auto [methodId, error] = exchange(
                TTabletTransportResponse(
                    MakeError(E_REJECTED, "unknown pipe server")),
                record);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(E_REJECTED),
                error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("unknown pipe server", error.GetMessage());
        }
    }

    Y_UNIT_TEST(ShouldReportTransportError)
    {
        TTestClientEndpoint endpoint;
        auto request = Allocate(endpoint, 100, MakeRecord("request"), 1_KB);

        const size_t responseBytes = SerializeError(
            E_TIMEOUT,
            "tablet request timed out",
            request->ResponseBuffer);

        NProto::TError record;
        auto [methodId, error] = ParseRdmaTabletResponse(
            *request,
            RDMA_PROTO_FAIL,
            responseBytes,
            &record);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_TIMEOUT),
            error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "tablet request timed out",
            error.GetMessage());
    }

    Y_UNIT_TEST(ShouldReportThrottling)
    {
        TTestClientEndpoint endpoint;
        auto request = Allocate(endpoint, 100, MakeRecord("request"), 1_KB);

        const size_t responseBytes =
            SerializeError(E_REJECTED, "throttled", request->ResponseBuffer);

        NProto::TError record;
        auto [methodId, error] = ParseRdmaTabletResponse(
            *request,
            RDMA_PROTO_THROTTLED,
            responseBytes,
            &record);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_FS_THROTTLED),
            error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("throttled", error.GetMessage());
    }

    Y_UNIT_TEST(ShouldReturnAllocationError)
    {
        TTestClientEndpoint endpoint;
        endpoint.AllocationError =
            MakeError(E_RDMA_UNAVAILABLE, "unable to allocate request");

        auto [request, error] = AllocateRdmaTabletRequest(
            endpoint,
            std::make_shared<TTestResponseHandler>(),
            MakeEnvelope(100),
            MakeRecord("request"),
            1_KB);
        UNIT_ASSERT(!request);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_RDMA_UNAVAILABLE),
            error.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectMalformedResponse)
    {
        TTestClientEndpoint endpoint;
        auto request = Allocate(endpoint, 100, MakeRecord("request"), 1_KB);

        NProto::TError record;
        auto [methodId, error] = ParseRdmaTabletResponse(
            *request,
            RDMA_PROTO_OK,
            RDMA_PROTO_HEADER_SIZE,
            &record);
        UNIT_ASSERT(HasError(error));
    }

    Y_UNIT_TEST(ShouldRejectUnexpectedMessageId)
    {
        TTestClientEndpoint endpoint;
        auto request = Allocate(endpoint, 100, MakeRecord("request"), 1_KB);

        const size_t responseBytes = TProtoMessageSerializer::Serialize(
            request->ResponseBuffer,
            TTabletRdmaProtocol::TabletRequest,
            0,   // flags
            MakeEnvelope(100));

        NProto::TError record;
        auto [methodId, error] = ParseRdmaTabletResponse(
            *request,
            RDMA_PROTO_OK,
            responseBytes,
            &record);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_NOT_IMPLEMENTED),
            error.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectMalformedResponseBody)
    {
        TTestClientEndpoint endpoint;
        auto request = Allocate(endpoint, 100, MakeRecord("request"), 1_KB);

        NProtoPrivate::TTabletRdmaResponse envelope;
        envelope.SetMethodId(101);

        // a length-delimited field cut short
        const TString body = "\x12\x7F";
        const TBlockDataRef dataRef(body.data(), body.size());
        const size_t responseBytes = TProtoMessageSerializer::SerializeWithData(
            request->ResponseBuffer,
            TTabletRdmaProtocol::TabletResponse,
            0,   // flags
            envelope,
            TBlockDataRefSpan(&dataRef, 1));

        NProto::TError record;
        auto [methodId, error] = ParseRdmaTabletResponse(
            *request,
            RDMA_PROTO_OK,
            responseBytes,
            &record);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_ARGUMENT),
            error.GetCode());
    }

    Y_UNIT_TEST(ShouldNotReadPastResponseBuffer)
    {
        TTestClientEndpoint endpoint;
        auto request = Allocate(endpoint, 100, MakeRecord("request"), 1_KB);

        const size_t bufferSize = request->ResponseBuffer.size();

        NProto::TError large;
        large.SetMessage(TString(bufferSize, 'a'));

        TString response(2 * bufferSize, 0);
        const size_t responseBytes = WriteResponse(response, 101, large);
        UNIT_ASSERT(responseBytes > bufferSize);

        memcpy(
            const_cast<char*>(request->ResponseBuffer.data()),
            response.data(),
            bufferSize);

        NProto::TError record;
        auto [methodId, error] = ParseRdmaTabletResponse(
            *request,
            RDMA_PROTO_OK,
            responseBytes,
            &record);
        UNIT_ASSERT_STRING_CONTAINS(
            error.GetMessage(),
            "invalid buffer length");
    }

    Y_UNIT_TEST(ShouldAllocateResponseBufferForLargestResponseRecord)
    {
        TTestClientEndpoint endpoint;

        NProto::TError largest;
        largest.SetMessage(TString(4_KB, 'a'));

        auto request = Allocate(
            endpoint,
            100,
            MakeRecord("request"),
            largest.ByteSizeLong());

        const size_t responseBytes =
            WriteResponse(request->ResponseBuffer, Max<ui32>(), largest);
        UNIT_ASSERT_VALUES_EQUAL(request->ResponseBuffer.size(), responseBytes);

        NProto::TError record;
        auto [methodId, error] = ParseRdmaTabletResponse(
            *request,
            RDMA_PROTO_OK,
            responseBytes,
            &record);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(largest.GetMessage(), record.GetMessage());
    }

    Y_UNIT_TEST(ShouldAllocateResponseBufferThatFitsErrorReply)
    {
        TTestClientEndpoint endpoint;
        auto request = Allocate(endpoint, 100, MakeRecord("request"), 0);

        UNIT_ASSERT_VALUES_EQUAL(200, request->ResponseBuffer.size());
    }
}

}   // namespace NCloud::NFileStore::NStorage
