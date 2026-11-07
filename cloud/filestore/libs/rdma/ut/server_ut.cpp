#include <cloud/filestore/libs/rdma/protocol.h>
#include <cloud/filestore/libs/rdma/server.h>
#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/rdma_test/test_server.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NCloud::NStorage::NRdma;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestRequestHandler final
    : public IRdmaTabletRequestHandler
{
    ui32 Calls = 0;
    ui64 TabletId = 0;
    ui32 MethodId = 0;
    TString Body;

    TPromise<TTabletTransportResponse> Response =
        NewPromise<TTabletTransportResponse>();

    TFuture<TTabletTransportResponse> HandleRequest(
        ui64 tabletId,
        ui32 methodId,
        TStringBuf requestBody,
        TCallContextBasePtr callContext) override
    {
        Y_UNUSED(callContext);

        ++Calls;
        TabletId = tabletId;
        MethodId = methodId;
        Body = TString(requestBody);
        return Response.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    std::shared_ptr<TTestRequestHandler> RequestHandler =
        std::make_shared<TTestRequestHandler>();

    size_t ResponseBytes = 0;
    bool Responded = false;
    ui32 ErrorCode = S_OK;
    TString ErrorMessage;
    bool Failed = false;

    std::shared_ptr<TTestRdmaServer> Server;
    IStartablePtr Target;

    TString In;
    TString Out;

    explicit TEnv(size_t outSize)
        : Out(outSize, 0)
    {
        Server = std::make_shared<TTestRdmaServer>(
            [this](void* context, size_t responseBytes)
            {
                Y_UNUSED(context);
                Responded = true;
                ResponseBytes = responseBytes;
            },
            [this](void* context, ui32 error, TStringBuf message)
            {
                Y_UNUSED(context);
                Failed = true;
                ErrorCode = error;
                ErrorMessage = TString(message);
            });

        auto config = std::make_shared<TFileStoreRdmaServerConfig>();
        config->ListenAddress = "::1";

        Target = CreateFileStoreRdmaServiceTarget(
            std::move(config),
            CreateLoggingService("console"),
            Server,
            RequestHandler);
        Target->Start();
    }

    void Send(TString in)
    {
        In = std::move(in);
        auto handler = Server->GetHandler();
        handler->HandleRequest(
            nullptr,
            handler->CreateCallContext(),
            In,
            Out);
    }

    TProtoMessageSerializer::TParseResult ParseResponse() const
    {
        UNIT_ASSERT(Responded);
        auto result = TTabletRdmaProtocol::Serializer()->Parse(
            TStringBuf(Out).Head(ResponseBytes));
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
        return result.ExtractResult();
    }
};

TString MakeRequest(ui64 tabletId, ui32 methodId, TStringBuf body)
{
    NProtoPrivate::TTabletRdmaRequest envelope;
    envelope.SetTabletId(tabletId);
    envelope.SetMethodId(methodId);

    TBlockDataRef dataRef;
    TBlockDataRefSpan data;
    if (body) {
        dataRef = TBlockDataRef(body.data(), body.size());
        data = TBlockDataRefSpan(&dataRef, 1);
    }

    TString buffer(
        TProtoMessageSerializer::MessageByteSize(envelope, body.size()),
        0);
    TProtoMessageSerializer::SerializeWithData(
        buffer,
        TTabletRdmaProtocol::TabletRequest,
        0,
        envelope,
        data);
    return buffer;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileStoreRdmaServiceTargetTest)
{
    Y_UNIT_TEST(ShouldListenOnConfiguredAddress)
    {
        TEnv env(4_KB);

        UNIT_ASSERT_VALUES_EQUAL("::1", env.Server->GetHost());
        UNIT_ASSERT_VALUES_EQUAL(10021, env.Server->GetPort());
    }

    Y_UNIT_TEST(ShouldPassDecodedRequestAndSerializeTypedResponse)
    {
        TEnv env(4_KB);

        env.Send(MakeRequest(42, 100, "request-body"));

        UNIT_ASSERT_VALUES_EQUAL(1, env.RequestHandler->Calls);
        UNIT_ASSERT_VALUES_EQUAL(42, env.RequestHandler->TabletId);
        UNIT_ASSERT_VALUES_EQUAL(100, env.RequestHandler->MethodId);
        UNIT_ASSERT_VALUES_EQUAL("request-body", env.RequestHandler->Body);
        UNIT_ASSERT(!env.Responded);

        auto record = std::make_shared<NProto::TError>();
        record->SetCode(E_IO);
        record->SetMessage("typed response");
        env.RequestHandler->Response.SetValue(
            TTabletTransportResponse(101, std::move(record)));

        UNIT_ASSERT(!env.Failed);
        auto parsed = env.ParseResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TTabletRdmaProtocol::TabletResponse),
            parsed.MsgId);
        const auto* envelope = static_cast<NProtoPrivate::TTabletRdmaResponse*>(
            parsed.Proto.get());
        UNIT_ASSERT_VALUES_EQUAL(101, envelope->GetMethodId());

        NProto::TError body;
        UNIT_ASSERT(
            body.ParseFromArray(parsed.Data.data(), parsed.Data.size()));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_IO), body.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("typed response", body.GetMessage());
    }

    Y_UNIT_TEST(ShouldSerializeStringResponse)
    {
        TEnv env(4_KB);

        env.Send(MakeRequest(1, 2, ""));
        env.RequestHandler->Response.SetValue(
            TTabletTransportResponse(3, TString("serialized-body")));

        auto parsed = env.ParseResponse();
        const auto* envelope = static_cast<NProtoPrivate::TTabletRdmaResponse*>(
            parsed.Proto.get());
        UNIT_ASSERT_VALUES_EQUAL(3, envelope->GetMethodId());
        UNIT_ASSERT_VALUES_EQUAL("serialized-body", TString(parsed.Data));
    }

    Y_UNIT_TEST(ShouldSerializeEmptyResponse)
    {
        TEnv env(4_KB);

        env.Send(MakeRequest(1, 2, ""));
        env.RequestHandler->Response.SetValue(
            TTabletTransportResponse(3, TString()));

        auto parsed = env.ParseResponse();
        UNIT_ASSERT(parsed.Data.empty());
    }

    Y_UNIT_TEST(ShouldReplyWithHandlerError)
    {
        TEnv env(4_KB);

        env.Send(MakeRequest(1, 2, "body"));
        env.RequestHandler->Response.SetValue(TTabletTransportResponse(
            MakeError(E_TIMEOUT, "tablet request timed out")));

        UNIT_ASSERT(!env.Responded);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_TIMEOUT), env.ErrorCode);
        UNIT_ASSERT_VALUES_EQUAL("tablet request timed out", env.ErrorMessage);
    }

    Y_UNIT_TEST(ShouldRejectMalformedRequest)
    {
        TEnv env(4_KB);

        env.Send(TString(RDMA_PROTO_HEADER_SIZE, 0));

        UNIT_ASSERT_VALUES_EQUAL(0, env.RequestHandler->Calls);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_REJECTED), env.ErrorCode);
    }

    Y_UNIT_TEST(ShouldRejectUnexpectedMessageId)
    {
        TEnv env(4_KB);

        NProtoPrivate::TTabletRdmaResponse envelope;
        envelope.SetMethodId(1);
        TString in(TProtoMessageSerializer::MessageByteSize(envelope, 0), 0);
        TProtoMessageSerializer::Serialize(
            in,
            TTabletRdmaProtocol::TabletResponse,
            0,
            envelope);

        env.Send(std::move(in));

        UNIT_ASSERT_VALUES_EQUAL(0, env.RequestHandler->Calls);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_REJECTED), env.ErrorCode);
    }

    Y_UNIT_TEST(ShouldRejectResponseThatDoesNotFitOutBuffer)
    {
        TEnv env(64);

        env.Send(MakeRequest(1, 2, "body"));
        env.RequestHandler->Response.SetValue(
            TTabletTransportResponse(3, TString(1_KB, 'x')));

        UNIT_ASSERT(!env.Responded);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_REJECTED), env.ErrorCode);
    }
}

}   // namespace NCloud::NFileStore::NStorage
