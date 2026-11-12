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

#include <util/generic/yexception.h>
#include <util/system/event.h>

#include <thread>

namespace NCloud::NFileStore::NStorage {

using namespace NCloud::NStorage::NRdma;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestRequestHandler final
    : public IRdmaTabletRequestHandler
{
    ui32 Calls = 0;
    NProtoPrivate::TTabletRdmaRequest Envelope;
    TString Body;
    TString Exception;

    TPromise<TTabletTransportResponse> Response =
        NewPromise<TTabletTransportResponse>();

    TFuture<TTabletTransportResponse> HandleRequest(
        const NProtoPrivate::TTabletRdmaRequest& envelope,
        TStringBuf requestBody,
        TCallContextBasePtr callContext) override
    {
        Y_UNUSED(callContext);

        ++Calls;
        if (Exception) {
            ythrow yexception() << Exception;
        }

        Envelope = envelope;
        Body = TString(requestBody);
        return Response.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

// The real server listens before StartEndpoint() returns the endpoint, so a
// poller thread may complete a request first.
struct TEarlyRequestServer final
    : public IServer
{
    const IServerPtr Server;
    const TStringBuf In;
    const TStringBuf Out;

    std::thread Poller;
    TManualEvent Handled;

    TEarlyRequestServer(IServerPtr server, TStringBuf in, TStringBuf out)
        : Server(std::move(server))
        , In(in)
        , Out(out)
    {}

    IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        IServerHandlerPtr handler) override
    {
        auto endpoint = Server->StartEndpoint(std::move(host), port, handler);

        Poller = std::thread(
            [this, handler]
            {
                handler->HandleRequest(
                    nullptr,
                    handler->CreateCallContext(),
                    In,
                    Out);
                Handled.Signal();
            });
        Handled.WaitT(TDuration::MilliSeconds(100));

        return endpoint;
    }

    void Join()
    {
        Poller.join();
    }

    void Start() override
    {}

    void Stop() override
    {}

    void DumpHtml(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    const std::shared_ptr<TTestRequestHandler> RequestHandler;

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
        : TEnv(outSize, std::make_shared<TTestRequestHandler>(), TString())
    {}

    TEnv(
            size_t outSize,
            std::shared_ptr<TTestRequestHandler> requestHandler,
            TString earlyRequest)
        : RequestHandler(std::move(requestHandler))
        , In(std::move(earlyRequest))
        , Out(outSize, 0)
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

        std::shared_ptr<TEarlyRequestServer> earlyServer;
        IServerPtr server = Server;
        if (In) {
            earlyServer =
                std::make_shared<TEarlyRequestServer>(Server, In, Out);
            server = earlyServer;
        }

        Target = CreateFileStoreRdmaServiceTarget(
            std::move(config),
            CreateLoggingService("console"),
            std::move(server),
            RequestHandler);
        Target->Start();

        if (earlyServer) {
            earlyServer->Join();
        }
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

TString MakeRequest(
    const NProtoPrivate::TTabletRdmaRequest& envelope,
    TStringBuf body)
{
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

TString MakeRequest(ui32 methodId, TStringBuf body)
{
    NProtoPrivate::TTabletRdmaRequest envelope;
    envelope.SetMethodId(methodId);
    return MakeRequest(envelope, body);
}

std::unique_ptr<NProto::TError> MakeRecord(TString message)
{
    auto record = std::make_unique<NProto::TError>();
    record->SetCode(E_IO);
    record->SetMessage(std::move(message));
    return record;
}

size_t ResponseByteSize(ui32 methodId, const NProto::TError& record)
{
    NProtoPrivate::TTabletRdmaResponse envelope;
    envelope.SetMethodId(methodId);
    return TProtoMessageSerializer::MessageByteSize(
        envelope,
        record.ByteSizeLong());
}

NProto::TError ParseRecord(TStringBuf data)
{
    NProto::TError record;
    UNIT_ASSERT(record.ParseFromArray(data.data(), data.size()));
    return record;
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

    Y_UNIT_TEST(ShouldListenOnAllAddressesByDefault)
    {
        auto server = std::make_shared<TTestRdmaServer>(
            [](void* context, size_t responseBytes)
            {
                Y_UNUSED(context);
                Y_UNUSED(responseBytes);
            },
            [](void* context, ui32 error, TStringBuf message)
            {
                Y_UNUSED(context);
                Y_UNUSED(error);
                Y_UNUSED(message);
            });

        auto target = CreateFileStoreRdmaServiceTarget(
            std::make_shared<TFileStoreRdmaServerConfig>(),
            CreateLoggingService("console"),
            server,
            std::make_shared<TTestRequestHandler>());
        target->Start();

        UNIT_ASSERT_VALUES_EQUAL("::", server->GetHost());
        UNIT_ASSERT_VALUES_EQUAL(10021, server->GetPort());
    }

    Y_UNIT_TEST(ShouldPassDecodedRequestAndSerializeTypedResponse)
    {
        TEnv env(4_KB);

        NProtoPrivate::TTabletRdmaRequest request;
        request.SetMethodId(100);
        request.MutablePipeServerId()->SetRawX1(7);
        request.MutablePipeServerId()->SetRawX2(8);
        env.Send(MakeRequest(request, "request-body"));

        UNIT_ASSERT_VALUES_EQUAL(1, env.RequestHandler->Calls);
        const auto& passed = env.RequestHandler->Envelope;
        UNIT_ASSERT_VALUES_EQUAL(100, passed.GetMethodId());
        UNIT_ASSERT_VALUES_EQUAL(7, passed.GetPipeServerId().GetRawX1());
        UNIT_ASSERT_VALUES_EQUAL(8, passed.GetPipeServerId().GetRawX2());
        UNIT_ASSERT_VALUES_EQUAL("request-body", env.RequestHandler->Body);
        UNIT_ASSERT(!env.Responded);

        env.RequestHandler->Response.SetValue(
            TTabletTransportResponse(101, MakeRecord("typed response")));

        UNIT_ASSERT(!env.Failed);
        auto parsed = env.ParseResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TTabletRdmaProtocol::TabletResponse),
            parsed.MsgId);
        const auto* envelope = static_cast<NProtoPrivate::TTabletRdmaResponse*>(
            parsed.Proto.get());
        UNIT_ASSERT_VALUES_EQUAL(101, envelope->GetMethodId());

        auto record = ParseRecord(parsed.Data);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_IO), record.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("typed response", record.GetMessage());
    }

    Y_UNIT_TEST(ShouldAnswerRequestCompletedWhileEndpointIsStarting)
    {
        auto requestHandler = std::make_shared<TTestRequestHandler>();
        requestHandler->Response.SetValue(
            TTabletTransportResponse(101, MakeRecord("early response")));

        TEnv env(4_KB, requestHandler, MakeRequest(100, "request-body"));

        UNIT_ASSERT_VALUES_EQUAL(1, requestHandler->Calls);
        UNIT_ASSERT_VALUES_EQUAL(100, requestHandler->Envelope.GetMethodId());
        UNIT_ASSERT(!env.Failed);
        auto parsed = env.ParseResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            "early response",
            ParseRecord(parsed.Data).GetMessage());
    }

    Y_UNIT_TEST(ShouldSerializeEmptyResponse)
    {
        TEnv env(4_KB);

        env.Send(MakeRequest(2, ""));
        env.RequestHandler->Response.SetValue(TTabletTransportResponse(
            3,
            std::make_unique<NProto::TError>()));

        auto parsed = env.ParseResponse();
        UNIT_ASSERT(parsed.Data.empty());
    }

    Y_UNIT_TEST(ShouldReplyWithHandlerError)
    {
        TEnv env(4_KB);

        env.Send(MakeRequest(2, "body"));
        env.RequestHandler->Response.SetValue(TTabletTransportResponse(
            MakeError(E_TIMEOUT, "tablet request timed out")));

        UNIT_ASSERT(!env.Responded);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_TIMEOUT), env.ErrorCode);
        UNIT_ASSERT_VALUES_EQUAL("tablet request timed out", env.ErrorMessage);
    }

    Y_UNIT_TEST(ShouldReplyWithErrorWhenHandlerThrows)
    {
        TEnv env(4_KB);
        env.RequestHandler->Exception = "handler failed";

        env.Send(MakeRequest(2, "body"));

        UNIT_ASSERT_VALUES_EQUAL(1, env.RequestHandler->Calls);
        UNIT_ASSERT(!env.Responded);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_FAIL), env.ErrorCode);
        UNIT_ASSERT_STRING_CONTAINS(env.ErrorMessage, "handler failed");
    }

    Y_UNIT_TEST(ShouldReplyWithErrorWhenResponseFutureFails)
    {
        TEnv env(4_KB);
        env.RequestHandler->Response.SetException("tablet request failed");

        env.Send(MakeRequest(2, "body"));

        UNIT_ASSERT(!env.Responded);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_FAIL), env.ErrorCode);
        UNIT_ASSERT_STRING_CONTAINS(env.ErrorMessage, "tablet request failed");
    }

    Y_UNIT_TEST(ShouldRejectMalformedRequest)
    {
        TEnv env(4_KB);

        env.Send(TString(RDMA_PROTO_HEADER_SIZE, 0));

        UNIT_ASSERT_VALUES_EQUAL(0, env.RequestHandler->Calls);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_ARGUMENT), env.ErrorCode);
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
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(E_NOT_IMPLEMENTED),
            env.ErrorCode);
    }

    Y_UNIT_TEST(ShouldSerializeResponseThatExactlyFitsOutBuffer)
    {
        auto record = MakeRecord("exact fit");
        const size_t responseBytes = ResponseByteSize(3, *record);

        TEnv env(responseBytes);

        env.Send(MakeRequest(2, "body"));
        env.RequestHandler->Response.SetValue(
            TTabletTransportResponse(3, std::move(record)));

        UNIT_ASSERT(!env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(responseBytes, env.ResponseBytes);
        auto parsed = env.ParseResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            "exact fit",
            ParseRecord(parsed.Data).GetMessage());
    }

    Y_UNIT_TEST(ShouldRejectResponseThatDoesNotFitOutBuffer)
    {
        auto record = MakeRecord("one byte too long");
        TEnv env(ResponseByteSize(3, *record) - 1);

        env.Send(MakeRequest(2, "body"));
        env.RequestHandler->Response.SetValue(
            TTabletTransportResponse(3, std::move(record)));

        UNIT_ASSERT(!env.Responded);
        UNIT_ASSERT(env.Failed);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(E_ARGUMENT), env.ErrorCode);
    }
}

}   // namespace NCloud::NFileStore::NStorage
