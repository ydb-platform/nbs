#include <cloud/blockstore/libs/service_rdma/rdma_target.h>
#include <cloud/blockstore/libs/service_rdma/rdma_protocol.h>

#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>

#include <functional>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestEndpoint: public NCloud::NStorage::NRdma::IServerEndpoint
{
    NThreading::TPromise<void> Done{NThreading::NewPromise<void>()};

public:
    void SendResponse(
        NCloud::NStorage::NRdma::IServerRequest* context,
        size_t responseBytes) override
    {
        Y_UNUSED(context);
        Y_UNUSED(responseBytes);
        Done.SetValue();
    }

    void SendError(
        NCloud::NStorage::NRdma::IServerRequest* context,
        ui32 error,
        TStringBuf message) override
    {
        Y_UNUSED(context);
        Y_UNUSED(error);
        Y_UNUSED(message);
        Done.SetValue();
    }

    NThreading::TFuture<void> WaitDone()
    {
        return Done.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSession final: public NCloud::NStorage::NRdma::IServerSession
{
    const ui64 Id;

    explicit TTestSession(ui64 id)
        : Id(id)
    {}

    ui64 GetId() const override
    {
        return Id;
    }

    TString GetPeer() const override
    {
        return "10.0.0.1:41234";
    }

    TInstant GetStartTs() const override
    {
        return TInstant::Seconds(100);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestRequest final: public NCloud::NStorage::NRdma::IServerRequest
{
    const ui64 SessionId;

    explicit TTestRequest(ui64 sessionId)
        : SessionId(sessionId)
    {}

    ui64 GetSessionId() const override
    {
        return SessionId;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestServer: public NCloud::NStorage::NRdma::IServer
{
    std::shared_ptr<TTestEndpoint> Endpoint;
    NCloud::NStorage::NRdma::IServerHandlerPtr Handler;

public:
    NCloud::NStorage::NRdma::IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        NCloud::NStorage::NRdma::IServerHandlerPtr handler) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Handler = std::move(handler);
        Endpoint = std::make_shared<TTestEndpoint>();
        return Endpoint;
    }

    void Start() override {}
    void Stop() override {}
    void DumpHtml(IOutputStream& out) const override { Y_UNUSED(out); }

    NCloud::NStorage::NRdma::IServerHandlerPtr GetHandler() const
    {
        return Handler;
    }

    std::shared_ptr<TTestEndpoint> GetEndpoint() const
    {
        return Endpoint;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    std::shared_ptr<TTestServer> Server;
    IMonitoringServicePtr Monitoring;
    IStartablePtr Target;

    NCloud::NStorage::NRdma::IServerHandlerPtr GetHandler() const
    {
        return Server->GetHandler();
    }

    std::shared_ptr<TTestEndpoint> GetEndpoint() const
    {
        return Server->GetEndpoint();
    }
};

TTestEnv CreateTestEnv(IBlockStorePtr service)
{
    auto server = std::make_shared<TTestServer>();

    NProto::TRdmaTarget rdmaTargetProto;
    auto config =
        std::make_shared<TBlockstoreServerRdmaTargetConfig>(rdmaTargetProto);

    auto logging = CreateLoggingService("console");
    auto traceSerializer = CreateTraceSerializerStub();
    auto monitoring = CreateMonitoringServiceStub();

    auto target = CreateBlockstoreServerRdmaTarget(
        config,
        std::move(logging),
        std::move(traceSerializer),
        monitoring,
        server,
        std::move(service));

    target->Start();

    return TTestEnv{
        std::move(server),
        std::move(monitoring),
        std::move(target)};
}

NMonitoring::TIndexMonPage* FindRootPage(const IMonitoringServicePtr& monitoring)
{
    auto rootPage = monitoring->GetMonPage("blockstore");
    UNIT_ASSERT(rootPage);

    auto* indexPage =
        dynamic_cast<NMonitoring::TIndexMonPage*>(rootPage.Get());
    UNIT_ASSERT(indexPage);

    return indexPage;
}

TString RenderMonPage(const IMonitoringServicePtr& monitoring)
{
    auto* page = dynamic_cast<NMonitoring::THtmlMonPage*>(
        FindRootPage(monitoring)->FindPage("RdmaTarget"));
    UNIT_ASSERT(page);

    TStringStream out;

    // OutputContent() only ever reaches for the output stream, so the rest of
    // the request can stay empty
    NMonitoring::TMonService2HttpRequest request{
        &out,
        nullptr,   // httpRequest
        nullptr,   // monService
        page,
        "",        // pathInfo
        nullptr};  // parent

    page->OutputContent(request);

    return out.Str();
}

// The registry is updated asynchronously, so the page only catches up with a
// mount some time after the response for it has been sent.
TString WaitForRenderedPage(
    const IMonitoringServicePtr& monitoring,
    const std::function<bool(const TString&)>& ready)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(5);

    for (;;) {
        auto html = RenderMonPage(monitoring);
        if (ready(html) || TInstant::Now() >= deadline) {
            return html;
        }

        Sleep(TDuration::MilliSeconds(10));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestHandlerTest)
{
    Y_UNIT_TEST(ShouldCloseGuardedSgListOnReadBlocksComplete)
    {
        auto service = std::make_shared<TTestService>();

        TGuardedSgList capturedSgList;
        bool handlerCalled = false;

        service->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            capturedSgList = request->Sglist;
            handlerCalled = true;
            return NThreading::MakeFuture(NProto::TReadBlocksLocalResponse{});
        };

        auto env = CreateTestEnv(service);
        auto handler = env.GetHandler();
        auto endpoint = env.GetEndpoint();

        // Serialize a ReadBlocks request (no data)
        NProto::TReadBlocksRequest request;
        request.SetBlockSize(4096);
        request.SetBlocksCount(1);

        const size_t inSize =
            NCloud::NStorage::NRdma::TProtoMessageSerializer::MessageByteSize(
                request,
                0);
        TString inBuf(inSize, 0);
        NCloud::NStorage::NRdma::TProtoMessageSerializer::Serialize(
            inBuf,
            TBlockStoreServerProtocol::EvReadBlocksRequest,
            0,
            request);

        // Output buffer: header + proto + 1 block of data
        TString outBuf(8_KB, 0);

        auto doneFuture = endpoint->WaitDone();
        handler->HandleRequest(
            nullptr,
            handler->CreateCallContext(),
            inBuf,
            outBuf);

        doneFuture.Wait();

        UNIT_ASSERT_C(handlerCalled, "ReadBlocksLocal handler was not called");

        // guardedSgList.Close() must have been called before SendResponse,
        // so Acquire() on any copy of it must return a false guard.
        auto guard = capturedSgList.Acquire();
        UNIT_ASSERT_C(
            !guard,
            "guardedSgList was not closed after ReadBlocks completed");

        env.Target->Stop();
    }

    Y_UNIT_TEST(ShouldCloseGuardedSgListOnWriteBlocksComplete)
    {
        auto service = std::make_shared<TTestService>();

        TGuardedSgList capturedSgList;
        bool handlerCalled = false;

        service->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            capturedSgList = request->Sglist;
            handlerCalled = true;
            return NThreading::MakeFuture(NProto::TWriteBlocksLocalResponse{});
        };

        auto env = CreateTestEnv(service);
        auto handler = env.GetHandler();
        auto endpoint = env.GetEndpoint();

        // Serialize a WriteBlocks request with 1 block of data
        NProto::TWriteBlocksRequest request;
        request.SetBlockSize(4096);

        const TString blockData(4096, 'X');
        const TBlockDataRef dataRef{blockData.data(), blockData.size()};
        const TBlockDataRefSpan dataSpan{&dataRef, 1};

        const size_t inSize =
            NCloud::NStorage::NRdma::TProtoMessageSerializer::MessageByteSize(
                request,
                blockData.size());
        TString inBuf(inSize, 0);
        NCloud::NStorage::NRdma::TProtoMessageSerializer::SerializeWithData(
            inBuf,
            TBlockStoreServerProtocol::EvWriteBlocksRequest,
            0,
            request,
            dataSpan);

        // Output buffer: header + proto (no data in response)
        TString outBuf(4_KB, 0);

        auto doneFuture = endpoint->WaitDone();
        handler->HandleRequest(
            nullptr,
            handler->CreateCallContext(),
            inBuf,
            outBuf);

        doneFuture.Wait();

        UNIT_ASSERT_C(handlerCalled, "WriteBlocksLocal handler was not called");

        // guardedSgList.Close() must have been called before SendResponse,
        // so Acquire() on any copy of it must return a false guard.
        auto guard = capturedSgList.Acquire();
        UNIT_ASSERT_C(
            !guard,
            "guardedSgList was not closed after WriteBlocks completed");

        env.Target->Stop();
    }

    Y_UNIT_TEST(ShouldForceRemoteBindingForMountVolume)
    {
        auto service = std::make_shared<TTestService>();

        bool handlerCalled = false;
        service->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            handlerCalled = true;
            UNIT_ASSERT(request->GetForceRemoteBinding());
            UNIT_ASSERT(
                request->GetVolumeMountMode() ==
                NProto::VOLUME_MOUNT_LOCAL);
            return NThreading::MakeFuture(NProto::TMountVolumeResponse{});
        };

        auto env = CreateTestEnv(service);
        auto handler = env.GetHandler();
        auto endpoint = env.GetEndpoint();

        NProto::TMountVolumeRequest request;
        request.SetVolumeMountMode(NProto::VOLUME_MOUNT_LOCAL);

        const size_t inSize =
            NCloud::NStorage::NRdma::TProtoMessageSerializer::MessageByteSize(
                request,
                0);
        TString inBuf(inSize, 0);
        NCloud::NStorage::NRdma::TProtoMessageSerializer::Serialize(
            inBuf,
            TBlockStoreServerProtocol::EvMountVolumeRequest,
            0,
            request);

        TString outBuf(4_KB, 0);

        auto doneFuture = endpoint->WaitDone();
        handler->HandleRequest(
            nullptr,
            handler->CreateCallContext(),
            inBuf,
            outBuf);

        doneFuture.Wait();

        UNIT_ASSERT_C(handlerCalled, "MountVolume handler was not called");

        env.Target->Stop();
    }

    Y_UNIT_TEST(ShouldShowMountedVolumeOfConnectionOnMonPage)
    {
        auto service = std::make_shared<TTestService>();
        service->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            Y_UNUSED(request);
            return NThreading::MakeFuture(NProto::TMountVolumeResponse{});
        };

        auto env = CreateTestEnv(service);
        auto handler = env.GetHandler();
        auto endpoint = env.GetEndpoint();

        TTestSession session(4242);
        handler->OnSessionCreated(session);

        NProto::TMountVolumeRequest request;
        request.SetDiskId("vol-1");
        request.MutableHeaders()->SetClientId("client-a");
        request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
        request.SetVolumeMountMode(NProto::VOLUME_MOUNT_LOCAL);
        request.SetMountSeqNumber(11);

        const size_t inSize =
            NCloud::NStorage::NRdma::TProtoMessageSerializer::MessageByteSize(
                request,
                0);
        TString inBuf(inSize, 0);
        NCloud::NStorage::NRdma::TProtoMessageSerializer::Serialize(
            inBuf,
            TBlockStoreServerProtocol::EvMountVolumeRequest,
            0,
            request);

        TString outBuf(4_KB, 0);

        TTestRequest context(session.GetId());

        auto doneFuture = endpoint->WaitDone();
        handler->HandleRequest(
            &context,
            handler->CreateCallContext(),
            inBuf,
            outBuf);

        doneFuture.Wait();

        const TString html = WaitForRenderedPage(
            env.Monitoring,
            [](const TString& html) { return html.Contains("vol-1"); });

        UNIT_ASSERT_STRING_CONTAINS(html, "10.0.0.1:41234");
        UNIT_ASSERT_STRING_CONTAINS(html, "vol-1");
        UNIT_ASSERT_STRING_CONTAINS(html, "client-a");
        UNIT_ASSERT_STRING_CONTAINS(html, "VOLUME_ACCESS_READ_ONLY");
        UNIT_ASSERT_STRING_CONTAINS(html, "VOLUME_MOUNT_LOCAL");

        env.Target->Stop();
    }

    Y_UNIT_TEST(ShouldShowConnectionWithoutMountsAndForgetItOnClose)
    {
        auto service = std::make_shared<TTestService>();
        auto env = CreateTestEnv(service);
        auto handler = env.GetHandler();

        TTestSession session(4242);
        handler->OnSessionCreated(session);

        auto html = WaitForRenderedPage(
            env.Monitoring,
            [](const TString& html)
            { return html.Contains("10.0.0.1:41234"); });
        UNIT_ASSERT_STRING_CONTAINS(html, "10.0.0.1:41234");

        handler->OnSessionClosed(session.GetId());

        html = WaitForRenderedPage(
            env.Monitoring,
            [](const TString& html)
            { return !html.Contains("10.0.0.1:41234"); });
        UNIT_ASSERT_C(
            !html.Contains("10.0.0.1:41234"),
            "closed connection is still shown: " << html);

        env.Target->Stop();
    }

    Y_UNIT_TEST(ShouldRegisterMonPageUnderBlockStoreIndex)
    {
        auto service = std::make_shared<TTestService>();
        auto env = CreateTestEnv(service);

        UNIT_ASSERT(FindRootPage(env.Monitoring)->FindPage("RdmaTarget"));

        env.Target->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
