#include "client.h"

#include "test_verbs.h"

#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/scope.h>
#include <util/stream/printf.h>

#include <thread>

namespace NCloud::NBlockStore::NRdma {

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

struct TRequestContext: public NRdma::TNullContext
{
    std::function<void(
        TStringBuf requestBuffer,
        TStringBuf responseBuffer,
        ui32 status,
        size_t responseBytes)> Handler;
};

struct TClientHandler
    : IClientHandler
{
    void HandleResponse(
        TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        auto* rc = static_cast<TRequestContext*>(req->Context.get());

        if (rc->Handler) {
            rc->Handler(
                req->RequestBuffer,
                req->ResponseBuffer,
                status,
                responseBytes);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: use custom timer

TEST(TRdmaClientTest, ShouldStartEndpoint)
{
        auto verbs =
            NVerbs::CreateTestVerbs(MakeIntrusive<NVerbs::TTestContext>());
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint("::", 10020);

        Y_UNUSED(clientEndpoint);
}

TEST(TRdmaClientTest, ShouldStartEndpointWithToS)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        auto verbs =
            NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->IpTypeOfService = 42;
        ASSERT_NE(42, testContext->ToS);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint("::", 10020);
        Y_UNUSED(clientEndpoint);
        ASSERT_EQ(42, testContext->ToS);
    }

TEST(TRdmaClientTest, ShouldDetachFromPoller)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto shared = client->StartEndpoint("::", 10020).ExtractValueSync();
        ASSERT_EQ(2u, shared.use_count());

        shared->Stop().Wait();
        auto deadline = GetCycleCount() + DurationToCycles(TDuration::Seconds(10));
        while (deadline > GetCycleCount() && shared.use_count() > 1) {
            SpinLockPause();
        }
        ASSERT_EQ(1u, shared.use_count());
    }

TEST(TRdmaClientTest, ShouldReturnErrorUponStartEndpointTimeout)
{
        auto verbs =
            NVerbs::CreateTestVerbs(MakeIntrusive<NVerbs::TTestContext>());
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = TDuration::Seconds(5);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint(
            "::",
            10020);

        try {
            clientEndpoint.GetValue(TDuration::Seconds(10));
            FAIL() << "expected exception";
        } catch (const TServiceError& e) {
            ASSERT_EQ(E_RDMA_UNAVAILABLE, e.GetCode()) << e.GetMessage();
        }
}

TEST(TRdmaClientTest, ShouldHandleGetAddressInfoError)
{
        auto context = MakeIntrusive<NVerbs::TTestContext>();
        auto verbs = NVerbs::CreateTestVerbs(context);
        auto monitoring = CreateMonitoringServiceStub();
        auto config = std::make_shared<TClientConfig>();
        config->MaxReconnectDelay = TDuration::Seconds(5);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            config);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        context->GetAddressInfo =
            [&](const TString& host,
                ui32 port,
                rdma_addrinfo* hints) -> NVerbs::TAddressInfoPtr
        {
            Y_UNUSED(host);
            Y_UNUSED(port);
            Y_UNUSED(hints);
            STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(EAGAIN));
        };

        try {
            auto clientEndpoint = client->StartEndpoint("::", 10020);
            clientEndpoint.GetValue(TDuration::Seconds(10));
            FAIL() << "expected exception";
        } catch (const TServiceError& e) {
            ASSERT_EQ(E_RDMA_UNAVAILABLE, e.GetCode()) << e.GetMessage();
        }
}

TEST(TRdmaClientTest, ShouldProcessRequests)
{
        // TODO(drbasic) reset to (RDMA_MAX_REQID - 1) or extract
        // TActiveRequests and make simple unit-test for requestId overflow
        constexpr size_t RequestCount = 10;

        constexpr size_t RequestBytes = 1024;
        constexpr size_t ResponseBytes = 1024;

        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = TDuration::Seconds(4);
        clientConfig->MaxResponseDelay = TDuration::Seconds(4);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto ep = client->StartEndpoint("::", 10020).GetValue(5s);

        testContext->PostSend = [&](auto* qp, auto* wr) {
            Y_UNUSED(qp);
            const auto* msg =
                reinterpret_cast<TRequestMessage*>(wr->sg_list[0].addr);
            testContext->ReqIds.push_back(msg->ReqId);
        };

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        auto makeContext = [](TManualEvent* ev, TResponse* response)
        {
            auto ctx = std::make_unique<TRequestContext>();
            ctx->Handler = [ev, response](
                               TStringBuf requestBuffer,
                               TStringBuf responseBuffer,
                               ui32 status,
                               size_t responseBytes)
            {
                Y_UNUSED(requestBuffer);

                response->Received = true;
                response->Buffer = responseBuffer;
                response->Status = status;
                response->Bytes = responseBytes;

                ev->Signal();
            };
            return ctx;
        };

        auto handleRequest = [](NVerbs::TTestContext& testContext)
        {
            while (true) {
                with_lock (testContext.CompletionLock) {
                    if (testContext.RecvEvents && testContext.ReqIds) {
                        auto* re = testContext.RecvEvents.front();
                        auto* responseMsg = reinterpret_cast<TResponseMessage*>(
                            re->sg_list[0].addr);
                        Zero(*responseMsg);
                        InitMessageHeader(responseMsg, RDMA_PROTO_VERSION);
                        responseMsg->ReqId = testContext.ReqIds.front();

                        testContext.ReqIds.pop_front();
                        testContext.RecvEvents.pop_front();
                        testContext.ProcessedRecvEvents.push_back(re);
                        testContext.CompletionHandle.Set();
                        break;
                    }
                }
            }
        };

        for (size_t i = 0; i < RequestCount; ++i) {
            {
                TManualEvent ev;
                TResponse response;

                auto r = ep->AllocateRequest(
                    std::make_shared<TClientHandler>(),
                    makeContext(&ev, &response),
                    RequestBytes,
                    ResponseBytes);
                ASSERT_FALSE(HasError(r.GetError()));

                auto request = r.ExtractResult();
                auto callContext = MakeIntrusive<TCallContextBase>(ui64{0});

                // Make sure that time spent before SendRequest doesn't count
                // towards the RDMA response timeout.
                auto retryDelay =
                    DurationToCyclesSafe(clientConfig->MaxResponseDelay) + 1;

                callContext->SetRequestStartedCycles(
                    GetCycleCount() - retryDelay);
                ep->SendRequest(std::move(request), callContext);

                handleRequest(*testContext);

                ev.WaitT(5s);
                ASSERT_TRUE(response.Received);
                ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_OK), response.Status);
            }

            if (i == 0 || i == RDMA_MAX_REQID - 3) {
                TManualEvent ev;
                TResponse response;

                auto timedOutRequest = ep->AllocateRequest(
                    std::make_shared<TClientHandler>(),
                    makeContext(&ev, &response),
                    RequestBytes,
                    ResponseBytes);
                ASSERT_FALSE(HasError(timedOutRequest.GetError()));

                auto request = timedOutRequest.ExtractResult();
                auto callContext = MakeIntrusive<TCallContextBase>(ui64{0});

                // Make sure that time spent before SendRequest doesn't count
                // towards the RDMA response timeout.
                auto retryDelay =
                    DurationToCyclesSafe(clientConfig->MaxResponseDelay) + 1;
                callContext->SetRequestStartedCycles(
                    GetCycleCount() - retryDelay);
                ep->SendRequest(std::move(request), callContext);

                ev.WaitT(TDuration::Seconds(5));
                ASSERT_TRUE(response.Received);
                ASSERT_EQ(
                    static_cast<ui32>(RDMA_PROTO_FAIL),
                    response.Status);

                NProto::TError error =
                    ParseError(response.Buffer.Head(response.Bytes));
                ASSERT_EQ(E_TIMEOUT, error.GetCode());

                // Handle the request after timeout to drain the test transport.
                handleRequest(*testContext);
            }

            auto counters = monitoring
                ->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "rdma_client");
            auto aborted = counters->GetCounter("AbortedRequests");
            ASSERT_EQ(aborted->Val(), 1);
        }
}

TEST(TRdmaClientTest, ShouldReuseChunks)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->BufferPool.ChunkSize = 80_MB;
        clientConfig->BufferPool.MaxChunkAlloc = 4_MB;

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(verbs, logging, monitoring, clientConfig);
        client->Start();
        Y_DEFER {
            client->Stop();
        };

        std::atomic<size_t> registered;
        testContext->RegisterMemoryRegion = [&](auto...)
        {
            registered++;
        };

        auto endpoint = client->StartEndpoint("localhost", 10020).GetValue(5s);

        TVector<TClientRequestPtr> requests;
        int maxRequestsInOneChunk = clientConfig->BufferPool.ChunkSize /
                                    clientConfig->BufferPool.MaxChunkAlloc;

        for (int i = 0; i < maxRequestsInOneChunk; i++) {
            auto [req, err] = endpoint->AllocateRequest(
                std::make_shared<TClientHandler>(),
                std::make_unique<TNullContext>(),
                4_MB,
                4_MB);
            ASSERT_FALSE(HasError(err));
            requests.push_back(std::move(req));
        }

        // 2 for recv/send buffers
        // 2 for input/output buffers
        ASSERT_EQ(registered.load(), 2u + 2u);
    }

TEST(TRdmaClientTest, ShouldAdjustMaxChunkAlloc)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->BufferPool.ChunkSize = 4_MB;
        clientConfig->BufferPool.MaxChunkAlloc = 8_MB;

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(verbs, logging, monitoring, clientConfig);
        client->Start();
        Y_DEFER {
            client->Stop();
        };

        std::atomic<size_t> registered;
        testContext->RegisterMemoryRegion = [&](auto...)
        {
            registered++;
        };

        auto endpoint = client->StartEndpoint("localhost", 10020).GetValue(5s);

        TVector<TClientRequestPtr> requests;
        int maxRequestsInOneChunk = clientConfig->BufferPool.ChunkSize /
                                    clientConfig->BufferPool.MaxChunkAlloc;
        constexpr int chunks = 10;

        for (int i = 0; i < maxRequestsInOneChunk * chunks; i++) {
            auto [req, err] = endpoint->AllocateRequest(
                std::make_shared<TClientHandler>(),
                std::make_unique<TNullContext>(),
                4_MB,
                4_MB);
            ASSERT_FALSE(HasError(err));
            requests.push_back(std::move(req));
        }

        // 2 for recv/send buffers
        // 2 * chunks for input/output buffers
        ASSERT_EQ(registered.load(), 2u + 2u * static_cast<unsigned>(chunks));
    }


TEST(TRdmaClientTest, ShouldAbortRequests)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = 1s;
        clientConfig->MaxResponseDelay = 1s;

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(verbs, logging, monitoring, clientConfig);
        client->Start();
        Y_DEFER {
            client->Stop();
        };

        TManualEvent sent;
        testContext->PostSend = [&](auto* qp, auto* wr) {
            Y_UNUSED(qp);
            const auto* msg =
                reinterpret_cast<TRequestMessage*>(wr->sg_list[0].addr);
            testContext->ReqIds.push_back(msg->ReqId);
            testContext->CompletionHandle.Set();
            sent.Signal();
        };

        auto endpoint = client->StartEndpoint("localhost", 10020).GetValue(5s);

        struct TClientHandler: IClientHandler
        {
            TManualEvent Done;

            void HandleResponse(
                TClientRequestPtr req,
                ui32 status,
                size_t responseBytes) override
            {
                Y_UNUSED(req);
                Y_UNUSED(responseBytes);

                ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), status);

                Done.Signal();
            }
        };

        auto handler = std::make_shared<TClientHandler>();
        auto request = endpoint->AllocateRequest(
            handler,
            std::make_unique<TNullContext>(),
            4096,   // requestBytes
            4096);  // responseBytes

        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(ui64{0}));
        ASSERT_TRUE(sent.WaitT(5s));

        Disconnect(testContext);
        ASSERT_TRUE(handler->Done.WaitT(5s));
    }

TEST(TRdmaClientTest, ShouldCancelRequests)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = 1s;
        clientConfig->MaxResponseDelay = 1s;

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(verbs, logging, monitoring, clientConfig);

        client->Start();
        Y_DEFER
        {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint("::", 10020);

        auto ep = clientEndpoint.GetValue(5s);

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        auto makeContext = [](TResponse& response, TManualEvent& ev)
        {
            auto ctx = std::make_unique<TRequestContext>();
            ctx->Handler = [&](TStringBuf requestBuffer,
                               TStringBuf responseBuffer,
                               ui32 status,
                               size_t responseBytes)
            {
                Y_UNUSED(requestBuffer);

                response =
                    TResponse{true, responseBuffer, status, responseBytes};
                ev.Signal();
            };
            return ctx;
        };

        TManualEvent ev1;
        TResponse response1;

        const size_t requestBytes = 1024;
        const size_t responseBytes = 1024;
        auto r1 = ep->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(response1, ev1),
            requestBytes,
            responseBytes);
        auto request1 = r1.ExtractResult();
        auto callContext = MakeIntrusive<TCallContextBase>(ui64{0});

        // make sure that time spent on request processing before SendRequest
        // won't be counted towards rdma timeout
        auto retryDelay =
            DurationToCyclesSafe(clientConfig->MaxResponseDelay) + 1;

        callContext->SetRequestStartedCycles(GetCycleCount() - retryDelay);
        auto reqId1 = ep->SendRequest(std::move(request1), callContext);

        TManualEvent ev2;
        TResponse response2;

        auto r2 = ep->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(response2, ev2),
            requestBytes,
            responseBytes);
        auto request2 = r2.ExtractResult();

        auto reqId2 = ep->SendRequest(std::move(request2), callContext);

        ep->CancelRequest(reqId2);

        ev2.WaitT(5s);
        ASSERT_TRUE(response2.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), response2.Status);

        NProto::TError error;
        bool parsed = error.ParseFromArray(
            response2.Buffer.Head(response2.Bytes).data(),
            response2.Bytes);

        ASSERT_TRUE(parsed);
        ASSERT_EQ(E_CANCELLED, error.GetCode());

        ASSERT_FALSE(response1.Received);

        ep->CancelRequest(reqId1);

        ev1.WaitT(5s);
        ASSERT_TRUE(response1.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), response1.Status);

        parsed = error.ParseFromArray(
            response1.Buffer.Head(response1.Bytes).data(),
            response1.Bytes);

        ASSERT_TRUE(parsed);
        ASSERT_EQ(E_CANCELLED, error.GetCode());

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "rdma_client");
        auto aborted = counters->GetCounter("AbortedRequests");
        auto queued = counters->GetCounter("QueuedRequests");
        ASSERT_EQ(queued->Val(), 0);
        ASSERT_EQ(aborted->Val(), 2);
    }

TEST(TRdmaClientTest, ShouldReconnect)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = TDuration::Seconds(1);
        clientConfig->MaxResponseDelay = TDuration::Seconds(1);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        with_lock(testContext->CompletionLock) {
            testContext->ModifyQP = [&](auto* qp, auto* attr, int mask) {
                Y_UNUSED(qp);
                Y_UNUSED(attr);
                Y_UNUSED(mask);

                // reschedule in the middle of the FlushQueues to let CQ trigger
                // the race
                std::this_thread::yield();
            };
        }

        auto clientEndpoint = client->StartEndpoint(
            "::",
            10020);

        testContext->PostSend = [&](auto* qp, auto* wr) {
            Y_UNUSED(qp);
            const auto* msg =
                reinterpret_cast<TRequestMessage*>(wr->sg_list[0].addr);
            testContext->ReqIds.push_back(msg->ReqId);
        };

        auto ep = clientEndpoint.GetValue(TDuration::Seconds(5));

        Disconnect(testContext);

        // wait for receive queue to initialize 2nd time after reconnect
        ui64 recv;
        do {
            recv = AtomicGet(testContext->PostRecvCounter);
        } while (recv != 2 * clientConfig->QueueSize);

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent ev;
        TResponse response;

        auto makeContext = [&]()
        {
            auto ctx = std::make_unique<TRequestContext>();
            ctx->Handler = [&](TStringBuf requestBuffer,
                               TStringBuf responseBuffer,
                               ui32 status,
                               size_t responseBytes)
            {
                Y_UNUSED(requestBuffer);

                response =
                    TResponse{true, responseBuffer, status, responseBytes};
                ev.Signal();
            };
            return ctx;
        };

        size_t requestBytes = 1024;
        size_t responseBytes = 1024;
        auto r = ep->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(),
            requestBytes,
            responseBytes);
        auto request = r.ExtractResult();
        auto callContext = MakeIntrusive<TCallContextBase>(ui64{0});
        auto retryDelay =
            DurationToCyclesSafe(clientConfig->MaxResponseDelay) + 1;
        callContext->SetRequestStartedCycles(GetCycleCount() - retryDelay);
        ep->SendRequest(std::move(request), callContext);

        while (true) {
            with_lock (testContext->CompletionLock) {
                if (testContext->RecvEvents && testContext->ReqIds) {
                    auto* re = testContext->RecvEvents.front();
                    auto* responseMsg = reinterpret_cast<TResponseMessage*>(
                        re->sg_list[0].addr);
                    Zero(*responseMsg);
                    InitMessageHeader(responseMsg, RDMA_PROTO_VERSION);
                    responseMsg->ReqId = testContext->ReqIds.front();

                    testContext->ReqIds.pop_front();
                    testContext->RecvEvents.pop_front();
                    testContext->ProcessedRecvEvents.push_back(re);
                    testContext->CompletionHandle.Set();
                    break;
                }
            }
        }

        ev.WaitT(TDuration::Seconds(5));
        ASSERT_TRUE(response.Received);
        ASSERT_EQ(0u, response.Status);
    }

TEST(TRdmaClientTest, ShouldForceReconnect)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = TDuration::Seconds(1);
        clientConfig->MaxResponseDelay = TDuration::Seconds(1);

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(verbs, logging, monitoring, clientConfig);

        client->Start();
        Y_DEFER
        {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint("::", 10020);
        auto ep = clientEndpoint.GetValue(TDuration::Seconds(5));

        // Increase reconnect timer delay up to "MaxReconnectDelay".
        for (int i = 0; i < 10; i++) {
            Disconnect(testContext);
        }

        const auto now = TInstant::Now();
        ep->TryForceReconnect();

        // wait for receive queue to initialize 2nd time after reconnect
        ui64 recv;
        do {
            recv = AtomicGet(testContext->PostRecvCounter);
        } while (recv != 2 * clientConfig->QueueSize);

        // Force reconnect should be less than "MaxReconnectDelay".
        const auto elapsed = now - TInstant::Now();
        ASSERT_LT(elapsed, clientConfig->MaxReconnectDelay);
    }

TEST(TRdmaClientTest, ShouldHandleErrors)
{
        auto context = MakeIntrusive<NVerbs::TTestContext>();
        context->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(context);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto endpoint = client->StartEndpoint("::", 10020)
            .GetValue(TDuration::Seconds(5));

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "rdma_client");

        auto active = counters->GetCounter("ActiveRecv");
        auto errors = counters->GetCounter("Errors");

        TVector<ibv_recv_wr*> recv;

        with_lock(context->CompletionLock) {
            // emulate IBV_QPS_ERR
            context->PostRecv = [](auto* qp, auto* wr) {
                Y_UNUSED(qp);
                Y_UNUSED(wr);
                STORAGE_THROW_SERVICE_ERROR(ENODEV) << "ibv_post_recv error";
            };
            context->ModifyQP = [&](auto* qp, auto* attr, int mask) {
                Y_UNUSED(qp);
                Y_UNUSED(mask);
                ASSERT_EQ(attr->qp_state, IBV_QPS_ERR);
            };
            for (size_t i = 0; i < 5; i++) {
                auto* wr = context->RecvEvents.back();
                context->RecvEvents.pop_back();
                context->ProcessedRecvEvents.push_back(wr);
                recv.push_back(wr);
            }
            context->HandleCompletionEvent = [&](ibv_wc* wc) {
                // good id, good opcode, error status
                if (wc->wr_id == recv[0]->wr_id) {
                    wc->status = IBV_WC_RETRY_EXC_ERR;
                    return;
                }
                // good id and opcode, success status, good message, but unknown request
                if (wc->wr_id == recv[1]->wr_id) {
                    auto* msg = reinterpret_cast<TResponseMessage*>(recv[1]->sg_list[0].addr);
                    InitMessageHeader(msg, RDMA_PROTO_VERSION);
                    return;
                }
                // bad id, good opcode
                if (wc->wr_id == recv[2]->wr_id) {
                    wc->wr_id = Max<ui64>();
                    return;
                }
                // good id, bad opcode
                if (wc->wr_id == recv[3]->wr_id) {
                    wc->opcode = IBV_WC_RECV_RDMA_WITH_IMM;
                    return;
                }
                // good id and opcode, success status, bad message
            };
            context->CompletionHandle.Set();
        }

        auto wait = [](auto& counter, auto value) {
            auto start = GetCycleCount();
            while (counter->Val() != value) {
                auto now = GetCycleCount();
                if (CyclesToDurationSafe(now - start) > TDuration::Seconds(5)) {
                    FAIL() << "timed out waiting for counter";
                }
                SpinLockPause();
            }
        };

        wait(errors, 7);
        wait(active, 6);
}

}   // namespace NCloud::NBlockStore::NRdma
