#include "client.h"

#include "test_verbs.h"
#include "utils.h"

#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/stream/printf.h>
#include <util/system/mutex.h>

#include <cstring>
#include <signal.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace NCloud::NStorage::NRdma {

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

NMonitoring::TDynamicCountersPtr GetClientCounters(
    const IMonitoringServicePtr& monitoring)
{
    return monitoring->GetCounters()
        ->GetSubgroup("counters", "rdma")
        ->GetSubgroup("component", "client");
}

ui64 GetHistogramSampleCount(
    const NMonitoring::TDynamicCountersPtr& counters,
    const TString& name)
{
    auto group = counters->GetSubgroup("histogram", name);
    group = group->GetSubgroup("units", "usec");
    auto histogram = group->FindHistogram(name);
    EXPECT_TRUE(histogram);
    if (!histogram) {
        return 0;
    }

    const auto snapshot = histogram->Snapshot();
    ui64 count = 0;
    for (size_t i = 0; i < snapshot->Count(); ++i) {
        count += snapshot->Value(i);
    }
    return count;
}

IClientPtr CreateTestClient(
    NVerbs::IVerbsPtr verbs,
    const ILoggingServicePtr& logging,
    const IMonitoringServicePtr& monitoring,
    TClientConfigPtr config)
{
    return CreateClient(
        std::move(verbs),
        TObservabilityProvider(
            logging,
            monitoring,
            "RDMA_TEST",
            "rdma",
            "client"),
        std::move(config));
}

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

        auto client = CreateTestClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        client->StartEndpoint("::", 10020);
}

TEST(TRdmaClientTest, ShouldStartEndpointWithToS)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();
    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->IpTypeOfService = 42;
    ASSERT_NE(42, testContext->ToS);

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto client = CreateTestClient(verbs, logging, monitoring, clientConfig);

    client->Start();
    Y_DEFER
    {
        client->Stop();
    };

    client->StartEndpoint("::", 10020);
    ASSERT_EQ(42, testContext->ToS);
}

TEST(TRdmaClientTest, ShouldUseConfiguredResolveTimeoutAndQpParamsOnConnect)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();
    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();

    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->ResolveTimeout = 123ms;
    clientConfig->QpRetryCount = 3;
    clientConfig->QpRnrRetryCount = 5;
    clientConfig->QpTimeout = 7;
    clientConfig->QpMinRnrTimer = 9;

    std::atomic<bool> resolveAddressCalled = false;
    std::atomic<bool> resolveRouteCalled = false;
    std::atomic<ui64> resolveAddressTimeoutUs = 0;
    std::atomic<ui64> resolveRouteTimeoutUs = 0;

    std::atomic<bool> connectCalled = false;
    std::atomic<int> connectRetryCount = -1;
    std::atomic<int> connectRnrRetryCount = -1;

    std::atomic<bool> modifyCalled = false;

    testContext->HandleResolveAddress =
        [&](rdma_cm_id* id, sockaddr* srcAddr, sockaddr* dstAddr, TDuration t)
    {
        Y_UNUSED(id);
        Y_UNUSED(srcAddr);
        Y_UNUSED(dstAddr);

        resolveAddressTimeoutUs.store(t.MicroSeconds());
        resolveAddressCalled.store(true);
    };

    testContext->HandleResolveRoute = [&](rdma_cm_id* id, TDuration t)
    {
        Y_UNUSED(id);

        resolveRouteTimeoutUs.store(t.MicroSeconds());
        resolveRouteCalled.store(true);
    };

    testContext->ModifyQP = [&](ibv_qp* qp, ibv_qp_attr* attr, int mask)
    {
        Y_UNUSED(qp);

        const int expectedMask = IBV_QP_TIMEOUT | IBV_QP_MIN_RNR_TIMER;

        EXPECT_EQ(expectedMask, mask);
        EXPECT_EQ(clientConfig->QpTimeout, attr->timeout);
        EXPECT_EQ(clientConfig->QpMinRnrTimer, attr->min_rnr_timer);

        modifyCalled.store(true);
    };

    testContext->HandleConnect = [&](rdma_cm_id* id, rdma_conn_param* param)
    {
        connectRetryCount.store(param->retry_count);
        connectRnrRetryCount.store(param->rnr_retry_count);
        connectCalled.store(true);

        TAcceptMessage acceptMsg{};
        InitMessageHeader(&acceptMsg, RDMA_PROTO_VERSION);

        NVerbs::EnqueueAcceptEvent(
            testContext,
            id,
            &acceptMsg,
            sizeof(acceptMsg));
    };

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto client = CreateTestClient(verbs, logging, monitoring, clientConfig);

    client->Start();
    Y_DEFER
    {
        client->Stop();
    };

    auto ep = client->StartEndpoint("::", 10020).ExtractValueSync();
    ASSERT_TRUE(ep);

    ASSERT_TRUE(resolveAddressCalled.load());
    ASSERT_TRUE(resolveRouteCalled.load());
    ASSERT_EQ(
        clientConfig->ResolveTimeout.MicroSeconds(),
        resolveAddressTimeoutUs.load());
    ASSERT_EQ(
        clientConfig->ResolveTimeout.MicroSeconds(),
        resolveRouteTimeoutUs.load());

    ASSERT_TRUE(connectCalled.load());
    ASSERT_EQ(clientConfig->QpRetryCount, connectRetryCount.load());
    ASSERT_EQ(clientConfig->QpRnrRetryCount, connectRnrRetryCount.load());

    ASSERT_TRUE(modifyCalled.load());
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

        auto client = CreateTestClient(
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
        while (shared.use_count() > 1) {
            SpinLockPause();
        }
    }

TEST(TRdmaClientTest, ShouldNotTriggerCompletionAfterFlushTimeout)
{
        auto context = MakeIntrusive<NVerbs::TTestContext>();
        context->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(context);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->WaitMode = EWaitMode::Poll;
        clientConfig->MaxReconnectDelay = 5s;

        // even though it can technically be less than POLL_TIMEOUT, actual
        // reaction time is still bound by it, because DisconnectFlushed runs
        // only after Wait times out
        clientConfig->FlushTimeout = 1s;

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateTestClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        struct TClientHandler: IClientHandler
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

        std::vector<std::unique_ptr<ibv_send_wr>> sends;
        context->PostSend = [&](auto* qp, auto* wr) {
            Y_UNUSED(qp);
            // hold send to complete it later
            with_lock (context->CompletionLock) {
                sends.push_back(std::make_unique<ibv_send_wr>(*wr));
            }

            // stall completion poller long enough for flush to time out
            sleep(clientConfig->FlushTimeout.Seconds());

            with_lock (context->CompletionLock) {
                while (sends.size()) {
                    context->SendEvents.push_back(sends.back().release());
                    sends.pop_back();
                }
                context->CompletionHandle.Set();
            }
        };

        // stall completion poller again to let connection poller destroy
        // endpoint and trigger use-after-free
        with_lock (context->CompletionLock) {
            context->HandleCompletionEvent = [&](ibv_wc* wc) {
                Y_UNUSED(wc);
                sleep(1);
            };
        }

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            std::make_unique<TNullContext>(),
            1024,
            1024);

        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        endpoint->Stop().Wait();
}

TEST(TRdmaClientTest, ShouldReturnErrorUponStartEndpointTimeout)
{
        auto verbs =
            NVerbs::CreateTestVerbs(MakeIntrusive<NVerbs::TTestContext>());
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = 5s;

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateTestClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        try {
            client->StartEndpoint("::", 10020).ExtractValueSync();
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
        config->MaxReconnectDelay = 5s;

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateTestClient(
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
            client->StartEndpoint("::", 10020).ExtractValueSync();
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
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->MaxResponseDelay = 4s;

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateTestClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr) {
            PostSend<TRequestMessage>(testContext, qp, wr);
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

        long timedOutRequests = 0;

        for (size_t i = 0; i < RequestCount; ++i) {
            TManualEvent ev;
            TResponse response;

            auto request = endpoint->AllocateRequest(
                std::make_shared<TClientHandler>(),
                makeContext(&ev, &response),
                RequestBytes,
                ResponseBytes);
            ASSERT_FALSE(HasError(request.GetError()));

            endpoint->SendRequest(
                request.ExtractResult(),
                MakeIntrusive<TCallContextBase>(0u));

            if (i != 0 && i != RDMA_MAX_REQID - 3) {
                // complete request right away
                handleRequest(*testContext);

                ev.WaitT(clientConfig->MaxResponseDelay + 1s);
                ASSERT_TRUE(response.Received);

                // request duration is measured against the wall clock, so it
                // can time out if the process stalls for some reason
                if (response.Status != RDMA_PROTO_OK) {
                    NProto::TError error =
                        ParseError(response.Buffer.Head(response.Bytes));
                    ASSERT_EQ(E_TIMEOUT, error.GetCode());
                    timedOutRequests++;
                }
            } else {
                // do not complete request to trigger timeout
                ev.WaitT(clientConfig->MaxResponseDelay + 1s);
                ASSERT_TRUE(response.Received);

                NProto::TError error =
                    ParseError(response.Buffer.Head(response.Bytes));
                ASSERT_EQ(E_TIMEOUT, error.GetCode());
                timedOutRequests++;

                // complete request to drain the test transport
                handleRequest(*testContext);
            }

            auto counters = GetClientCounters(monitoring);
            auto aborted = counters->GetCounter("AbortedRequests");
            ASSERT_EQ(aborted->Val(), timedOutRequests);
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

        auto client =
            CreateTestClient(verbs, logging, monitoring, clientConfig);
        client->Start();
        Y_DEFER {
            client->Stop();
        };

        std::atomic<size_t> registered;
        testContext->RegisterMemoryRegion = [&](auto...)
        {
            registered++;
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

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

        auto client =
            CreateTestClient(verbs, logging, monitoring, clientConfig);
        client->Start();
        Y_DEFER {
            client->Stop();
        };

        std::atomic<size_t> registered;
        testContext->RegisterMemoryRegion = [&](auto...)
        {
            registered++;
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

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

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client =
            CreateTestClient(verbs, logging, monitoring, clientConfig);
        client->Start();
        Y_DEFER {
            client->Stop();
        };

        TManualEvent sent;
        testContext->PostSend = [&](auto* qp, auto* wr) {
            Y_UNUSED(qp);
            Y_UNUSED(wr);
            sent.Signal();
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

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
        ASSERT_FALSE(HasError(request.GetError()));

        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        ASSERT_TRUE(sent.Wait());

        Disconnect(testContext);
        ASSERT_TRUE(handler->Done.Wait());
    }

void TestDispatchResponsesOutsideCompletionPoller(
    EWaitMode waitMode,
    bool throwFirst = false)
{
    constexpr ui32 QueueSize = 2;
    constexpr ui32 ExpectedPostedRecvs = 2 * QueueSize;

    auto testContext = MakeIntrusive<NVerbs::TTestContext>();
    testContext->AllowConnect = true;

    std::atomic<ui32> postedSends = 0;
    TManualEvent requestsSent;
    testContext->PostSend = [&](auto* qp, auto* wr) {
        PostSend<TRequestMessage>(testContext, qp, wr);
        if (postedSends.fetch_add(1) + 1 == QueueSize) {
            requestsSent.Signal();
        }
    };

    std::atomic<ui32> postedRecvs = 0;
    TManualEvent responsesReposted;
    testContext->PostRecv = [&](auto* qp, auto* wr) {
        Y_UNUSED(qp, wr);
        if (postedRecvs.fetch_add(1) + 1 == ExpectedPostedRecvs) {
            responsesReposted.Signal();
        }
    };

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->WaitMode = waitMode;
    clientConfig->SendQueueSize = QueueSize;
    clientConfig->RecvQueueSize = QueueSize;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});
    auto client = CreateTestClient(
        verbs,
        logging,
        monitoring,
        clientConfig);

    struct TResponseContext final: TNullContext
    {
        bool Block = false;
    };

    struct TBlockingHandler final: IClientHandler
    {
        const bool ThrowFirst;

        TManualEvent FirstStarted;
        TManualEvent ReleaseFirst;
        TManualEvent FirstDone;
        TManualEvent SecondDone;

        explicit TBlockingHandler(bool throwFirst)
            : ThrowFirst(throwFirst)
        {}

        void HandleResponse(
            TClientRequestPtr req,
            ui32 status,
            size_t responseBytes) override
        {
            Y_UNUSED(responseBytes);
            EXPECT_EQ(static_cast<ui32>(RDMA_PROTO_OK), status);

            auto* context = static_cast<TResponseContext*>(req->Context.get());
            if (context->Block) {
                FirstStarted.Signal();
                ReleaseFirst.WaitI();
                FirstDone.Signal();
                if (ThrowFirst) {
                    ythrow yexception() << "expected response handler failure";
                }
            } else {
                SecondDone.Signal();
            }
        }
    };

    auto handler = std::make_shared<TBlockingHandler>(throwFirst);
    client->Start();
    Y_DEFER
    {
        handler->ReleaseFirst.Signal();
        client->Stop();
    };

    auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

    for (bool block: {true, false}) {
        auto context = std::make_unique<TResponseContext>();
        context->Block = block;

        auto request = endpoint->AllocateRequest(
            handler,
            std::move(context),
            1024,
            1024);
        ASSERT_FALSE(HasError(request.GetError()));

        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));
    }

    ASSERT_TRUE(requestsSent.WaitT(5s));
    {
        auto guard = Guard(testContext->CompletionLock);
        ASSERT_EQ(QueueSize, testContext->ReqIds.size());
        ASSERT_EQ(QueueSize, testContext->RecvEvents.size());

        for (ui32 i = 0; i < QueueSize; ++i) {
            auto* recv = testContext->RecvEvents.front();
            testContext->RecvEvents.pop_front();

            auto* response = reinterpret_cast<TResponseMessage*>(
                recv->sg_list[0].addr);
            Zero(*response);
            InitMessageHeader(response, RDMA_PROTO_VERSION);
            response->ReqId = testContext->ReqIds.front();
            testContext->ReqIds.pop_front();

            testContext->ProcessedRecvEvents.push_back(recv);
        }
        testContext->CompletionHandle.Set();
    }

    ASSERT_TRUE(handler->FirstStarted.WaitT(5s));
    ASSERT_TRUE(responsesReposted.WaitT(5s));
    ASSERT_EQ(
        ExpectedPostedRecvs,
        AtomicGet(testContext->PostRecvCounter));
    ASSERT_FALSE(handler->SecondDone.WaitT(100ms));

    auto counters = GetClientCounters(monitoring);
    auto queuedCallbacks =
        counters->GetCounter("QueuedResponseCallbacks");
    auto activeCallbacks =
        counters->GetCounter("ActiveResponseCallbacks");
    auto completedCallbacks =
        counters->GetCounter("CompletedResponseCallbacks");
    auto callbackErrors = counters->GetCounter("ResponseCallbackErrors");

    ASSERT_EQ(1, queuedCallbacks->Val());
    ASSERT_EQ(1, activeCallbacks->Val());
    ASSERT_EQ(0, completedCallbacks->Val());
    ASSERT_EQ(0, callbackErrors->Val());

    TManualEvent clientStopped;
    std::thread stopThread([&] {
        client->Stop();
        clientStopped.Signal();
    });
    Y_DEFER {
        handler->ReleaseFirst.Signal();
        if (stopThread.joinable()) {
            stopThread.join();
        }
    };

    ASSERT_FALSE(clientStopped.WaitT(100ms));
    handler->ReleaseFirst.Signal();
    ASSERT_TRUE(handler->FirstDone.WaitT(5s));
    ASSERT_TRUE(handler->SecondDone.WaitT(5s));
    ASSERT_TRUE(clientStopped.WaitT(5s));
    stopThread.join();

    ASSERT_EQ(0, queuedCallbacks->Val());
    ASSERT_EQ(0, activeCallbacks->Val());
    ASSERT_EQ(2, completedCallbacks->Val());
    ASSERT_EQ(throwFirst ? 1 : 0, callbackErrors->Val());
    ASSERT_EQ(
        ui64{2},
        GetHistogramSampleCount(
            counters,
            "ResponseCallbackQueueWait"));
    ASSERT_EQ(
        ui64{2},
        GetHistogramSampleCount(
            counters,
            "ResponseCallbackExecutionTime"));
}

TEST(TRdmaClientTest, ShouldDispatchResponsesOutsideCompletionPoller)
{
    TestDispatchResponsesOutsideCompletionPoller(EWaitMode::BusyWait);
}

TEST(TRdmaClientTest, ShouldDispatchResponsesOutsideCompletionPollerInPollMode)
{
    TestDispatchResponsesOutsideCompletionPoller(EWaitMode::Poll);
}

TEST(TRdmaClientTest, ShouldContinueDispatchingAfterResponseHandlerThrows)
{
    TestDispatchResponsesOutsideCompletionPoller(EWaitMode::BusyWait, true);
}

enum class EStopFromResponseScenario
{
    ClientRequest,
    ClientStop,
    EndpointRequest,
    EndpointAndWait,
};

void RunStopFromResponseScenario(
    EStopFromResponseScenario scenario,
    EWaitMode waitMode)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();
    testContext->AllowConnect = true;

    TManualEvent requestSent;
    testContext->PostSend = [&](auto* qp, auto* wr) {
        PostSend<TRequestMessage>(testContext, qp, wr);
        requestSent.Signal();
    };

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->WaitMode = waitMode;
    clientConfig->SendQueueSize = 1;
    clientConfig->RecvQueueSize = 1;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});
    auto client = CreateTestClient(
        verbs,
        logging,
        monitoring,
        clientConfig);

    struct TStoppingHandler final: IClientHandler
    {
        const EStopFromResponseScenario Scenario;

        IClientPtr Client;
        IClientEndpointPtr Endpoint;
        TManualEvent Returned;
        std::atomic<ui32> ErrorCode = 0;

        explicit TStoppingHandler(EStopFromResponseScenario scenario)
            : Scenario(scenario)
        {}

        void HandleResponse(
            TClientRequestPtr req,
            ui32 status,
            size_t responseBytes) override
        {
            Y_UNUSED(req, responseBytes);
            Y_ABORT_UNLESS(status == static_cast<ui32>(RDMA_PROTO_OK));

            switch (Scenario) {
                case EStopFromResponseScenario::ClientRequest:
                    Client->RequestStop();
                    Client->RequestStop();
                    break;
                case EStopFromResponseScenario::ClientStop:
                    Client->Stop();
                    Client->Stop();
                    break;
                case EStopFromResponseScenario::EndpointRequest:
                    Endpoint->RequestStop();
                    Endpoint->RequestStop();
                    break;
                case EStopFromResponseScenario::EndpointAndWait:
                    try {
                        Endpoint->Stop().GetValueSync();
                        ErrorCode = S_OK;
                    } catch (const TServiceError& e) {
                        ErrorCode = e.GetCode();
                    }
                    break;
            }
            Returned.Signal();
        }
    };

    auto handler = std::make_shared<TStoppingHandler>(scenario);

    client->Start();
    auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();
    handler->Client = client;
    handler->Endpoint = endpoint;

    auto request = endpoint->AllocateRequest(
        handler,
        std::make_unique<TNullContext>(),
        1024,
        1024);
    Y_ABORT_UNLESS(!HasError(request.GetError()));

    endpoint->SendRequest(
        request.ExtractResult(),
        MakeIntrusive<TCallContextBase>(0u));
    Y_ABORT_UNLESS(requestSent.WaitT(5s));

    {
        auto guard = Guard(testContext->CompletionLock);
        Y_ABORT_UNLESS(testContext->ReqIds.size() == 1);
        Y_ABORT_UNLESS(testContext->RecvEvents.size() == 1);

        auto* recv = testContext->RecvEvents.front();
        testContext->RecvEvents.pop_front();

        auto* response = reinterpret_cast<TResponseMessage*>(
            recv->sg_list[0].addr);
        Zero(*response);
        InitMessageHeader(response, RDMA_PROTO_VERSION);
        response->ReqId = testContext->ReqIds.front();
        testContext->ReqIds.pop_front();

        testContext->ProcessedRecvEvents.push_back(recv);
        testContext->CompletionHandle.Set();
    }

    Y_ABORT_UNLESS(handler->Returned.WaitT(5s));

    if (scenario == EStopFromResponseScenario::ClientRequest ||
        scenario == EStopFromResponseScenario::ClientStop)
    {
        try {
            client->StartEndpoint("::", 10020).ExtractValueSync();
            Y_ABORT("endpoint unexpectedly started after client stop request");
        } catch (const TServiceError& e) {
            Y_ABORT_UNLESS(e.GetCode() == E_RDMA_UNAVAILABLE);
        }
        client->Stop();
        client->Stop();
    } else {
        if (scenario == EStopFromResponseScenario::EndpointAndWait) {
            Y_ABORT_UNLESS(handler->ErrorCode == E_INVALID_STATE);
        }
        NVerbs::Flush(testContext);
        endpoint->Stop().GetValueSync();
        endpoint->Stop().GetValueSync();
        client->Stop();
    }

    handler->Client.reset();
    handler->Endpoint.reset();
    endpoint.reset();
    client.reset();
}

void TestStopFromResponseInSubprocess(
    EStopFromResponseScenario scenario,
    EWaitMode waitMode)
{
    const pid_t pid = fork();
    ASSERT_GE(pid, 0);

    if (pid == 0) {
        RunStopFromResponseScenario(scenario, waitMode);
        _exit(0);
    }

    int status = 0;
    const auto deadline = TInstant::Now() + TDuration::Seconds(15);
    while (TInstant::Now() < deadline) {
        const pid_t result = waitpid(pid, &status, WNOHANG);
        if (result < 0) {
            kill(pid, SIGKILL);
            waitpid(pid, &status, 0);
            FAIL() << "waitpid failed";
            return;
        }
        if (result == pid) {
            ASSERT_TRUE(WIFEXITED(status)) << status;
            ASSERT_EQ(0, WEXITSTATUS(status));
            return;
        }
        std::this_thread::sleep_for(10ms);
    }

    ASSERT_EQ(0, kill(pid, SIGKILL));
    ASSERT_EQ(pid, waitpid(pid, &status, 0));
    FAIL() << "shutdown scenario timed out";
}

TEST(TRdmaClientTest, ShouldRequestClientStopFromResponseCallback)
{
    for (auto waitMode: {EWaitMode::Poll, EWaitMode::BusyWait}) {
        TestStopFromResponseInSubprocess(
            EStopFromResponseScenario::ClientRequest,
            waitMode);
    }
}

TEST(TRdmaClientTest, ShouldRejectClientStopFromResponseCallback)
{
    for (auto waitMode: {EWaitMode::Poll, EWaitMode::BusyWait}) {
        TestStopFromResponseInSubprocess(
            EStopFromResponseScenario::ClientStop,
            waitMode);
    }
}

TEST(TRdmaClientTest, ShouldRequestEndpointStopFromResponseCallback)
{
    for (auto waitMode: {EWaitMode::Poll, EWaitMode::BusyWait}) {
        TestStopFromResponseInSubprocess(
            EStopFromResponseScenario::EndpointRequest,
            waitMode);
    }
}

TEST(TRdmaClientTest, ShouldRejectEndpointStopWaitFromResponseCallback)
{
    for (auto waitMode: {EWaitMode::Poll, EWaitMode::BusyWait}) {
        TestStopFromResponseInSubprocess(
            EStopFromResponseScenario::EndpointAndWait,
            waitMode);
    }
}

void TestDeferredResponsePreventsReconnect(
    bool abortRequest,
    EWaitMode waitMode)
{
    constexpr ui32 QueueSize = 2;
    const TString RequestMarker = "deferred request";
    const TString ResponseMarker = "deferred response";

    auto testContext = MakeIntrusive<NVerbs::TTestContext>();
    testContext->AllowConnect = true;

    std::atomic<ui32> qpCount = 0;
    TManualEvent firstQpCreated;
    TManualEvent secondQpCreated;
    testContext->CreateQP = [&](auto* id, auto* attr) {
        Y_UNUSED(id, attr);
        switch (qpCount.fetch_add(1) + 1) {
            case 1:
                firstQpCreated.Signal();
                break;
            case 2:
                secondQpCreated.Signal();
                break;
        }
    };

    TManualEvent requestSent;
    testContext->PostSend = [&](auto* qp, auto* wr) {
        PostSend<TRequestMessage>(testContext, qp, wr);
        requestSent.Signal();
    };

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->WaitMode = waitMode;
    clientConfig->SendQueueSize = QueueSize;
    clientConfig->RecvQueueSize = QueueSize;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});
    auto client = CreateTestClient(
        verbs,
        logging,
        monitoring,
        clientConfig);

    struct TDeferredHandler final: IClientHandler
    {
        TMutex Lock;
        TClientRequestPtr DeferredRequest;
        ui32 Status = 0;
        size_t ResponseBytes = 0;
        TManualEvent Handled;

        void HandleResponse(
            TClientRequestPtr req,
            ui32 status,
            size_t responseBytes) override
        {
            {
                auto guard = Guard(Lock);
                DeferredRequest = std::move(req);
                Status = status;
                ResponseBytes = responseBytes;
            }
            Handled.Signal();
        }

        TString ReadRequest(size_t bytes)
        {
            auto guard = Guard(Lock);
            Y_ABORT_UNLESS(DeferredRequest);
            return TString(DeferredRequest->RequestBuffer.Head(bytes));
        }

        TString ReadResponse(size_t bytes)
        {
            auto guard = Guard(Lock);
            Y_ABORT_UNLESS(DeferredRequest);
            return TString(DeferredRequest->ResponseBuffer.Head(bytes));
        }

        std::pair<ui32, size_t> GetResult()
        {
            auto guard = Guard(Lock);
            return {Status, ResponseBytes};
        }

        void Release()
        {
            TClientRequestPtr request;
            {
                auto guard = Guard(Lock);
                request = std::move(DeferredRequest);
            }
        }
    };

    auto handler = std::make_shared<TDeferredHandler>();
    client->Start();
    Y_DEFER
    {
        handler->Release();
        client->Stop();
    };

    auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();
    ASSERT_TRUE(firstQpCreated.WaitT(5s));

    auto request = endpoint->AllocateRequest(
        handler,
        std::make_unique<TNullContext>(),
        1024,
        1024);
    ASSERT_FALSE(HasError(request.GetError()));

    auto clientRequest = request.ExtractResult();
    std::memcpy(
        const_cast<char*>(clientRequest->RequestBuffer.data()),
        RequestMarker.data(),
        RequestMarker.size());
    std::memcpy(
        const_cast<char*>(clientRequest->ResponseBuffer.data()),
        ResponseMarker.data(),
        ResponseMarker.size());

    endpoint->SendRequest(
        std::move(clientRequest),
        MakeIntrusive<TCallContextBase>(0u));
    ASSERT_TRUE(requestSent.WaitT(5s));

    if (abortRequest) {
        Disconnect(testContext);
    } else {
        auto guard = Guard(testContext->CompletionLock);
        ASSERT_FALSE(testContext->ReqIds.empty());
        ASSERT_FALSE(testContext->RecvEvents.empty());

        auto* recv = testContext->RecvEvents.front();
        testContext->RecvEvents.pop_front();

        auto* response = reinterpret_cast<TResponseMessage*>(
            recv->sg_list[0].addr);
        Zero(*response);
        InitMessageHeader(response, RDMA_PROTO_VERSION);
        response->ReqId = testContext->ReqIds.front();
        response->Status = RDMA_PROTO_OK;
        response->ResponseBytes = ResponseMarker.size();
        testContext->ReqIds.pop_front();

        testContext->ProcessedRecvEvents.push_back(recv);
        testContext->CompletionHandle.Set();
    }

    ASSERT_TRUE(handler->Handled.WaitT(5s));
    if (!abortRequest) {
        Disconnect(testContext);
    }
    endpoint->TryForceReconnect();

    ASSERT_FALSE(secondQpCreated.WaitT(500ms));
    ASSERT_EQ(RequestMarker, handler->ReadRequest(RequestMarker.size()));

    const auto [status, responseBytes] = handler->GetResult();
    if (abortRequest) {
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), status);
        const auto error = ParseError(handler->ReadResponse(responseBytes));
        ASSERT_EQ(E_RDMA_UNAVAILABLE, error.GetCode());
    } else {
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_OK), status);
        ASSERT_EQ(ResponseMarker.size(), responseBytes);
        ASSERT_EQ(
            ResponseMarker,
            handler->ReadResponse(ResponseMarker.size()));
    }

    handler->Release();
    endpoint->TryForceReconnect();
    ASSERT_TRUE(secondQpCreated.WaitT(5s));
}

TEST(TRdmaClientTest, ShouldKeepCompletedRequestBuffersUntilRequestIsReleased)
{
    TestDeferredResponsePreventsReconnect(false, EWaitMode::Poll);
}

TEST(TRdmaClientTest, ShouldKeepAbortedRequestBuffersUntilRequestIsReleased)
{
    TestDeferredResponsePreventsReconnect(true, EWaitMode::BusyWait);
}

TEST(TRdmaClientTest, ShouldCancelRequests)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client =
            CreateTestClient(verbs, logging, monitoring, clientConfig);

        client->Start();
        Y_DEFER
        {
            client->Stop();
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

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

        auto request1 = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(response1, ev1),
            requestBytes,
            responseBytes);
        ASSERT_FALSE(HasError(request1.GetError()));

        auto reqId1 = endpoint->SendRequest(
            request1.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        TManualEvent ev2;
        TResponse response2;

        auto request2 = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(response2, ev2),
            requestBytes,
            responseBytes);
        ASSERT_FALSE(HasError(request2.GetError()));

        auto reqId2 = endpoint->SendRequest(
            request2.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        endpoint->CancelRequest(reqId2);

        ev2.Wait();
        ASSERT_TRUE(response2.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), response2.Status);

        NProto::TError error2 =
            ParseError(response2.Buffer.Head(response2.Bytes));
        ASSERT_EQ(E_CANCELLED, error2.GetCode());

        ASSERT_FALSE(response1.Received);
        endpoint->CancelRequest(reqId1);

        ev1.Wait();
        ASSERT_TRUE(response1.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), response1.Status);

        NProto::TError error1 =
            ParseError(response1.Buffer.Head(response1.Bytes));
        ASSERT_EQ(E_CANCELLED, error1.GetCode());

        auto counters = GetClientCounters(monitoring);
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

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateTestClient(
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

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr) {
            PostSend<TRequestMessage>(testContext, qp, wr);
        };

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

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(),
            requestBytes,
            responseBytes);
        ASSERT_FALSE(HasError(request.GetError()));

        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

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

        ev.Wait();
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
        clientConfig->MaxReconnectDelay = 5s;

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client =
            CreateTestClient(verbs, logging, monitoring, clientConfig);

        client->Start();
        Y_DEFER
        {
            client->Stop();
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        // Increase reconnect timer delay up to "MaxReconnectDelay".
        for (int i = 0; i < 10; i++) {
            Disconnect(testContext);
        }

        const auto now = TInstant::Now();
        endpoint->TryForceReconnect();

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

        auto client = CreateTestClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        auto counters = GetClientCounters(monitoring);
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

        while (errors->Val() != 7 || active->Val() != 6) {
            SpinLockPause();
        }
}

TEST(TRdmaClientTest, ShouldNegotiateProtocolVersionFromAcceptMessage)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->MaxReconnectDelay = 5s;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto client = CreateTestClient(verbs, logging, monitoring, clientConfig);

    client->Start();
    Y_DEFER
    {
        client->Stop();
    };

    std::atomic<int> acceptedConnectVersion = 0;

    testContext->HandleConnect = [&](auto* id, auto* param)
    {
        acceptedConnectVersion = ParseMessageHeader(param->private_data);

        TAcceptMessage acceptMsg{};
        InitMessageHeader(&acceptMsg, RDMA_PROTO_PREV_VERSION);
        NVerbs::EnqueueAcceptEvent(
            testContext,
            id,
            &acceptMsg,
            sizeof(acceptMsg));
    };

    auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();
    ASSERT_TRUE(endpoint);

    // first connect attempt is sent with the current protocol version
    ASSERT_EQ(RDMA_PROTO_VERSION, acceptedConnectVersion.load());

    // after a successful connect, the request message must be encoded with
    // the negotiated (previous) protocol version
    TManualEvent sent;
    std::atomic<int> sentVersion = 0;
    testContext->PostSend = [&](auto* qp, auto* wr)
    {
        Y_UNUSED(qp);
        with_lock (testContext->CompletionLock) {
            const auto* msg =
                reinterpret_cast<TRequestMessage*>(wr->sg_list[0].addr);
            sentVersion = ParseMessageHeader(msg);
        }
        sent.Signal();
    };

    auto request = endpoint->AllocateRequest(
        std::make_shared<TClientHandler>(),
        std::make_unique<TNullContext>(),
        1024,
        1024);
    ASSERT_FALSE(HasError(request.GetError()));

    endpoint->SendRequest(
        request.ExtractResult(),
        MakeIntrusive<TCallContextBase>(0u));

    sent.Wait();
    ASSERT_EQ(RDMA_PROTO_PREV_VERSION, sentVersion.load());
}

TEST(TRdmaClientTest, ShouldDisconnectOnUnsupportedProtocolVersionInAccept)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->MaxReconnectDelay = 5s;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto client = CreateTestClient(verbs, logging, monitoring, clientConfig);

    client->Start();
    Y_DEFER
    {
        client->Stop();
    };

    std::atomic<int> connectAttempts = 0;
    testContext->HandleConnect = [&](auto* id, auto* param)
    {
        Y_UNUSED(param);
        ++connectAttempts;

        // accept message with an unsupported protocol version (newer than
        // the current one)
        TAcceptMessage acceptMsg{};
        InitMessageHeader(&acceptMsg, RDMA_PROTO_VERSION + 1);
        NVerbs::EnqueueAcceptEvent(
            testContext,
            id,
            &acceptMsg,
            sizeof(acceptMsg));
    };

    try {
        client->StartEndpoint("::", 10020).ExtractValueSync();
        FAIL() << "expected exception";
    } catch (const TServiceError& e) {
        ASSERT_EQ(E_RDMA_UNAVAILABLE, e.GetCode()) << e.GetMessage();
    }

    // there should have been at least one connect attempt that was rejected
    // due to the unsupported version
    ASSERT_GE(connectAttempts.load(), 1);
}

TEST(TRdmaClientTest, ShouldDowngradeProtocolVersionOnRejection)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->MaxReconnectDelay = 5s;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto client = CreateTestClient(verbs, logging, monitoring, clientConfig);

    client->Start();
    Y_DEFER
    {
        client->Stop();
    };

    std::atomic<int> connectAttempts = 0;
    std::atomic<int> firstConnectVersion = 0;
    std::atomic<int> secondConnectVersion = 0;

    testContext->HandleConnect = [&](auto* id, auto* param)
    {
        const int version = ParseMessageHeader(param->private_data);
        const int attempt = ++connectAttempts;

        if (attempt == 1) {
            firstConnectVersion = version;
            TRejectMessage rejectMsg{};
            InitMessageHeader(&rejectMsg, RDMA_PROTO_VERSION_1);
            rejectMsg.Status = SafeCast<ui16>(RDMA_PROTO_INVALID_REQUEST);
            rejectMsg.QueueSize = SafeCast<ui16>(clientConfig->QueueSize);
            rejectMsg.MaxBufferSize =
                SafeCast<ui32>(clientConfig->MaxBufferSize);
            NVerbs::EnqueueRejectEvent(
                testContext,
                id,
                &rejectMsg,
                sizeof(rejectMsg));
            return;
        }

        if (attempt == 2) {
            secondConnectVersion = version;
        }

        TAcceptMessage acceptMsg{};
        InitMessageHeader(&acceptMsg, version);
        NVerbs::EnqueueAcceptEvent(
            testContext,
            id,
            &acceptMsg,
            sizeof(acceptMsg));
    };

    auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();
    ASSERT_TRUE(endpoint);

    ASSERT_EQ(RDMA_PROTO_VERSION, firstConnectVersion.load());
    ASSERT_EQ(RDMA_PROTO_PREV_VERSION, secondConnectVersion.load());
}

TEST(TRdmaClientTest, ShouldAdjustQueueSizeOnConfigMismatchInRejection)
{
    constexpr ui16 ServerRecvQueueSize = 4;
    constexpr ui16 ServerSendQueueSize = 8;

    auto testContext = MakeIntrusive<NVerbs::TTestContext>();

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->SendQueueSize = 16;
    clientConfig->RecvQueueSize = 4;
    clientConfig->MaxReconnectDelay = 5s;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto client = CreateTestClient(verbs, logging, monitoring, clientConfig);

    client->Start();
    Y_DEFER
    {
        client->Stop();
    };

    std::atomic<int> connectAttempts = 0;
    std::atomic<ui16> firstSendQueueSize = 0;
    std::atomic<ui16> firstRecvQueueSize = 0;
    std::atomic<ui16> secondSendQueueSize = 0;
    std::atomic<ui16> secondRecvQueueSize = 0;

    testContext->HandleConnect = [&](auto* id, auto* param)
    {
        const auto* connectMsg =
            static_cast<const TConnectMessage*>(param->private_data);
        const int attempt = ++connectAttempts;

        if (attempt == 1) {
            firstSendQueueSize = SafeCast<ui16>(connectMsg->SendQueueSize);
            firstRecvQueueSize = SafeCast<ui16>(connectMsg->RecvQueueSize);

            TRejectMessage2 rejectMsg{};
            InitMessageHeader(&rejectMsg, RDMA_PROTO_VERSION);
            rejectMsg.Status = SafeCast<ui16>(RDMA_PROTO_CONFIG_MISMATCH);
            rejectMsg.SendQueueSize = ServerSendQueueSize;
            rejectMsg.RecvQueueSize = ServerRecvQueueSize;
            rejectMsg.MaxBufferSize =
                SafeCast<ui32>(clientConfig->MaxBufferSize);
            NVerbs::EnqueueRejectEvent(
                testContext,
                id,
                &rejectMsg,
                sizeof(rejectMsg));
            return;
        }

        if (attempt == 2) {
            secondSendQueueSize = SafeCast<ui16>(connectMsg->SendQueueSize);
            secondRecvQueueSize = SafeCast<ui16>(connectMsg->RecvQueueSize);
        }

        TAcceptMessage acceptMsg{};
        InitMessageHeader(&acceptMsg, RDMA_PROTO_VERSION);
        NVerbs::EnqueueAcceptEvent(
            testContext,
            id,
            &acceptMsg,
            sizeof(acceptMsg));
    };

    auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();
    ASSERT_TRUE(endpoint);

    // initial attempt uses configured queue sizes
    ASSERT_EQ(clientConfig->SendQueueSize, firstSendQueueSize.load());
    ASSERT_EQ(clientConfig->RecvQueueSize, firstRecvQueueSize.load());

    // after the V2 config-mismatch reject the client must:
    //   SendQueueSize = msg->RecvQueueSize / 2
    //   RecvQueueSize = msg->SendQueueSize * 2
    ASSERT_EQ(
        static_cast<ui16>(ServerRecvQueueSize / 2),
        secondSendQueueSize.load());
    ASSERT_EQ(
        static_cast<ui16>(ServerSendQueueSize * 2),
        secondRecvQueueSize.load());
}

TEST(TRdmaClientTest, ShouldDisconnectOnUnknownProtocolVersionInRejectMessage)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->MaxReconnectDelay = 5s;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto client = CreateTestClient(verbs, logging, monitoring, clientConfig);

    client->Start();
    Y_DEFER
    {
        client->Stop();
    };

    std::atomic<int> connectAttempts = 0;
    testContext->HandleConnect = [&](auto* id, auto* param)
    {
        Y_UNUSED(param);
        ++connectAttempts;

        // synthesize a reject message with a totally unknown version that
        // doesn't match any switch branch in HandleRejected
        TRejectMessage2 rejectMsg{};
        InitMessageHeader(&rejectMsg, RDMA_PROTO_VERSION + 5);
        NVerbs::EnqueueRejectEvent(
            testContext,
            id,
            &rejectMsg,
            sizeof(rejectMsg));
    };

    try {
        client->StartEndpoint("::", 10020).ExtractValueSync();
        FAIL() << "expected exception";
    } catch (const TServiceError& e) {
        ASSERT_EQ(E_RDMA_UNAVAILABLE, e.GetCode()) << e.GetMessage();
    }

    ASSERT_GE(connectAttempts.load(), 1);
}

}   // namespace NCloud::NStorage::NRdma
