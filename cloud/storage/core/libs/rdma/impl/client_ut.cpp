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
#include <util/stream/printf.h>

#include <thread>

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

TEST(TRdmaClientTest, ShouldInvalidateMemoryWindowsOnSuccessButNotOnTimeout)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->MaxResponseDelay = 1s;

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

        testContext->HandleConnect = [&](auto* id, auto* param)
        {
            Y_UNUSED(param);

            TAcceptMessage acceptMsg{};
            InitMessageHeader(&acceptMsg, RDMA_PROTO_VERSION);
            acceptMsg.Unused = RDMA_ACCEPT_FLAG_NONE;
            NVerbs::EnqueueAcceptEvent(
                testContext,
                id,
                &acceptMsg,
                sizeof(acceptMsg));
        };

        std::atomic<size_t> localInvalidationsPosted = 0;
        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            for (auto* current = wr; current; current = current->next) {
                if (current->opcode == IBV_WR_LOCAL_INV) {
                    localInvalidationsPosted.fetch_add(1);
                }
            }

            PostSend<TRequestMessage>(testContext, qp, wr);
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

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

        auto completeOneRequest = [&](ui32 status)
        {
            while (true) {
                with_lock (testContext->CompletionLock) {
                    if (testContext->RecvEvents && testContext->ReqIds) {
                        auto* re = testContext->RecvEvents.front();
                        auto* responseMsg = reinterpret_cast<TResponseMessage*>(
                            re->sg_list[0].addr);
                        Zero(*responseMsg);
                        InitMessageHeader(responseMsg, RDMA_PROTO_VERSION);
                        responseMsg->ReqId = testContext->ReqIds.front();
                        responseMsg->Status = status;
                        responseMsg->ResponseBytes = 0;

                        testContext->ReqIds.pop_front();
                        testContext->RecvEvents.pop_front();
                        testContext->ProcessedRecvEvents.push_back(re);
                        testContext->CompletionHandle.Set();
                        return;
                    }
                }
            }
        };

        // Happy path: request completes and local invalidation is posted for
        // both In/Out windows before the handler is called.
        TManualEvent ev1;
        TResponse response1;
        auto request1 = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(&ev1, &response1),
            4096,
            4096);
        ASSERT_FALSE(HasError(request1.GetError()));

        endpoint->SendRequest(
            request1.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));
        completeOneRequest(RDMA_PROTO_OK);

        ASSERT_TRUE(ev1.WaitT(clientConfig->MaxResponseDelay + 1s));
        ASSERT_TRUE(response1.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_OK), response1.Status);
        ASSERT_EQ(2u, localInvalidationsPosted.load());

        // Timeout path: request is aborted, so windows are not recycled and no
        // local invalidation is posted for this timed out request.
        TManualEvent ev2;
        TResponse response2;
        auto request2 = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(&ev2, &response2),
            4096,
            4096);
        ASSERT_FALSE(HasError(request2.GetError()));

        endpoint->SendRequest(
            request2.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        ASSERT_TRUE(ev2.WaitT(clientConfig->MaxResponseDelay + 1s));
        ASSERT_TRUE(response2.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), response2.Status);
        NProto::TError error = ParseError(response2.Buffer.Head(response2.Bytes));
        ASSERT_EQ(E_TIMEOUT, error.GetCode());
        ASSERT_EQ(2u, localInvalidationsPosted.load());

        // Drain delayed response from test transport.
        completeOneRequest(RDMA_PROTO_OK);
}

TEST(TRdmaClientTest, ShouldKeepInvalidationResourcesUntilQpDestroy)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 100ms;
        clientConfig->FlushTimeout = 5s;

        auto client = CreateTestClient(
            NVerbs::CreateTestVerbs(testContext),
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES}),
            CreateMonitoringServiceStub(),
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        std::atomic<ui32> invalidatedRKey1 = 0;
        std::atomic<ui32> invalidatedRKey2 = 0;
        std::atomic<bool> qpDestroyed = false;
        std::atomic<size_t> prematureMwDestructions = 0;
        TManualEvent invalidationPosted;
        TManualEvent qpDestroyedEvent;

        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            if (wr->opcode == IBV_WR_LOCAL_INV) {
                invalidatedRKey1 = wr->invalidate_rkey;
                if (wr->next) {
                    invalidatedRKey2 = wr->next->invalidate_rkey;
                }
                // Keep LOCAL_INV in the simulated SQ without producing CQE.
                invalidationPosted.Signal();
                return;
            }
            PostSend<TRequestMessage>(testContext, qp, wr);
        };
        testContext->DestroyQP = [&](rdma_cm_id*) {
            qpDestroyed = true;
            qpDestroyedEvent.Signal();
        };
        testContext->DestroyMemoryWindow = [&](ibv_mw* mw) {
            Y_UNUSED(mw);
            if (!qpDestroyed.load()) {
                ++prematureMwDestructions;
            }
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        TManualEvent responseReceived;
        auto ctx = std::make_unique<TRequestContext>();
        ctx->Handler = [&](TStringBuf, TStringBuf, ui32, size_t) {
            responseReceived.Signal();
        };

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            std::move(ctx),
            4096,
            4096);
        ASSERT_FALSE(HasError(request.GetError()));
        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        while (true) {
            with_lock (testContext->CompletionLock) {
                if (testContext->RecvEvents && testContext->ReqIds) {
                    auto* recv = testContext->RecvEvents.front();
                    auto* msg = reinterpret_cast<TResponseMessage*>(
                        recv->sg_list[0].addr);
                    Zero(*msg);
                    InitMessageHeader(msg, RDMA_PROTO_VERSION);
                    msg->ReqId = testContext->ReqIds.front();
                    msg->Status = RDMA_PROTO_OK;

                    testContext->ReqIds.pop_front();
                    testContext->RecvEvents.pop_front();
                    testContext->ProcessedRecvEvents.push_back(recv);
                    testContext->CompletionHandle.Set();
                    break;
                }
            }
        }

        ASSERT_TRUE(invalidationPosted.WaitT(5s));
        NVerbs::Disconnect(testContext);
        ASSERT_TRUE(responseReceived.WaitT(5s));

        // Retired LOCAL_INV owns the missing send slot, so teardown must not
        // wait for FlushTimeout when its terminal CQE never arrives.
        ASSERT_TRUE(qpDestroyedEvent.WaitT(1s));
        ASSERT_EQ(0u, prematureMwDestructions.load());
}

TEST(TRdmaClientTest, ShouldDeferCancelUntilTerminalSendCompletionWithMemoryWindows)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->MaxResponseDelay = 1s;

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

        TManualEvent sendPosted;
        ibv_send_wr* delayedSend = nullptr;

        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            Y_UNUSED(qp);

            with_lock (testContext->CompletionLock) {
                for (auto* current = wr; current; current = current->next) {
                    auto* copy = new ibv_send_wr(*current);
                    copy->next = nullptr;

                    if (current->opcode == IBV_WR_SEND) {
                        delayedSend = copy;
                        sendPosted.Signal();
                    } else {
                        testContext->SendEvents.push_back(copy);
                    }
                }

                if (testContext->SendEvents) {
                    testContext->CompletionHandle.Set();
                }
            }
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent done;
        TResponse response;
        auto ctx = std::make_unique<TRequestContext>();
        ctx->Handler = [&](TStringBuf requestBuffer,
                           TStringBuf responseBuffer,
                           ui32 status,
                           size_t responseBytes)
        {
            Y_UNUSED(requestBuffer);
            response = TResponse{true, responseBuffer, status, responseBytes};
            done.Signal();
        };

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            std::move(ctx),
            4096,
            4096);
        ASSERT_FALSE(HasError(request.GetError()));

        auto reqId = endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        ASSERT_TRUE(sendPosted.WaitT(5s));

        endpoint->CancelRequest(reqId);
        ASSERT_FALSE(done.WaitT(200ms));

        {
            with_lock (testContext->CompletionLock) {
                ASSERT_NE(nullptr, delayedSend);
                testContext->SendEvents.push_back(delayedSend);
                delayedSend = nullptr;
                testContext->CompletionHandle.Set();
            }
        }

        ASSERT_TRUE(done.WaitT(clientConfig->MaxResponseDelay + 2s));
        ASSERT_TRUE(response.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), response.Status);

        NProto::TError error = ParseError(response.Buffer.Head(response.Bytes));
        ASSERT_EQ(E_CANCELLED, error.GetCode());
}

TEST(TRdmaClientTest, ShouldDeferTimeoutUntilTerminalSendCompletionWithMemoryWindows)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->MaxResponseDelay = 1s;

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

        TManualEvent sendPosted;
        ibv_send_wr* delayedSend = nullptr;

        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            Y_UNUSED(qp);

            with_lock (testContext->CompletionLock) {
                for (auto* current = wr; current; current = current->next) {
                    auto* copy = new ibv_send_wr(*current);
                    copy->next = nullptr;

                    if (current->opcode == IBV_WR_SEND) {
                        delayedSend = copy;
                        sendPosted.Signal();
                    } else {
                        testContext->SendEvents.push_back(copy);
                    }
                }

                if (testContext->SendEvents) {
                    testContext->CompletionHandle.Set();
                }
            }
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent done;
        TResponse response;
        auto ctx = std::make_unique<TRequestContext>();
        ctx->Handler = [&](TStringBuf requestBuffer,
                           TStringBuf responseBuffer,
                           ui32 status,
                           size_t responseBytes)
        {
            Y_UNUSED(requestBuffer);
            response = TResponse{true, responseBuffer, status, responseBytes};
            done.Signal();
        };

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            std::move(ctx),
            4096,
            4096);
        ASSERT_FALSE(HasError(request.GetError()));

        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        ASSERT_TRUE(sendPosted.WaitT(5s));

        // Timeout should fire while the request is still in SendRequest state,
        // but callback must stay deferred until terminal SEND completion.
        ASSERT_FALSE(done.WaitT(clientConfig->MaxResponseDelay + 200ms));

        {
            with_lock (testContext->CompletionLock) {
                ASSERT_NE(nullptr, delayedSend);
                testContext->SendEvents.push_back(delayedSend);
                delayedSend = nullptr;
                testContext->CompletionHandle.Set();
            }
        }

        ASSERT_TRUE(done.WaitT(clientConfig->MaxResponseDelay + 2s));
        ASSERT_TRUE(response.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), response.Status);

        NProto::TError error = ParseError(response.Buffer.Head(response.Bytes));
        ASSERT_EQ(E_TIMEOUT, error.GetCode());
}

TEST(TRdmaClientTest, ShouldAbortOnFirstSendFlushCompletionWithMemoryWindows)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->MaxResponseDelay = 30s;

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

        TManualEvent sendPosted;
        ibv_send_wr* delayedSend = nullptr;

        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            Y_UNUSED(qp);

            with_lock (testContext->CompletionLock) {
                for (auto* current = wr; current; current = current->next) {
                    auto* copy = new ibv_send_wr(*current);
                    copy->next = nullptr;

                    if (current->opcode == IBV_WR_SEND) {
                        delayedSend = copy;
                        sendPosted.Signal();
                    } else {
                        testContext->SendEvents.push_back(copy);
                    }
                }

                if (testContext->SendEvents) {
                    testContext->CompletionHandle.Set();
                }
            }
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent done;
        TResponse response;
        auto ctx = std::make_unique<TRequestContext>();
        ctx->Handler = [&](TStringBuf requestBuffer,
                           TStringBuf responseBuffer,
                           ui32 status,
                           size_t responseBytes)
        {
            Y_UNUSED(requestBuffer);
            response = TResponse{true, responseBuffer, status, responseBytes};
            done.Signal();
        };

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            std::move(ctx),
            4096,
            4096);
        ASSERT_FALSE(HasError(request.GetError()));

        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        ASSERT_TRUE(sendPosted.WaitT(5s));

        const ui64 delayedSendWrId = delayedSend->wr_id;
        testContext->HandleCompletionEvent = [&](ibv_wc* wc)
        {
            if (wc->wr_id == delayedSendWrId &&
                wc->opcode == IBV_WC_SEND)
            {
                wc->status = IBV_WC_WR_FLUSH_ERR;
            }
        };

        {
            with_lock (testContext->CompletionLock) {
                ASSERT_NE(nullptr, delayedSend);
                testContext->SendEvents.push_back(delayedSend);
                delayedSend = nullptr;
                testContext->CompletionHandle.Set();
            }
        }

        ASSERT_TRUE(done.WaitT(5s));
        ASSERT_TRUE(response.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), response.Status);

        NProto::TError error = ParseError(response.Buffer.Head(response.Bytes));
        ASSERT_EQ(E_RDMA_UNAVAILABLE, error.GetCode());
}

TEST(TRdmaClientTest, ShouldKeepResourcesUntilQpDestroyOnPartialPostSend)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->FlushTimeout = 1s;

        auto client = CreateTestClient(
            NVerbs::CreateTestVerbs(testContext),
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES}),
            CreateMonitoringServiceStub(),
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        std::atomic<bool> failPartially = true;
        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            if (failPartially.load() && wr->next) {
                // Emulate a provider which accepted the first BIND_MW only.
                with_lock (testContext->CompletionLock) {
                    auto* accepted = new ibv_send_wr(*wr);
                    accepted->next = nullptr;
                    testContext->SendEvents.push_back(accepted);
                    testContext->CompletionHandle.Set();
                }
                return;
            }
            PostSend<TRequestMessage>(testContext, qp, wr);
        };
        testContext->GetBadSendWr = [&](ibv_send_wr* wr)
        {
            if (failPartially.exchange(false)) {
                return wr->next;
            }
            return static_cast<ibv_send_wr*>(nullptr);
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();

        TManualEvent done;
        ui32 status = 0;
        NProto::TError error;
        auto ctx = std::make_unique<TRequestContext>();
        ctx->Handler = [&](TStringBuf requestBuffer,
                           TStringBuf responseBuffer,
                           ui32 responseStatus,
                           size_t responseBytes)
        {
            Y_UNUSED(requestBuffer);
            status = responseStatus;
            error = ParseError(responseBuffer.Head(responseBytes));
            done.Signal();
        };

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            std::move(ctx),
            4096,
            4096);
        ASSERT_FALSE(HasError(request.GetError()));

        endpoint->SendRequest(
            request.ExtractResult(),
            MakeIntrusive<TCallContextBase>(0u));

        ASSERT_TRUE(done.WaitT(5s));
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_FAIL), status);
        ASSERT_EQ(E_RDMA_UNAVAILABLE, error.GetCode());
}

TEST(TRdmaClientTest, ShouldUseSendWithInvalidateForOutMemoryWindow)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->MaxResponseDelay = 1s;

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

        testContext->HandleConnect = [&](auto* id, auto* param)
        {
            Y_UNUSED(param);

            TAcceptMessage acceptMsg{};
            InitMessageHeader(&acceptMsg, RDMA_PROTO_VERSION);
            acceptMsg.Unused = RDMA_ACCEPT_FLAG_SEND_WITH_INV;
            NVerbs::EnqueueAcceptEvent(
                testContext,
                id,
                &acceptMsg,
                sizeof(acceptMsg));
        };

        std::atomic<ui32> expectedOutRKey = 0;
        std::atomic<size_t> localInvalidationsPosted = 0;
        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            for (auto* current = wr; current; current = current->next) {
                if (current->opcode == IBV_WR_LOCAL_INV) {
                    localInvalidationsPosted.fetch_add(1);
                }

                if (current->opcode == IBV_WR_SEND &&
                    current->sg_list &&
                    current->num_sge > 0)
                {
                    const auto* msg = reinterpret_cast<TRequestMessage*>(
                        current->sg_list[0].addr);
                    expectedOutRKey.store(msg->Out.RKey);
                }
            }

            PostSend<TRequestMessage>(testContext, qp, wr);
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();
        testContext->HandleCompletionEvent = [&](ibv_wc* wc)
        {
            if (wc->opcode == IBV_WC_RECV) {
                wc->wc_flags |= IBV_WC_WITH_INV;
                wc->invalidated_rkey = expectedOutRKey.load();
            }
        };

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent ev;
        TResponse response;
        auto makeContext = [&](TManualEvent* done, TResponse* out)
        {
            auto ctx = std::make_unique<TRequestContext>();
            ctx->Handler = [done, out](
                               TStringBuf requestBuffer,
                               TStringBuf responseBuffer,
                               ui32 status,
                               size_t responseBytes)
            {
                Y_UNUSED(requestBuffer);

                out->Received = true;
                out->Buffer = responseBuffer;
                out->Status = status;
                out->Bytes = responseBytes;

                done->Signal();
            };
            return ctx;
        };

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(&ev, &response),
            4096,
            4096);
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
                    responseMsg->Status = RDMA_PROTO_OK;
                    responseMsg->ResponseBytes = 0;

                    testContext->ReqIds.pop_front();
                    testContext->RecvEvents.pop_front();
                    testContext->ProcessedRecvEvents.push_back(re);
                    testContext->CompletionHandle.Set();
                    break;
                }
            }
        }

        ASSERT_TRUE(ev.WaitT(clientConfig->MaxResponseDelay + 1s));
        ASSERT_TRUE(response.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_OK), response.Status);

        // With SEND_WITH_INV support: In is invalidated locally, Out remotely.
        ASSERT_EQ(1u, localInvalidationsPosted.load());
}

TEST(TRdmaClientTest, ShouldDisableRecycleWhenSendWithInvIsMissing)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->MaxResponseDelay = 1s;

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

        testContext->HandleConnect = [&](auto* id, auto* param)
        {
            Y_UNUSED(param);

            TAcceptMessage acceptMsg{};
            InitMessageHeader(&acceptMsg, RDMA_PROTO_VERSION);
            acceptMsg.Unused = RDMA_ACCEPT_FLAG_SEND_WITH_INV;
            NVerbs::EnqueueAcceptEvent(
                testContext,
                id,
                &acceptMsg,
                sizeof(acceptMsg));
        };

        std::atomic<size_t> localInvalidationsPosted = 0;
        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            for (auto* current = wr; current; current = current->next) {
                if (current->opcode == IBV_WR_LOCAL_INV) {
                    localInvalidationsPosted.fetch_add(1);
                }
            }

            PostSend<TRequestMessage>(testContext, qp, wr);
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();
        // Intentionally do not set IBV_WC_WITH_INV on RECV completions.
        testContext->HandleCompletionEvent = [&](ibv_wc* wc)
        {
            if (wc->opcode == IBV_WC_RECV) {
                wc->wc_flags = 0;
            }
        };

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent ev;
        TResponse response;
        auto makeContext = [&](TManualEvent* done, TResponse* out)
        {
            auto ctx = std::make_unique<TRequestContext>();
            ctx->Handler = [done, out](
                               TStringBuf requestBuffer,
                               TStringBuf responseBuffer,
                               ui32 status,
                               size_t responseBytes)
            {
                Y_UNUSED(requestBuffer);

                out->Received = true;
                out->Buffer = responseBuffer;
                out->Status = status;
                out->Bytes = responseBytes;

                done->Signal();
            };
            return ctx;
        };

        auto counters = GetClientCounters(monitoring);
        auto errors = counters->GetCounter("Errors");
        const auto errorsBefore = errors->Val();

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(&ev, &response),
            4096,
            4096);
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
                    responseMsg->Status = RDMA_PROTO_OK;
                    responseMsg->ResponseBytes = 0;

                    testContext->ReqIds.pop_front();
                    testContext->RecvEvents.pop_front();
                    testContext->ProcessedRecvEvents.push_back(re);
                    testContext->CompletionHandle.Set();
                    break;
                }
            }
        }

        ASSERT_TRUE(ev.WaitT(clientConfig->MaxResponseDelay + 1s));
        ASSERT_TRUE(response.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_OK), response.Status);

        // Missing remote invalidation disables recycle path for this request.
        ASSERT_EQ(0u, localInvalidationsPosted.load());
        ASSERT_GT(errors->Val(), errorsBefore);
}

TEST(TRdmaClientTest, ShouldDisableRecycleOnUnexpectedSendWithInvRKey)
{
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->UseMemoryWindows = true;
        clientConfig->MaxReconnectDelay = 5s;
        clientConfig->MaxResponseDelay = 1s;

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

        testContext->HandleConnect = [&](auto* id, auto* param)
        {
            Y_UNUSED(param);

            TAcceptMessage acceptMsg{};
            InitMessageHeader(&acceptMsg, RDMA_PROTO_VERSION);
            acceptMsg.Unused = RDMA_ACCEPT_FLAG_SEND_WITH_INV;
            NVerbs::EnqueueAcceptEvent(
                testContext,
                id,
                &acceptMsg,
                sizeof(acceptMsg));
        };

        std::atomic<ui32> expectedOutRKey = 0;
        std::atomic<size_t> localInvalidationsPosted = 0;
        testContext->PostSend = [&](ibv_qp* qp, ibv_send_wr* wr)
        {
            for (auto* current = wr; current; current = current->next) {
                if (current->opcode == IBV_WR_LOCAL_INV) {
                    localInvalidationsPosted.fetch_add(1);
                }

                if (current->opcode == IBV_WR_SEND &&
                    current->sg_list &&
                    current->num_sge > 0)
                {
                    const auto* msg = reinterpret_cast<TRequestMessage*>(
                        current->sg_list[0].addr);
                    expectedOutRKey.store(msg->Out.RKey);
                }
            }

            PostSend<TRequestMessage>(testContext, qp, wr);
        };

        auto endpoint = client->StartEndpoint("::", 10020).ExtractValueSync();
        testContext->HandleCompletionEvent = [&](ibv_wc* wc)
        {
            if (wc->opcode == IBV_WC_RECV) {
                wc->wc_flags |= IBV_WC_WITH_INV;
                wc->invalidated_rkey = expectedOutRKey.load() + 1;
            }
        };

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent ev;
        TResponse response;
        auto makeContext = [&](TManualEvent* done, TResponse* out)
        {
            auto ctx = std::make_unique<TRequestContext>();
            ctx->Handler = [done, out](
                               TStringBuf requestBuffer,
                               TStringBuf responseBuffer,
                               ui32 status,
                               size_t responseBytes)
            {
                Y_UNUSED(requestBuffer);

                out->Received = true;
                out->Buffer = responseBuffer;
                out->Status = status;
                out->Bytes = responseBytes;

                done->Signal();
            };
            return ctx;
        };

        auto counters = GetClientCounters(monitoring);
        auto errors = counters->GetCounter("Errors");
        const auto errorsBefore = errors->Val();

        auto request = endpoint->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(&ev, &response),
            4096,
            4096);
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
                    responseMsg->Status = RDMA_PROTO_OK;
                    responseMsg->ResponseBytes = 0;

                    testContext->ReqIds.pop_front();
                    testContext->RecvEvents.pop_front();
                    testContext->ProcessedRecvEvents.push_back(re);
                    testContext->CompletionHandle.Set();
                    break;
                }
            }
        }

        ASSERT_TRUE(ev.WaitT(clientConfig->MaxResponseDelay + 1s));
        ASSERT_TRUE(response.Received);
        ASSERT_EQ(static_cast<ui32>(RDMA_PROTO_OK), response.Status);

        // Wrong invalidated_rkey also disables recycle path.
        ASSERT_EQ(0u, localInvalidationsPosted.load());
        ASSERT_GT(errors->Val(), errorsBefore);
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
