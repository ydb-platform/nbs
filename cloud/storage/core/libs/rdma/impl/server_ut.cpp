#include "server.h"

#include "client.h"
#include "test_verbs.h"

#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/scope.h>
#include <util/stream/printf.h>

#include <atomic>
#include <cstring>
#include <thread>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

NMonitoring::TDynamicCountersPtr GetServerCounters(
    const IMonitoringServicePtr& monitoring)
{
    return monitoring->GetCounters()
        ->GetSubgroup("counters", "rdma")
        ->GetSubgroup("component", "server");
}

IServerPtr CreateTestServer(
    NVerbs::IVerbsPtr verbs,
    const ILoggingServicePtr& logging,
    const IMonitoringServicePtr& monitoring,
    TServerConfigPtr config)
{
    return CreateServer(
        std::move(verbs),
        TObservabilityProvider(
            logging,
            monitoring,
            "RDMA_TEST",
            "rdma",
            "server"),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

struct TClientHandler
    : IClientHandler
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

struct TServerHandler
    : IServerHandler
{
    TCallContextBasePtr CreateCallContext() override
    {
        return MakeIntrusive<TCallContextBase>(ui64{0});
    }

    void HandleRequest(
        void* context,
        TCallContextBasePtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        Y_UNUSED(context);
        Y_UNUSED(callContext);
        Y_UNUSED(in);
        Y_UNUSED(out);
    }
};

struct TDelayedServerHandler final
    : IServerHandler
{
    NThreading::TPromise<void> RequestReceived =
        NThreading::NewPromise<void>();

    void* Context = nullptr;
    TStringBuf Out;

    TCallContextBasePtr CreateCallContext() override
    {
        return MakeIntrusive<TCallContextBase>(ui64{0});
    }

    void HandleRequest(
        void* context,
        TCallContextBasePtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(in);

        Context = context;
        Out = out;
        RequestReceived.SetValue();
    }
};

template <typename TPredicate>
bool WaitUntil(
    TPredicate predicate,
    TDuration timeout = TDuration::Seconds(5))
{
    const auto start = GetCycleCount();
    while (!predicate()) {
        if (CyclesToDurationSafe(GetCycleCount() - start) > timeout) {
            return false;
        }
        SpinLockPause();
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRdmaServerTest, ShouldStartEndpoint)
{
    auto verbs =
        NVerbs::CreateTestVerbs(MakeIntrusive<NVerbs::TTestContext>());
    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();
    auto clientConfig = std::make_shared<TClientConfig>();

    auto logging = CreateLoggingService(
        "console",
        TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(
        verbs,
        logging,
        monitoring,
        serverConfig);

    server->Start();

    auto serverEndpoint = server->StartEndpoint(
        "::",
        10020,
        std::make_shared<TServerHandler>());

    server->Stop();
}

TEST(TRdmaServerTest, ShouldStartEndpointWithToS)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();
    auto verbs =
        NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();
    auto clientConfig = std::make_shared<TClientConfig>();
    serverConfig->IpTypeOfService = 42;
    ASSERT_NE(42, testContext->ToS);

    auto logging = CreateLoggingService(
        "console",
        TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(
        verbs,
        logging,
        monitoring,
        serverConfig);

    server->Start();

    auto serverEndpoint = server->StartEndpoint(
        "::",
        10020,
        std::make_shared<TServerHandler>());
    ASSERT_EQ(42, testContext->ToS);

    server->Stop();
}

TEST(TRdmaServerTest, ShouldUseConfiguredQpParamsOnAcceptAndSetupQp)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();
    auto verbs = NVerbs::CreateTestVerbs(testContext);

    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();
    serverConfig->QpRetryCount = 2;
    serverConfig->QpRnrRetryCount = 4;
    serverConfig->QpTimeout = 6;
    serverConfig->QpMinRnrTimer = 8;

    std::atomic<bool> acceptCalled = false;
    std::atomic<int> acceptRetryCount = -1;
    std::atomic<int> acceptRnrRetryCount = -1;
    std::atomic<bool> modifyCalled = false;

    testContext->HandleAccept = [&](rdma_cm_id* id, rdma_conn_param* param)
    {
        Y_UNUSED(id);
        acceptRetryCount.store(param->retry_count);
        acceptRnrRetryCount.store(param->rnr_retry_count);
        acceptCalled.store(true);
    };

    testContext->ModifyQP = [&](ibv_qp* qp, ibv_qp_attr* attr, int mask)
    {
        Y_UNUSED(qp);

        if ((mask & IBV_QP_STATE) && attr->qp_state == IBV_QPS_ERR) {
            NVerbs::Flush(testContext);
            return;
        }

        const int expectedMask = IBV_QP_TIMEOUT | IBV_QP_MIN_RNR_TIMER;

        EXPECT_EQ(expectedMask, mask);
        EXPECT_EQ(serverConfig->QpTimeout, attr->timeout);
        EXPECT_EQ(serverConfig->QpMinRnrTimer, attr->min_rnr_timer);

        modifyCalled.store(true);
    };

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(verbs, logging, monitoring, serverConfig);
    server->Start();
    Y_DEFER
    {
        server->Stop();
    };

    auto endpoint =
        server->StartEndpoint("::", 10020, std::make_shared<TServerHandler>());
    Y_UNUSED(endpoint);

    NVerbs::CreateConnection(testContext);

    auto start = GetCycleCount();
    while ((!acceptCalled.load() || !modifyCalled.load()) &&
           CyclesToDurationSafe(GetCycleCount() - start) <
               TDuration::Seconds(5))
    {
        SpinLockPause();
    }

    ASSERT_TRUE(acceptCalled.load());
    ASSERT_EQ(serverConfig->QpRetryCount, acceptRetryCount.load());
    ASSERT_EQ(serverConfig->QpRnrRetryCount, acceptRnrRetryCount.load());

    ASSERT_TRUE(modifyCalled.load());
}

TEST(TRdmaServerTest, StartEndpointShouldNotThrow)
{
    auto context = MakeIntrusive<NVerbs::TTestContext>();

    context->Listen = [](rdma_cm_id* id, int backlog) {
        Y_UNUSED(id);
        Y_UNUSED(backlog);

        STORAGE_THROW_SERVICE_ERROR(ENODEV)
            << "rdma_listen error: No such device";
    };

    auto verbs = NVerbs::CreateTestVerbs(std::move(context));
    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();
    auto clientConfig = std::make_shared<TClientConfig>();

    auto logging = CreateLoggingService(
        "console",
        TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(
        verbs,
        logging,
        monitoring,
        serverConfig);

    server->Start();

    try {
        auto serverEndpoint = server->StartEndpoint(
            "::",
            10020,
            std::make_shared<TServerHandler>());

    } catch (...) {
        FAIL() << "should not throw";
    }

    server->Stop();
}

TEST(TRdmaServerTest, ShouldHandleSessionError)
{
    NThreading::TPromise<void> done = NThreading::NewPromise<void>();
    auto context = MakeIntrusive<NVerbs::TTestContext>();

    context->CreateQP = [](rdma_cm_id* id, ibv_qp_init_attr* attr) {
        Y_UNUSED(id);
        Y_UNUSED(attr);
        STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(EINVAL));
    };

    context->DestroyQP = [](rdma_cm_id* id) {
        if (id->qp == nullptr) {
            STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(EINVAL));
        }
    };

    context->Reject = [&](rdma_cm_id* id, const void* data, ui8 size) {
        Y_UNUSED(data);
        Y_UNUSED(size);

        // TestRdmaDestroyId doesn't free anything, just fills rdma_cm_id
        // with FF's
        rdma_cm_id reference;
        memset(&reference, 0xFF, sizeof(rdma_cm_id));

        // make sure rdma_destroy_id hasn't been called before Reject
        ASSERT_NE(
            0,
            memcmp(
                reinterpret_cast<void*>(&reference),
                reinterpret_cast<void*>(id),
                sizeof(rdma_cm_id)));

        done.SetValue();
    };

    auto verbs = NVerbs::CreateTestVerbs(context);
    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();

    auto logging = CreateLoggingService(
        "console",
        TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(
        verbs,
        logging,
        monitoring,
        serverConfig);

    server->Start();

    auto serverEndpoint = server->StartEndpoint(
        "::",
        10020,
        std::make_shared<TServerHandler>());

    NVerbs::CreateConnection(context);

    done.GetFuture().Wait();
    server->Stop();
}

TEST(TRdmaServerTest, ShouldHandleErrors)
{
    auto context = MakeIntrusive<NVerbs::TTestContext>();

    context->ModifyQP = [&](ibv_qp* qp, ibv_qp_attr* attr, int mask)
    {
        Y_UNUSED(qp);

        if (!((mask & IBV_QP_STATE) && attr->qp_state == IBV_QPS_ERR)) {
            return;
        }
        NVerbs::Flush(context);
    };

    auto verbs = NVerbs::CreateTestVerbs(context);
    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();

    auto logging = CreateLoggingService(
        "console",
        TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(
        verbs,
        logging,
        monitoring,
        serverConfig);

    server->Start();
    Y_DEFER {
        server->Stop();
    };

    auto endpoint = server
        ->StartEndpoint(
            "::",
            10020,
            std::make_shared<TServerHandler>());

    // emulate client connection

    NVerbs::CreateConnection(context);

    // wait for client session

    auto wait = [](auto& counter, auto value) {
        auto start = GetCycleCount();
        while (counter->Val() != value) {
            auto now = GetCycleCount();
            if (CyclesToDurationSafe(now - start) > TDuration::Seconds(5)) {
                ASSERT_EQ(value, counter->Val());
            }
            SpinLockPause();
        }
    };

    auto counters = GetServerCounters(monitoring);

    auto activeRecv = counters->GetCounter("ActiveRecv");
    auto abortedRequests = counters->GetCounter("AbortedRequests");
    auto activeRequests = counters->GetCounter("ActiveRequests");
    auto errors = counters->GetCounter("Errors");

    wait(activeRecv, serverConfig->QueueSize);

    // emulate exchange with the client

    ibv_recv_wr badWr;
    badWr.wr_id = Max<ui64>();
    TVector<ibv_recv_wr*> recv;

    with_lock(context->CompletionLock) {
        recv.push_back(&badWr);
        context->ProcessedRecvEvents.push_back(&badWr);

        for (size_t i = 0; i < 4; i++) {
            auto* wr = context->RecvEvents.back();
            context->RecvEvents.pop_back();
            context->ProcessedRecvEvents.push_back(wr);
            recv.push_back(wr);
        }
        context->HandleCompletionEvent = [&](ibv_wc* wc) {
            // bad id, good opcode
            if (wc->wr_id == badWr.wr_id) {
                return;
            }
            // good id, good opcode, error status
            if (wc->wr_id == recv[1]->wr_id) {
                wc->status = IBV_WC_RETRY_EXC_ERR;
                return;
            }
            // good id, bad opcode
            if (wc->wr_id == recv[2]->wr_id) {
                wc->opcode = IBV_WC_RECV_RDMA_WITH_IMM;
                return;
            }
            // good id and opcode, success status, good message
            if (wc->wr_id == recv[3]->wr_id) {
                auto* msg = reinterpret_cast<TRequestMessage*>(recv[3]->sg_list[0].addr);
                InitMessageHeader(msg, RDMA_PROTO_VERSION);
                msg->In.Length = 4096;
                return;
            }
            // fail all reads
            if (wc->opcode == IBV_WC_RDMA_READ) {
                wc->status = IBV_WC_GENERAL_ERR;
                return;
            }
            // good id and opcode, success status, bad message
        };
        context->CompletionHandle.Set();
    }

    wait(errors, 5);
    wait(activeRecv, 8);
    wait(abortedRequests, 1);
    wait(activeRequests, 0);
}

TEST(TRdmaServerTest, ShouldRejectConnectionOnConfigMismatchInStrictValidation)
{
    NThreading::TPromise<void> done = NThreading::NewPromise<void>();
    std::atomic<int> rejectCount = 0;
    std::atomic<bool> createQpCalled = false;

    auto context = MakeIntrusive<NVerbs::TTestContext>();

    context->CreateQP = [&](rdma_cm_id* id, ibv_qp_init_attr* attr)
    {
        Y_UNUSED(id);
        Y_UNUSED(attr);
        createQpCalled.store(true);
    };

    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();
    serverConfig->StrictValidation = true;
    serverConfig->QueueSize = 1;
    serverConfig->SendQueueSize = 20;
    serverConfig->RecvQueueSize = 40;

    context->Reject = [&](rdma_cm_id* id, const void* data, ui8 size)
    {
        Y_UNUSED(id);

        EXPECT_EQ(sizeof(TRejectMessage2), size);

        const auto* rejectMsg = static_cast<const TRejectMessage2*>(data);
        EXPECT_EQ(RDMA_PROTO_VERSION, ParseMessageHeader(rejectMsg));
        EXPECT_EQ(RDMA_PROTO_CONFIG_MISMATCH, rejectMsg->Status);
        EXPECT_EQ(serverConfig->SendQueueSize, rejectMsg->SendQueueSize);
        EXPECT_EQ(serverConfig->RecvQueueSize, rejectMsg->RecvQueueSize);
        EXPECT_EQ(serverConfig->MaxBufferSize, rejectMsg->MaxBufferSize);

        if (++rejectCount == 3) {
            done.TrySetValue();
        }
    };

    auto verbs = NVerbs::CreateTestVerbs(context);

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(verbs, logging, monitoring, serverConfig);

    server->Start();
    Y_DEFER
    {
        server->Stop();
    };

    auto serverEndpoint =
        server->StartEndpoint("::", 10020, std::make_shared<TServerHandler>());
    Y_UNUSED(serverEndpoint);

    NVerbs::CreateConnection(
        context,
        static_cast<ui16>(serverConfig->RecvQueueSize + 1),
        static_cast<ui16>(serverConfig->RecvQueueSize),
        serverConfig->MaxBufferSize);

    NVerbs::CreateConnection(
        context,
        static_cast<ui16>(serverConfig->SendQueueSize),
        static_cast<ui16>(serverConfig->SendQueueSize - 1),
        serverConfig->MaxBufferSize);

    NVerbs::CreateConnection(
        context,
        static_cast<ui16>(serverConfig->SendQueueSize),
        static_cast<ui16>(serverConfig->RecvQueueSize),
        serverConfig->MaxBufferSize + 1);

    ASSERT_TRUE(done.GetFuture().Wait(TDuration::Seconds(5)));
    EXPECT_EQ(3, rejectCount.load());
    EXPECT_FALSE(createQpCalled.load());
}

TEST(TRdmaServerTest, ShouldKeepSessionAliveUntilHandlerCompletes)
{
    auto context = MakeIntrusive<NVerbs::TTestContext>();

    NThreading::TPromise<void> flushTriggered = NThreading::NewPromise<void>();

    // Combine flush-on-error with a signal so the test can wait until Stop()
    // has actually entered the flush phase before calling SendResponse.
    context->ModifyQP = [&](ibv_qp* qp, ibv_qp_attr* attr, int mask) mutable
    {
        Y_UNUSED(qp);

        if ((mask & IBV_QP_STATE) && attr->qp_state == IBV_QPS_ERR) {
            NVerbs::Flush(context);
            flushTriggered.TrySetValue();
            return;
        }
    };

    std::atomic<rdma_cm_id*> sessionId{nullptr};
    NThreading::TPromise<void> sessionDestroyed =
        NThreading::NewPromise<void>();

    context->HandleAccept = [&](rdma_cm_id* id, rdma_conn_param* param)
    {
        Y_UNUSED(param);
        sessionId.store(id);
    };

    context->DestroyQP = [&](rdma_cm_id* id)
    {
        Y_UNUSED(id);
        sessionDestroyed.TrySetValue();
    };

    auto verbs = NVerbs::CreateTestVerbs(context);
    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();
    serverConfig->QueueSize = 1;
    serverConfig->SendQueueSize = 1;
    serverConfig->RecvQueueSize = 1;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(verbs, logging, monitoring, serverConfig);
    server->Start();

    auto handler = std::make_shared<TDelayedServerHandler>();
    auto endpoint = server->StartEndpoint("::", 10020, handler);

    NVerbs::CreateConnection(
        context,
        static_cast<ui16>(serverConfig->SendQueueSize),
        static_cast<ui16>(serverConfig->RecvQueueSize),
        serverConfig->MaxBufferSize);

    ASSERT_TRUE(WaitUntil(
        [&]
        {
            return sessionId.load() != nullptr &&
                   AtomicGet(context->PostRecvCounter) >=
                       static_cast<int>(serverConfig->RecvQueueSize);
        }));

    with_lock (context->CompletionLock) {
        ASSERT_FALSE(context->RecvEvents.empty());

        auto* recv = context->RecvEvents.back();
        context->RecvEvents.pop_back();

        auto* msg = reinterpret_cast<TRequestMessage*>(recv->sg_list[0].addr);
        memset(msg, 0, sizeof(*msg));
        InitMessageHeader(msg, RDMA_PROTO_VERSION);
        msg->ReqId = 1;
        msg->Out.Length = 4_KB;

        context->ProcessedRecvEvents.push_back(recv);
        context->CompletionHandle.Set();
    }

    handler->RequestReceived.GetFuture().GetValueSync();
    ASSERT_NE(nullptr, handler->Context);

    // Run Stop() in a background thread — it will block inside the flush loop
    // waiting for ExecutingRequests == 0.
    std::thread stopThread([&] { server->Stop(); });
    Y_DEFER
    {
        if (stopThread.joinable()) {
            stopThread.join();
        }
    };

    // Wait until Stop() has actually reached the flush phase (ModifyQP hook
    // fired), guaranteeing it is now blocked on IsFlushed().
    flushTriggered.GetFuture().GetValueSync();

    ASSERT_TRUE(WaitUntil(
        [c = GetServerCounters(monitoring)]
        {
            return c->GetCounter("ActiveRecv")->Val() == 0 &&
                   c->GetCounter("ActiveSend")->Val() == 0 &&
                   c->GetCounter("ActiveWrite")->Val() == 0 &&
                   c->GetCounter("ActiveRead")->Val() == 0;
        }));

    ASSERT_FALSE(sessionDestroyed.GetFuture().HasValue());

    endpoint->SendResponse(handler->Context, 0);

    stopThread.join();
    sessionDestroyed.GetFuture().GetValueSync();
}

TEST(TRdmaServerTest, ShouldKeepSessionAliveUntilHandlerCompletesOnDisconnect)
{
    auto context = MakeIntrusive<NVerbs::TTestContext>();
    context->ModifyQP = [&](ibv_qp* qp, ibv_qp_attr* attr, int mask)
    {
        Y_UNUSED(qp);

        if (!((mask & IBV_QP_STATE) && attr->qp_state == IBV_QPS_ERR)) {
            return;
        }
        NVerbs::Flush(context);
    };

    std::atomic<rdma_cm_id*> sessionId{nullptr};
    NThreading::TPromise<void> sessionDestroyed =
        NThreading::NewPromise<void>();

    context->HandleAccept = [&](rdma_cm_id* id, rdma_conn_param* param)
    {
        Y_UNUSED(param);
        sessionId.store(id);
    };

    context->DestroyQP = [&](rdma_cm_id* id)
    {
        Y_UNUSED(id);
        sessionDestroyed.TrySetValue();
    };

    auto verbs = NVerbs::CreateTestVerbs(context);
    auto monitoring = CreateMonitoringServiceStub();
    auto serverConfig = std::make_shared<TServerConfig>();
    serverConfig->QueueSize = 1;
    serverConfig->SendQueueSize = 1;
    serverConfig->RecvQueueSize = 1;

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto server = CreateTestServer(verbs, logging, monitoring, serverConfig);
    server->Start();
    Y_DEFER
    {
        server->Stop();
    };

    auto handler = std::make_shared<TDelayedServerHandler>();
    auto endpoint = server->StartEndpoint("::", 10020, handler);

    NVerbs::CreateConnection(
        context,
        static_cast<ui16>(serverConfig->SendQueueSize),
        static_cast<ui16>(serverConfig->RecvQueueSize),
        serverConfig->MaxBufferSize);

    ASSERT_TRUE(WaitUntil(
        [&]
        {
            return sessionId.load() != nullptr &&
                   AtomicGet(context->PostRecvCounter) >=
                       static_cast<int>(serverConfig->RecvQueueSize);
        }));

    with_lock (context->CompletionLock) {
        ASSERT_FALSE(context->RecvEvents.empty());

        auto* recv = context->RecvEvents.back();
        context->RecvEvents.pop_back();

        auto* msg = reinterpret_cast<TRequestMessage*>(recv->sg_list[0].addr);
        memset(msg, 0, sizeof(*msg));
        InitMessageHeader(msg, RDMA_PROTO_VERSION);
        msg->ReqId = 1;
        msg->Out.Length = 4_KB;

        context->ProcessedRecvEvents.push_back(recv);
        context->CompletionHandle.Set();
    }

    handler->RequestReceived.GetFuture().GetValueSync();
    ASSERT_NE(nullptr, handler->Context);

    verbs->Disconnect(sessionId.load());

    ASSERT_TRUE(WaitUntil(
        [c = GetServerCounters(monitoring)]
        {
            return c->GetCounter("ActiveRecv")->Val() == 0 &&
                   c->GetCounter("ActiveSend")->Val() == 0 &&
                   c->GetCounter("ActiveWrite")->Val() == 0 &&
                   c->GetCounter("ActiveRead")->Val() == 0;
        }));

    ASSERT_FALSE(sessionDestroyed.GetFuture().HasValue());

    endpoint->SendResponse(handler->Context, 0);

    sessionDestroyed.GetFuture().GetValueSync();
}

int NVerbs::DestroyId(rdma_cm_id* id)
{
    memset(id, 0xFF, sizeof(rdma_cm_id));
    return 0;
}

}   // namespace NCloud::NStorage::NRdma
