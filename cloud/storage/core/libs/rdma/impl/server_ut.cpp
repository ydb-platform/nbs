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

    TVector<ibv_recv_wr*> recv;

    with_lock(context->CompletionLock) {
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
            // bad id, good opcode
            if (wc->wr_id == recv[1]->wr_id) {
                wc->wr_id = Max<ui64>();
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

    context->Reject = [&](rdma_cm_id* id, const void* data, ui8 size)
    {
        Y_UNUSED(id);

        EXPECT_EQ(sizeof(TRejectMessage), size);

        const auto* rejectMsg = static_cast<const TRejectMessage*>(data);
        EXPECT_EQ(RDMA_PROTO_VERSION, ParseMessageHeader(rejectMsg));
        EXPECT_EQ(RDMA_PROTO_CONFIG_MISMATCH, rejectMsg->Status);
        EXPECT_EQ(
            serverConfig->SendQueueSize + serverConfig->RecvQueueSize,
            rejectMsg->QueueSize);
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
        static_cast<ui16>(serverConfig->SendQueueSize + 1),
        static_cast<ui16>(serverConfig->RecvQueueSize),
        serverConfig->MaxBufferSize);

    NVerbs::CreateConnection(
        context,
        static_cast<ui16>(serverConfig->SendQueueSize),
        static_cast<ui16>(serverConfig->RecvQueueSize - 1),
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

int NVerbs::DestroyId(rdma_cm_id* id)
{
    memset(id, 0xFF, sizeof(rdma_cm_id));
    return 0;
}

}   // namespace NCloud::NStorage::NRdma
