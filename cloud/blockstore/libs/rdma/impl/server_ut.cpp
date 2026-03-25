#include "client.h"
#include "server.h"
#include "test_verbs.h"

#include <cstring>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <cloud/blockstore/libs/rdma/iface/protocol.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/printf.h>

namespace NCloud::NBlockStore::NRdma {

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
    void HandleRequest(
        void* context,
        TCallContextPtr callContext,
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

Y_UNIT_TEST_SUITE(TRdmaServerTest)
{
    Y_UNIT_TEST(ShouldStartEndpoint)
    {
        auto verbs =
            NVerbs::CreateTestVerbs(MakeIntrusive<NVerbs::TTestContext>());
        auto monitoring = CreateMonitoringServiceStub();
        auto serverConfig = std::make_shared<TServerConfig>();
        auto clientConfig = std::make_shared<TClientConfig>();

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto server = CreateServer(
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

    Y_UNIT_TEST(ShouldStartEndpointWithToS)
    {
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        auto verbs =
            NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto serverConfig = std::make_shared<TServerConfig>();
        auto clientConfig = std::make_shared<TClientConfig>();
        serverConfig->IpTypeOfService = 42;
        UNIT_ASSERT_VALUES_UNEQUAL(42, testContext->ToS);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto server = CreateServer(
            verbs,
            logging,
            monitoring,
            serverConfig);

        server->Start();

        auto serverEndpoint = server->StartEndpoint(
            "::",
            10020,
            std::make_shared<TServerHandler>());
        UNIT_ASSERT_VALUES_EQUAL(42, testContext->ToS);

        server->Stop();
    }

    Y_UNIT_TEST(StartEndpointShouldNotThrow)
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

        auto server = CreateServer(
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
            UNIT_FAIL("should not throw");
        }

        server->Stop();
    }

    Y_UNIT_TEST(ShouldHandleSessionError)
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
            UNIT_ASSERT(memcmp(
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

        auto server = CreateServer(
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

    Y_UNIT_TEST(ShouldHandleErrors)
    {
        NThreading::TPromise<void> done = NThreading::NewPromise<void>();
        auto context = MakeIntrusive<NVerbs::TTestContext>();

        auto verbs = NVerbs::CreateTestVerbs(context);
        auto monitoring = CreateMonitoringServiceStub();
        auto serverConfig = std::make_shared<TServerConfig>();

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto server = CreateServer(
            verbs,
            logging,
            monitoring,
            serverConfig);

        server->Start();
        Y_DEFER {
            server->Stop();
        };

        auto endpoint = server
            ->StartEndpoint("::", 10020, std::make_shared<TServerHandler>());

        // emulate client connection

        NVerbs::CreateConnection(context);

        // wait for client session

        auto wait = [](auto& counter, auto value) {
            auto start = GetCycleCount();
            while (counter->Val() != value) {
                auto now = GetCycleCount();
                if (CyclesToDurationSafe(now - start) > TDuration::Seconds(5)) {
                    UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
                }
                SpinLockPause();
            }
        };

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "rdma_server");

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
};

int NVerbs::DestroyId(rdma_cm_id* id)
{
    memset(id, 0xFF, sizeof(rdma_cm_id));
    return 0;
}

}   // namespace NCloud::NBlockStore::NRdma
