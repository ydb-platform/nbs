#include "client.h"
#include "server.h"
#include "test_verbs.h"

#include <cstring>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

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

            throw TServiceError(ENODEV) << "rdma_listen error: No such device";
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
            throw TServiceError(MAKE_SYSTEM_ERROR(EINVAL));
        };

        context->Reject = [&](rdma_cm_id* id, const void* data, ui8 size) {
            Y_UNUSED(id);
            Y_UNUSED(data);
            Y_UNUSED(size);

            // test implementation of rdma_destroy_id just fills id with FF's
            rdma_cm_id reference;
            rdma_destroy_id(&reference);

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
};

}   // namespace NCloud::NBlockStore::NRdma

int rdma_destroy_id(struct rdma_cm_id *id) {
    memset(id, 0xFF, sizeof(rdma_cm_id));
    return 0;
}
