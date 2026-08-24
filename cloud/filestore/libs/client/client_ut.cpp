#include "client.h"

#include "config.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/server/config.h>
#include <cloud/filestore/libs/server/server.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/tls_certificate_provider.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NClient {

using namespace NThreading;

using namespace NCloud::NFileStore::NServer;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

TPortManager PortManager;

std::pair<TClientConfigPtr, TServerConfigPtr> CreateConfigs()
{
    NProto::TClientConfig client;
    client.SetPort(PortManager.GetPort(9021));

    NProto::TServerConfig server;
    server.SetPort(client.GetPort());

    return {
        std::make_shared<TClientConfig>(client),
        std::make_shared<TServerConfig>(server),
    };
}

void DoTestShouldHandleRequest(
    ui64 requestId,
    ui64 expectedCriticalEventCount)
{
    auto logging = CreateLoggingService("console");

    auto service = std::make_shared<TFileStoreTest>();
    service->PingHandler = [] (auto, auto) {
        return MakeFuture<NProto::TPingResponse>();
    };

    auto [clientConfig, serverConfig] = CreateConfigs();

    auto registry = CreateRequestStatsRegistryStub();
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto server = CreateServer(
        serverConfig,
        logging,
        registry->GetRequestStats(),
        counters,
        CreateProfileLogStub(),
        CreateSchedulerStub(),
        service,
        CreateCertificateProviderStub());
    server->Start();

    auto client = CreateFileStoreClient(
        clientConfig,
        logging,
        CreateCertificateProviderStub());
    client->Start();

    InitCriticalEventsCounter(counters);
    auto criticalEvent = counters->GetCounter(
        GetCriticalEventForClientRequestIdIsZero(),
        true);

    auto context = MakeIntrusive<TCallContext>(requestId);
    auto request = std::make_shared<NProto::TPingRequest>();
    request->MutableHeaders()->SetRequestId(requestId);

    UNIT_ASSERT_VALUES_EQUAL(0, criticalEvent->Val());

    auto future = client->Ping(
        std::move(context),
        std::move(request));

    const auto& response = future.GetValue(WaitTimeout);
    UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
    UNIT_ASSERT_VALUES_EQUAL(
        expectedCriticalEventCount,
        criticalEvent->Val());

    client->Stop();
    server->Stop();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileStoreClientTest)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        DoTestShouldHandleRequest(
            CreateRequestId(),
            0 /* expectedCriticalEventCount */);
    }

    // A zero request id triggers a debug abort in the client.
#ifdef NDEBUG
    Y_UNIT_TEST(ShouldReportCriticalEventForZeroRequestId)
    {
        DoTestShouldHandleRequest(
            0 /* requestId */,
            1 /* expectedCriticalEventCount */);
    }
#endif
}

}   // namespace NCloud::NFileStore::NClient
