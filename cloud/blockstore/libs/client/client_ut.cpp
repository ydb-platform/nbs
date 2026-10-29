#include "session.h"

#include "client.h"
#include "config.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/grpc/tls_certificate_provider.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TClientTest)
{
    Y_UNIT_TEST(ShouldReturnNotImplementedErrorStatusCodeForUnsupportedRequestsFromDataEndpoint)
    {
        TPortManager portManager;
        ui16 dataPort = portManager.GetPort(9001);

        NProto::TClientAppConfig clientAppConfig;
        auto& clientConfig = *clientAppConfig.MutableClientConfig();
        clientConfig.SetPort(dataPort);
        clientConfig.SetClientId(CreateGuidAsString());

        auto result = CreateClient(
            std::make_shared<TClientAppConfig>(clientAppConfig),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateServerStatsStub(),
            CreateCertificateProviderStub());

        auto client = result.ExtractResult();
        UNIT_ASSERT_C(client, FormatError(result.GetError()));

        auto clientEndpoint = client->CreateDataEndpoint();

        client->Start();
        clientEndpoint->Start();
        Y_DEFER {
            clientEndpoint->Stop();
            client->Stop();
        };

        auto futurePing = clientEndpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());
        UNIT_ASSERT(futurePing.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_IMPLEMENTED,
            futurePing.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldStopWithRequestsInFlight)
    {
        TPortManager portManager;
        const ui16 port = portManager.GetPort(9001);   // nobody listens

        NProto::TClientAppConfig clientAppConfig;
        auto& clientConfig = *clientAppConfig.MutableClientConfig();
        clientConfig.SetHost("localhost");
        clientConfig.SetInsecurePort(port);
        clientConfig.SetClientId(CreateGuidAsString());
        clientConfig.SetRequestTimeout(5000);

        auto result = CreateClient(
            std::make_shared<TClientAppConfig>(clientAppConfig),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateServerStatsStub(),
            CreateCertificateProviderStub());

        auto client = result.ExtractResult();
        UNIT_ASSERT_C(client, FormatError(result.GetError()));
        client->Start();

        auto endpoint = client->CreateEndpoint();

        // enough requests that some are still queued unsent when Stop() runs
        TVector<TFuture<NProto::TPingResponse>> futures;
        for (int i = 0; i < 100; ++i) {
            futures.push_back(endpoint->Ping(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TPingRequest>()));
        }

        client->Stop();

        for (const auto& future: futures) {
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT_C(
                HasError(future.GetValue()),
                future.GetValue().ShortDebugString());
        }
    }

    Y_UNIT_TEST(ShouldCreateMultiHostEndpointPerCall)
    {
        auto result = CreateMultiHostClient(
            std::make_shared<TClientAppConfig>(NProto::TClientAppConfig()),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateServerStatsStub(),
            CreateCertificateProviderStub());
        auto client = result.ExtractResult();
        UNIT_ASSERT_C(client, FormatError(result.GetError()));

        auto first = client->CreateEndpoint("localhost", 9001, false);
        auto second = client->CreateEndpoint("localhost", 9001, false);
        UNIT_ASSERT(first != second);

        auto io = client->CreateIOEndpoint("localhost", 9001, false);
        UNIT_ASSERT(io != client->CreateIOEndpoint("localhost", 9001, false));
    }

    Y_UNIT_TEST(ShouldFinishRequestWhoseEndpointWasReleased)
    {
        TPortManager portManager;
        const ui16 port = portManager.GetPort(9001);   // nobody listens

        NProto::TClientAppConfig clientAppConfig;
        clientAppConfig.MutableClientConfig()->SetRequestTimeout(1000);

        auto result = CreateMultiHostClient(
            std::make_shared<TClientAppConfig>(clientAppConfig),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateServerStatsStub(),
            CreateCertificateProviderStub());
        auto client = result.ExtractResult();
        UNIT_ASSERT_C(client, FormatError(result.GetError()));
        client->Start();

        // the only handle to the endpoint goes away right after the request
        // is enqueued; whether the executor sends it before or after that is
        // a race, so under ASAN this catches a handler that borrows the stub
        auto future = client->CreateEndpoint("localhost", port, false)
                          ->Ping(
                              MakeIntrusive<TCallContext>(),
                              std::make_shared<NProto::TPingRequest>());

        client->Stop();

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_C(
            HasError(future.GetValue()),
            future.GetValue().ShortDebugString());
    }
}

}   // namespace NCloud::NBlockStore::NClient
