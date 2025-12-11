#include "client.h"

#include "config.h"

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/server/config.h>
#include <cloud/filestore/libs/server/server.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileStoreClientTest)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        auto logging = CreateLoggingService("console");

        auto service = std::make_shared<TFileStoreTest>();
        service->PingHandler = [](auto, auto)
        {
            return MakeFuture<NProto::TPingResponse>();
        };

        auto [clientConfig, serverConfig] = CreateConfigs();

        auto registry = CreateRequestStatsRegistryStub();
        auto server = CreateServer(
            serverConfig,
            logging,
            registry->GetRequestStats(),
            CreateProfileLogStub(),
            service);
        server->Start();

        auto client = CreateFileStoreClient(clientConfig, logging);
        client->Start();

        auto context = MakeIntrusive<TCallContext>();
        auto request = std::make_shared<NProto::TPingRequest>();

        auto future = client->Ping(std::move(context), std::move(request));

        const auto& response = future.GetValue(WaitTimeout);
        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));

        client->Stop();
        server->Stop();
    }
}

}   // namespace NCloud::NFileStore::NClient
