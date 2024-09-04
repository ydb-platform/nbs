#include "server.h"

#include <cloud/blockstore/libs/endpoint_proxy/client/client.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/guid.h>

namespace NCloud::NBlockStore::NServer {

namespace {
std::shared_ptr<NProto::TStartProxyEndpointRequest> CreateStartRequest(
    ui64 blocksCount,
    ui32 blockSize,
    TString unixSocketPath,
    TString nbdDevice)
{
    auto startRequest = std::make_shared<NProto::TStartProxyEndpointRequest>();
    startRequest->SetBlocksCount(blocksCount);
    startRequest->SetBlockSize(blockSize);
    startRequest->SetUnixSocketPath(std::move(unixSocketPath));
    startRequest->SetNbdDevice(std::move(nbdDevice));
    return startRequest;
}
}   // namespace

Y_UNIT_TEST_SUITE(TServerTest)
{
    Y_UNIT_TEST(StartRequestShouldNotReturnAlreadyAfterFailure)
    {
        // Test scenario:
        // 1. Start endpoint proxy server
        // 2. Start endpoint proxy client
        // 3. Send start endpoint request with invalid device path
        // 4. Expect response with error
        // 5. Send start endpoint request with invalid device path again
        // 6. Expect response with error
        const ui16 port = 8122;
        auto unixSocketPath = CreateGuidAsString();
        TEndpointProxyServerConfig serverConfig(
            port,
            0,
            "",
            "",
            "",
            unixSocketPath,
            false,
            "",
            TDuration::Seconds(1));

        auto server = CreateServer(
            serverConfig,
            CreateWallClockTimer(),
            CreateScheduler(),
            CreateLoggingService("server", TLogSettings{}));
        UNIT_ASSERT(server);
        server->Start();

        NClient::TEndpointProxyClientConfig clientConfig;
        clientConfig.Host = "127.0.0.1";
        clientConfig.Port = port;
        clientConfig.UnixSocketPath = unixSocketPath;
        auto client = NClient::CreateClient(
            clientConfig,
            CreateScheduler(),
            CreateWallClockTimer(),
            CreateLoggingService("client", TLogSettings{}));
        UNIT_ASSERT(client);
        client->Start();

        const ui64 blocksCount = 10000;
        const ui32 blockSize = 4096;
        const TString nbdDevice = "/dev/nbd9999";
        auto resp1 = client
                         ->StartProxyEndpoint(CreateStartRequest(
                             blocksCount,
                             blockSize,
                             unixSocketPath,
                             nbdDevice))
                         .GetValueSync();
        UNIT_ASSERT(HasError(resp1));

        auto resp2 = client
                         ->StartProxyEndpoint(CreateStartRequest(
                             blocksCount,
                             blockSize,
                             unixSocketPath,
                             nbdDevice))
                         .GetValueSync();
        UNIT_ASSERT(HasError(resp2));

        client->Stop();
        server->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NServer
