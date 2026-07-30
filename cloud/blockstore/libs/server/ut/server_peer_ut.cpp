#include "../server.h"

#include "../server_test.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace NCloud::NBlockStore::NClient;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerPeerTest)
{
    Y_UNIT_TEST(ShouldNormalizeIpv6PeerAddress)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 dataPort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->PingHandler =
            [&] (std::shared_ptr<NProto::TPingRequest> request) {
                const auto& peer = request->GetHeaders().GetInternal().GetPeer();

                UNIT_ASSERT_C(!peer.empty(), peer);
                UNIT_ASSERT_VALUES_EQUAL_C(TString::npos, peer.find("%5B"), peer);
                UNIT_ASSERT_VALUES_EQUAL_C(TString::npos, peer.find("%5D"), peer);

                if (peer.find("ipv6:") == 0) {
                    UNIT_ASSERT_C(peer.find('[') != TString::npos, peer);
                    UNIT_ASSERT_C(peer.find("]:") != TString::npos, peer);
                }

                return MakeFuture<NProto::TPingResponse>();
            };

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildServer(service);

        auto client = testFactory.CreateClientBuilder()
            .SetPort(port)
            .SetDataPort(dataPort)
            .BuildClient();

        server->Start();
        client->Start();
        Y_DEFER {
            client->Stop();
            server->Stop();
        };

        auto endpoint = client->CreateEndpoint();
        endpoint = testFactory.CreateDurableClient(std::move(endpoint));

        endpoint->Start();
        Y_DEFER {
            endpoint->Stop();
        };

        auto future = endpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }
}

}   // namespace NCloud::NBlockStore::NServer
