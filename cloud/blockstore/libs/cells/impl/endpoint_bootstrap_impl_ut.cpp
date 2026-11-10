#include "endpoint_bootstrap_impl.h"

#include <cloud/blockstore/libs/icv2/iface/data_endpoint_factory_stub.h>
#include <cloud/blockstore/libs/icv2/testlib/data_endpoint_factory_test.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

TCellHostConfig MakeHostConfig()
{
    NProto::TCellConfig cellProto;
    cellProto.SetCellId("cell-2");
    TCellConfig cell(std::move(cellProto));
    NProto::TCellHostConfig host;
    host.SetFqdn("host-b");
    return TCellHostConfig(host, cell);
}

////////////////////////////////////////////////////////////////////////////////

class TConnectionHandler final: public NIcV2::IHostConnectionHandler
{
public:
    ui32 Disconnections = 0;

    void HandleDisconnected() noexcept override
    {
        ++Disconnections;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TCellHostEndpointBootstrapTest)
{
    Y_UNIT_TEST(RejectsAnUnconfiguredFactory)
    {
        auto setup = CreateCellHostEndpointBootstrap();
        auto [endpoint, error] =
            setup->SetupHostIcV2Endpoint({}, MakeHostConfig(), {});
        UNIT_ASSERT(!endpoint);
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), E_ARGUMENT);
    }

    Y_UNIT_TEST(ReportsUnavailableTransportFromTheStub)
    {
        TBootstrap bootstrap{
            .IcV2EndpointFactory = NIcV2::CreateDataEndpointFactoryStub(),
        };
        auto setup = CreateCellHostEndpointBootstrap();
        auto [endpoint, error] =
            setup->SetupHostIcV2Endpoint(bootstrap, MakeHostConfig(), {});
        UNIT_ASSERT(!endpoint);
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), E_NOT_IMPLEMENTED);
    }

    Y_UNIT_TEST(ForwardsSelectedCellHostAndConnectionHandler)
    {
        auto factory = std::make_shared<NIcV2::TTestDataEndpointFactory>();
        auto service = std::make_shared<TTestService>();
        auto handler = std::make_shared<TConnectionHandler>();
        ui32 reads = 0;
        service->ReadBlocksLocalHandler = [&](auto request)
        {
            Y_UNUSED(request);

            ++reads;
            return NThreading::MakeFuture<NProto::TReadBlocksLocalResponse>();
        };
        factory->EnqueueEndpoint(service);
        TBootstrap bootstrap{.IcV2EndpointFactory = factory};
        auto setup = CreateCellHostEndpointBootstrap();
        auto [endpoint, error] =
            setup->SetupHostIcV2Endpoint(bootstrap, MakeHostConfig(), handler);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT(endpoint);
        const auto requests = factory->GetRequests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].CellId, "cell-2");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Host, "host-b");
        UNIT_ASSERT(endpoint
                        ->ReadBlocksLocal(
                            MakeIntrusive<TCallContext>(),
                            std::make_shared<NProto::TReadBlocksLocalRequest>())
                        .Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(reads, 1);
        factory->SetStatus(
            endpoint,
            {.State = NIcV2::EHostChannelState::Connected});
        factory->NotifyDisconnected(endpoint);
        UNIT_ASSERT_VALUES_EQUAL(handler->Disconnections, 1);
    }

    Y_UNIT_TEST(PropagatesFactoryFailure)
    {
        auto factory = std::make_shared<NIcV2::TTestDataEndpointFactory>();
        factory->EnqueueError(MakeError(E_FAIL, "endpoint setup failed"));
        TBootstrap bootstrap{.IcV2EndpointFactory = factory};
        auto setup = CreateCellHostEndpointBootstrap();
        auto [endpoint, error] =
            setup->SetupHostIcV2Endpoint(bootstrap, MakeHostConfig(), {});
        UNIT_ASSERT(!endpoint);
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), E_FAIL);
        UNIT_ASSERT_VALUES_EQUAL(error.GetMessage(), "endpoint setup failed");
    }
}

}   // namespace NCloud::NBlockStore::NCells
