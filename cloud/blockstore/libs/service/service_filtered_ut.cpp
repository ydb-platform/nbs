#include "service_filtered.h"

#include "service_test.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFilteredServiceTest)
{
    Y_UNIT_TEST(ShouldFailForbiddenRequests)
    {
        auto testService = std::make_shared<TTestService>();

        testService->StartEndpointHandler =
            [&](std::shared_ptr<NProto::TStartEndpointRequest>)
        {
            return MakeFuture<NProto::TStartEndpointResponse>();
        };
        testService->StopEndpointHandler =
            [&](std::shared_ptr<NProto::TStopEndpointRequest>)
        {
            return MakeFuture<NProto::TStopEndpointResponse>();
        };
        testService->ListEndpointsHandler =
            [&](std::shared_ptr<NProto::TListEndpointsRequest>)
        {
            return MakeFuture<NProto::TListEndpointsResponse>();
        };
        testService->KickEndpointHandler =
            [&](std::shared_ptr<NProto::TKickEndpointRequest>)
        {
            return MakeFuture<NProto::TKickEndpointResponse>();
        };

        auto service = CreateFilteredService(
            testService,
            {EBlockStoreRequest::KickEndpoint,
             EBlockStoreRequest::StopEndpoint});

        {
            auto future = service->StartEndpoint(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TStartEndpointRequest>());
            auto resp = future.GetValue();
            UNIT_ASSERT(HasError(resp));
            UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, resp.GetError().GetCode());
        }
        {
            auto future = service->StopEndpoint(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TStopEndpointRequest>());
            auto resp = future.GetValue();
            UNIT_ASSERT_C(!HasError(resp), resp);
        }
        {
            auto future = service->ListEndpoints(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListEndpointsRequest>());
            auto resp = future.GetValue();
            UNIT_ASSERT(HasError(resp));
            UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, resp.GetError().GetCode());
        }
        {
            auto future = service->KickEndpoint(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TKickEndpointRequest>());
            auto resp = future.GetValue();
            UNIT_ASSERT_C(!HasError(resp), resp);
        }
    }
}

}   // namespace NCloud::NBlockStore
