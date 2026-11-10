#include "data_endpoint_factory_stub.h"

#include "data_endpoint_factory.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NIcV2 {

Y_UNIT_TEST_SUITE(TDataEndpointFactoryStubTest)
{
    Y_UNIT_TEST(RejectsCreationWithoutReturningAnEndpoint)
    {
        auto factory = CreateDataEndpointFactoryStub();
        auto [endpoint, error] =
            factory->CreateDataEndpoint("cell", "host", {});
        UNIT_ASSERT(!endpoint);
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), E_NOT_IMPLEMENTED);
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
