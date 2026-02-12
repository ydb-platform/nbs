#include "test_grpc_device_provider.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGrpcDeviceProviderTest)
{
    Y_UNIT_TEST(ShouldListDevices)
    {
        const ILoggingServicePtr logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});

        const TString socketPath = GetEnv("INFRA_DEVICE_PROVIDER_SOCKET");
        UNIT_ASSERT_UNEQUAL("", socketPath);

        TTestGrpcDeviceProvider provider{logging, socketPath};
        provider.Start();

        auto future = provider.ListNVMeDevices();
        const auto& devices = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(4, devices.size());

        provider.Stop();
    }
}

}   // namespace NCloud::NBlockStore
