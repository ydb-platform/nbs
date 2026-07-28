#include "test_grpc_device_provider.h"

#include "device_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/folder/tempdir.h>
#include <util/system/env.h>

namespace NCloud::NBlockStore {

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGrpcDeviceProviderTest)
{
    Y_UNIT_TEST(ShouldListDevices)
    {
        const ILoggingServicePtr logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});

        const TString socketPath = GetEnv("INFRA_DEVICE_PROVIDER_SOCKET");
        UNIT_ASSERT_UNEQUAL("", socketPath);

        {
            auto deviceProvider = CreateTestGrpcDeviceProvider(
                logging,
                "unix://nbs@" + socketPath);
            deviceProvider->Start();

            auto future = deviceProvider->ListNVMeDevices();
            const auto& devices = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(4, devices.size());

            deviceProvider->Stop();
        }
        {
            auto deviceProvider = CreateTestGrpcDeviceProvider(
                logging,
                "unix://ydb@" + socketPath);
            deviceProvider->Start();

            auto future = deviceProvider->ListNVMeDevices();
            const auto& devices = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());

            deviceProvider->Stop();
        }
    }

    Y_UNIT_TEST(ShouldNotHangOnStopWithPendingRequest)
    {
        const ILoggingServicePtr logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});

        TTempDir tempDir;

        const TString socketPath = tempDir.Path() / "non-existent.sock";

        auto deviceProvider =
            CreateTestGrpcDeviceProvider(logging, "unix://nbs@" + socketPath);
        deviceProvider->Start();

        {
            auto future = deviceProvider->ListNVMeDevices();

            deviceProvider->Stop();

            auto [_, error] = ResultOrError(future);

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_GRPC_CANCELLED,
                error.GetCode(),
                FormatError(error));

            UNIT_ASSERT_C(
                GetErrorKind(error) == EErrorKind::ErrorRetriable,
                FormatError(error));
        }

        {
            auto future = deviceProvider->ListNVMeDevices();
            auto [_, error] = ResultOrError(future);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));
        }

        deviceProvider->Stop();
    }

    Y_UNIT_TEST(ShouldNotHangOnDestruction)
    {
        const ILoggingServicePtr logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});

        TTempDir tempDir;

        {
            const TString socketPath = tempDir.Path() / "non-existent.sock";

            auto deviceProvider = CreateTestGrpcDeviceProvider(
                logging,
                "unix://nbs@" + socketPath);

            Y_UNUSED(deviceProvider);
        }

        {
            const TString socketPath = GetEnv("INFRA_DEVICE_PROVIDER_SOCKET");
            UNIT_ASSERT_UNEQUAL("", socketPath);

            auto deviceProvider = CreateTestGrpcDeviceProvider(
                logging,
                "unix://nbs@" + socketPath);

            Y_UNUSED(deviceProvider);
        }
    }
}

}   // namespace NCloud::NBlockStore
