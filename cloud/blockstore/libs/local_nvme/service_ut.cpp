#include "service.h"

#include "config.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/folder/tempdir.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalNVMeServiceTest)
{
    Y_UNIT_TEST(ShouldLoadDevices)
    {
        TTempDir tempDir;
        const TString cacheFilePath = tempDir.Path() / "nvme.txt";

        auto generateNVMe = [i = 0] () mutable {
            NProto::TNVMeDevice d;

            d.SetSerialNumber(Sprintf("sn-%d", i));
            d.SetModel(Sprintf("model #%d", i + 1));
            d.SetCapacity(1_GB * (i + 1));
            d.SetPCIVendorId(100 * i);
            d.SetPCIDeviceId(200 * i);
            d.SetPCIAddress(Sprintf("%02d:00:%02d", i, i + 1));
            d.SetIOMMUGroup(40 + i);

            ++i;

            return d;
        };

        TVector<NProto::TNVMeDevice> expectedDevices{
            generateNVMe(),
            generateNVMe(),
            generateNVMe(),
        };

        {
            NProto::TNVMeDeviceList proto;
            proto.MutableDevices()->Assign(
                expectedDevices.begin(),
                expectedDevices.end());

            SerializeToTextFormat(proto, cacheFilePath);
        }

        NProto::TLocalNVMeConfig protoConfig;
        protoConfig.SetNVMeDevicesCacheFile(cacheFilePath);
        auto config = std::make_shared<TLocalNVMeConfig>(protoConfig);
        auto logging = CreateLoggingService("console");
        auto service = CreateLocalNVMeService(config, logging);
        service->Start();
        Y_DEFER
        {
            service->Stop();
        };

        const auto devices = service->GetNVMeDevices();
        UNIT_ASSERT_VALUES_EQUAL(expectedDevices.size(), devices.size());

        for (size_t i = 0; i != devices.size(); ++i) {
            const auto& lhs = expectedDevices[i];
            const auto& rhs = devices[i];

            UNIT_ASSERT_VALUES_EQUAL(
                lhs.GetSerialNumber(),
                rhs.GetSerialNumber());

            UNIT_ASSERT_VALUES_EQUAL(lhs.GetModel(), rhs.GetModel());

            UNIT_ASSERT_VALUES_EQUAL(lhs.GetCapacity(), rhs.GetCapacity());

            UNIT_ASSERT_VALUES_EQUAL(
                lhs.GetPCIVendorId(),
                rhs.GetPCIVendorId());

            UNIT_ASSERT_VALUES_EQUAL(
                lhs.GetPCIDeviceId(),
                rhs.GetPCIDeviceId());

            UNIT_ASSERT_VALUES_EQUAL(lhs.GetPCIAddress(), rhs.GetPCIAddress());

            UNIT_ASSERT_VALUES_EQUAL(lhs.GetIOMMUGroup(), rhs.GetIOMMUGroup());
        }

        for (const auto& d: devices) {
            auto future =
                service->AcquireNVMeDevice(d.GetSerialNumber());
            UNIT_ASSERT_VALUES_EQUAL(S_OK, future.GetValueSync().GetCode());
        }

        for (const auto& d: devices) {
            auto future =
                service->ReleaseNVMeDevice(d.GetSerialNumber());
            UNIT_ASSERT_VALUES_EQUAL(S_OK, future.GetValueSync().GetCode());
        }

        {
            auto future = service->AcquireNVMeDevice("unk");
            UNIT_ASSERT_VALUES_EQUAL(
                E_NOT_FOUND,
                future.GetValueSync().GetCode());
        }

        {
            auto future = service->ReleaseNVMeDevice("unk");
            UNIT_ASSERT_VALUES_EQUAL(
                E_NOT_FOUND,
                future.GetValueSync().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldStub)
    {
        auto logging = CreateLoggingService("console");
        auto service = CreateLocalNVMeServiceStub(logging);
        service->Start();

        UNIT_ASSERT_VALUES_EQUAL(0, service->GetNVMeDevices().size());
        auto future = service->ReleaseNVMeDevice("foo");
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, future.GetValueSync().GetCode());

        service->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
