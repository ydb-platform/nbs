#include "device_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/folder/tempdir.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<NProto::TNVMeDevice> PrepareDeviceFile(const TString& path)
{
    NProto::TNVMeDeviceList list;
    ParseProtoTextFromString(
        R"(
        Devices {
            SerialNumber: "NVME_0"
            PCIAddress: "f1:00.0"
            IOMMUGroup: 10
            VendorId: 0x100
            DeviceId: 0x200
            Model: "Test NVMe 1"
        }
        Devices {
            SerialNumber: "NVME_1"
            PCIAddress: "31:00.0"
            IOMMUGroup: 20
            VendorId: 0x300
            DeviceId: 0x400
            Model: "Test NVMe 2"
        }
        Devices {
            SerialNumber: "NVME_2"
            PCIAddress: "33:00.0"
            IOMMUGroup: 30
            VendorId: 0x100
            DeviceId: 0x200
            Model: "Test NVMe 1"
        }
    )",
        list);

    UNIT_ASSERT_VALUES_EQUAL(3, list.DevicesSize());

    SerializeToTextFormat(list, path);

    return {
        std::make_move_iterator(list.MutableDevices()->begin()),
        std::make_move_iterator(list.MutableDevices()->end())};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalNVMeDeviceProviderTest)
{
    Y_UNIT_TEST(ShouldListDevices)
    {
        TTempDir tempDir;
        const TString path = tempDir.Path() / "devices.txt";
        const auto expectedDevices = PrepareDeviceFile(path);

        auto deviceProvider = CreateFileNVMeDeviceProvider(path);

        auto future = deviceProvider->ListNVMeDevices();
        const auto& devices = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(expectedDevices.size(), devices.size());

        for (size_t i = 0; i != devices.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[i].DebugString(),
                devices[i].DebugString());
        }
    }
}

}   // namespace NCloud::NBlockStore
