#include "service_ut.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceCreateVolumeFromDeviceTest)
{
    Y_UNIT_TEST(ShouldCreateVolumeFromDevice)
    {
        constexpr ui32 blockSize = 512;
        const ui64 blockCount = 100'000'000'000 / blockSize;
        const TString agentId = "agent.yandex.cloud.net";
        const TString diskId = "local0";

        auto createDevice = [&](TString name, TString uuid)
        {
            NProto::TDeviceConfig device;

            device.SetAgentId(agentId);
            device.SetDeviceName(std::move(name));
            device.SetDeviceUUID(std::move(uuid));
            device.SetBlocksCount(blockCount);
            device.SetBlockSize(blockSize);

            return device;
        };

        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();

        diskRegistryState->Devices = {
            createDevice("/dev/nvme3n1", "uuid-1"),
            createDevice("/dev/nvme3n2", "uuid-2"),
            createDevice("/dev/nvme3n3", "uuid-3"),
            createDevice("/dev/nvme3n4", "uuid-4")};

        TTestEnv env{1, 1, 4, 1, {diskRegistryState}};

        TServiceClient service{env.GetRuntime(), SetupTestEnv(env)};

        {
            auto response =
                service.CreateVolumeFromDevice(diskId, agentId, "/dev/nvme3n3");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = service.DescribeVolume(diskId);

            const auto& volume = response->Record.GetVolume();

            UNIT_ASSERT_VALUES_EQUAL(0, volume.GetTabletVersion());
            UNIT_ASSERT_VALUES_EQUAL(1, volume.GetDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(
                agentId,
                volume.GetDevices(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                volume.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "/dev/nvme3n3",
                volume.GetDevices(0).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, volume.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(blockCount, volume.GetBlocksCount());
            UNIT_ASSERT_GT(volume.GetCreationTs(), 0);
        }

        service.DestroyVolume(diskId);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
