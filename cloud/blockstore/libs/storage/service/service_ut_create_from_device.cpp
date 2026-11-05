#include "service_ut.h"

#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/string/printf.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DeviceBlockSize = 512;
constexpr ui64 DeviceBlockCount = 100'000'000'000 / DeviceBlockSize;
const TString AgentId = "agent.yandex.cloud.net";

NProto::TDeviceConfig CreateDevice(TString name, TString uuid)
{
    NProto::TDeviceConfig device;

    device.SetAgentId(AgentId);
    device.SetDeviceName(std::move(name));
    device.SetDeviceUUID(std::move(uuid));
    device.SetBlocksCount(DeviceBlockCount);
    device.SetBlockSize(DeviceBlockSize);

    return device;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceCreateVolumeFromDeviceTest)
{
    Y_UNIT_TEST(ShouldCreateVolumeFromDevice)
    {
        const TString diskId = "local0";

        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();

        diskRegistryState->Devices = {
            CreateDevice("/dev/nvme3n1", "uuid-1"),
            CreateDevice("/dev/nvme3n2", "uuid-2"),
            CreateDevice("/dev/nvme3n3", "uuid-3"),
            CreateDevice("/dev/nvme3n4", "uuid-4")
        };

        TTestEnv env {1, 1, 4, 1, {diskRegistryState}};

        TServiceClient service {env.GetRuntime(), SetupTestEnv(env)};

        {
            auto response = service.CreateVolumeFromDevice(
                diskId,
                AgentId,
                "/dev/nvme3n3");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = service.DescribeVolume(diskId);

            const auto& volume = response->Record.GetVolume();

            UNIT_ASSERT_VALUES_EQUAL(0, volume.GetTabletVersion());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL),
                static_cast<int>(volume.GetStorageMediaKind()));
            UNIT_ASSERT_VALUES_EQUAL(1, volume.GetDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(AgentId, volume.GetDevices(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", volume.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("/dev/nvme3n3", volume.GetDevices(0).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(DeviceBlockSize, volume.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(DeviceBlockCount, volume.GetBlocksCount());
            UNIT_ASSERT_GT(volume.GetCreationTs(), 0);
        }

        service.DestroyVolume(diskId);
    }

    Y_UNIT_TEST(ShouldCreateVolumeFromDeviceWithStorageMediaKind)
    {
        struct TTestCase
        {
            NCloud::NProto::EStorageMediaKind MediaKind;
            ui32 BlockSize;
        };

        const TVector<TTestCase> testCases = {
            {NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL, DefaultLocalSSDBlockSize},
            {NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL, DefaultBlockSize},
            {NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED, DefaultBlockSize},
            {NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED, DefaultBlockSize},
        };

        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();

        for (size_t i = 0; i != testCases.size(); ++i) {
            diskRegistryState->Devices.push_back(CreateDevice(
                Sprintf("/dev/nvme3n%zu", i + 1),
                Sprintf("uuid-%zu", i + 1)));
        }

        TTestEnv env {1, 1, 4, 1, {diskRegistryState}};

        TServiceClient service {env.GetRuntime(), SetupTestEnv(env)};

        for (size_t i = 0; i != testCases.size(); ++i) {
            const auto& testCase = testCases[i];
            const TString diskId = Sprintf("disk-%zu", i + 1);

            service.CreateVolumeFromDevice(
                diskId,
                AgentId,
                Sprintf("/dev/nvme3n%zu", i + 1),
                testCase.MediaKind);

            auto response = service.DescribeVolume(diskId);

            const auto& volume = response->Record.GetVolume();

            UNIT_ASSERT_VALUES_EQUAL_C(
                static_cast<int>(testCase.MediaKind),
                static_cast<int>(volume.GetStorageMediaKind()),
                diskId);
            UNIT_ASSERT_VALUES_EQUAL(1, volume.GetDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(
                Sprintf("uuid-%zu", i + 1),
                volume.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(testCase.BlockSize, volume.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(
                DeviceBlockCount * DeviceBlockSize / testCase.BlockSize,
                volume.GetBlocksCount());

            service.DestroyVolume(diskId);
        }
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedMediaKinds)
    {
        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();

        diskRegistryState->Devices = {
            CreateDevice("/dev/nvme3n1", "uuid-1"),
        };

        TTestEnv env {1, 1, 4, 1, {diskRegistryState}};

        TServiceClient service {env.GetRuntime(), SetupTestEnv(env)};

        for (auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_SSD,
              NCloud::NProto::STORAGE_MEDIA_HYBRID,
              NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2,
              NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
              NCloud::NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP})
        {
            service.SendCreateVolumeFromDeviceRequest(
                "disk-1",
                AgentId,
                "/dev/nvme3n1",
                mediaKind);

            auto response = service.RecvCreateVolumeFromDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                MediaKindToString(mediaKind));
        }

        UNIT_ASSERT(diskRegistryState->Disks.empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
