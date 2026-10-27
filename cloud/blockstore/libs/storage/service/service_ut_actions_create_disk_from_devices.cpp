#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DeviceBlockSize = 512;
constexpr ui32 LocalVolumeBlockSize = 512;
constexpr ui32 ReplicatedVolumeBlockSize = 4096;
constexpr ui64 DeviceSize = 1_GB;
constexpr ui64 DeviceBlockCount = DeviceSize / DeviceBlockSize;
const TString DiskId = "Disk-1";

TDiskRegistryStatePtr CreateDiskRegistryState(ui64 unadjustedBlockCount)
{
    auto state = MakeIntrusive<TDiskRegistryState>();
    for (const auto* uuid: {"uuid1", "uuid42", "uuid-spare"}) {
        auto& device = state->Devices.emplace_back();
        device.SetDeviceUUID(uuid);
        device.SetDeviceName(uuid);
        device.SetBlockSize(DeviceBlockSize);
        device.SetBlocksCount(DeviceBlockCount);
        device.SetUnadjustedBlockCount(unadjustedBlockCount);
        device.SetNodeId(1);
    }
    return state;
}

NProto::TStorageServiceConfig CreateConfig()
{
    NProto::TStorageServiceConfig config;
    config.SetAllocationUnitNonReplicatedSSD(1);
    config.SetMultipartitionVolumesEnabled(true);
    config.SetBytesPerPartition(999_TB);
    config.SetBytesPerStripe(16_MB);
    return config;
}

struct TCreateDiskFromDevicesTestEnv
{
    const ui32 VolumeBlockSize;
    const ui64 VolumeBlockCount;
    const TDiskRegistryStatePtr DiskRegistryState;
    TTestEnv Env;
    const ui32 NodeIdx;
    TServiceClient Service;

    explicit TCreateDiskFromDevicesTestEnv(
        ui32 volumeBlockSize = LocalVolumeBlockSize,
        ui64 unadjustedBlockCount = DeviceBlockCount)
        : VolumeBlockSize(volumeBlockSize)
        , VolumeBlockCount(2 * DeviceSize / VolumeBlockSize)
        , DiskRegistryState(CreateDiskRegistryState(unadjustedBlockCount))
        , Env(1, 1, 4, 1, {DiskRegistryState})
        , NodeIdx(SetupTestEnv(Env, CreateConfig()))
        , Service(Env.GetRuntime(), NodeIdx)
    {}

    void CreateLocalVolume()
    {
        Service.CreateVolume(
            DiskId,
            VolumeBlockCount,
            VolumeBlockSize,
            "",  // folderId
            "",  // cloudId
            NProto::STORAGE_MEDIA_SSD_LOCAL);

        const auto response = Service.DescribeVolume(DiskId);
        const auto& volume = response->Record.GetVolume();
        UNIT_ASSERT_VALUES_EQUAL(VolumeBlockSize, volume.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(VolumeBlockCount, volume.GetBlocksCount());
        UNIT_ASSERT(
            volume.GetStorageMediaKind() == NProto::STORAGE_MEDIA_SSD_LOCAL);
        UNIT_ASSERT_VALUES_EQUAL(2, volume.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL("uuid1", volume.GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid42", volume.GetDevices(1).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            DeviceSize / VolumeBlockSize,
            volume.GetDevices(0).GetBlockCount());
    }

    void RemoveDiskRegistryEntry()
    {
        auto& runtime = Env.GetRuntime();
        runtime.EnableScheduleForActor(MakeDiskRegistryProxyServiceId());
        const auto sender = runtime.AllocateEdgeActor(NodeIdx);

        auto request =
            std::make_unique<TEvDiskRegistry::TEvDeallocateDiskRequest>();
        request->Record.SetDiskId(DiskId);
        runtime.Send(
            new NActors::IEventHandle(
                MakeDiskRegistryProxyServiceId(),
                sender,
                request.release()),
            NodeIdx);

        TAutoPtr<NActors::IEventHandle> handle;
        const auto* response = runtime.GrabEdgeEventRethrow<
            TEvDiskRegistry::TEvDeallocateDiskResponse>(handle);
        UNIT_ASSERT_C(!HasError(response->GetError()), response->GetError());
        UNIT_ASSERT(!DiskRegistryState->Disks.contains(DiskId));
    }

    auto CreateDiskFromDevices(const TVector<TString>& deviceIds)
    {
        NProto::TCreateVolumeFromDevicesRequest request;
        request.SetDiskId(DiskId);
        request.SetBlockSize(VolumeBlockSize);
        request.SetDoNotCreateTabletIfExists(true);
        for (const auto& deviceId: deviceIds) {
            request.AddDeviceUUIDs(deviceId);
        }

        TString input;
        UNIT_ASSERT(
            google::protobuf::util::MessageToJsonString(request, &input).ok());
        Service.SendExecuteActionRequest("creatediskfromdevices", input);
        return Service.RecvExecuteActionResponse();
    }
};

void TestRecoverDevicesWithUnadjustedBlockCount(ui32 volumeBlockSize)
{
    // The original allocation uses the adjusted size; recovery in the real
    // Disk Registry restores UnadjustedBlockCount for these same devices.
    constexpr ui64 unadjustedBlockCount = DeviceBlockCount + 8;
    TCreateDiskFromDevicesTestEnv env(volumeBlockSize, unadjustedBlockCount);
    env.CreateLocalVolume();
    env.RemoveDiskRegistryEntry();

    const auto response = env.CreateDiskFromDevices({"uuid1", "uuid42"});
    UNIT_ASSERT_C(!HasError(response->GetError()), response->GetError());

    const auto describe = env.Service.DescribeVolume(DiskId);
    const auto& volume = describe->Record.GetVolume();
    UNIT_ASSERT_VALUES_EQUAL(env.VolumeBlockSize, volume.GetBlockSize());
    UNIT_ASSERT_VALUES_EQUAL(env.VolumeBlockCount, volume.GetBlocksCount());
    UNIT_ASSERT_VALUES_EQUAL(2, volume.DevicesSize());
    UNIT_ASSERT_VALUES_EQUAL("uuid1", volume.GetDevices(0).GetDeviceUUID());
    UNIT_ASSERT_VALUES_EQUAL("uuid42", volume.GetDevices(1).GetDeviceUUID());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceCreateDiskFromDevicesTest)
{
    SIMPLE_UNIT_FORKED_TEST(ShouldRejectMultipartitionVolume)
    {
        TCreateDiskFromDevicesTestEnv env(ReplicatedVolumeBlockSize);
        env.Service.CreateVolume(
            DiskId,
            env.VolumeBlockCount,
            env.VolumeBlockSize,
            "",  // folderId
            "",  // cloudId
            NProto::STORAGE_MEDIA_SSD,
            NProto::TVolumePerformanceProfile(),
            "",  // placementGroupId
            0,   // placementPartitionIndex
            2);  // partitionsCount

        const auto describe = env.Service.DescribeVolume(DiskId);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            describe->Record.GetVolume().GetPartitionsCount());
        UNIT_ASSERT_VALUES_EQUAL(
            ReplicatedVolumeBlockSize,
            describe->Record.GetVolume().GetBlockSize());
        UNIT_ASSERT(!env.DiskRegistryState->Disks.contains(DiskId));

        // An incompatible volume must be rejected before sending ReallocateDisk,
        // whose allocation request assumes exactly one partition.
        const auto response = env.CreateDiskFromDevices({"uuid1", "uuid42"});
        UNIT_ASSERT_C(HasError(response->GetError()), response->GetError());
    }

    Y_UNIT_TEST(ShouldRejectIncompleteDeviceList)
    {
        TCreateDiskFromDevicesTestEnv env;
        env.CreateLocalVolume();
        env.RemoveDiskRegistryEntry();

        const auto response = env.CreateDiskFromDevices({"uuid1"});
        UNIT_ASSERT_C(
            HasError(response->GetError()),
            "Recovery with a missing device succeeded");

        // A rejected recovery must not replace the missing device with a spare.
        const auto describe = env.Service.DescribeVolume(DiskId);
        const auto& volume = describe->Record.GetVolume();
        UNIT_ASSERT_VALUES_EQUAL(2, volume.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL("uuid1", volume.GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid42", volume.GetDevices(1).GetDeviceUUID());
    }

    Y_UNIT_TEST(ShouldRecoverDevicesWithUnadjustedBlockCount)
    {
        TestRecoverDevicesWithUnadjustedBlockCount(LocalVolumeBlockSize);
    }

    Y_UNIT_TEST(ShouldRecoverDevicesWithUnadjustedBlockCountAndLargerVolumeBlocks)
    {
        TestRecoverDevicesWithUnadjustedBlockCount(ReplicatedVolumeBlockSize);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
