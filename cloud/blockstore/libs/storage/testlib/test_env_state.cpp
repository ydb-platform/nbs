#include "test_env_state.h"

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TDeviceConfig* FindDeviceConfig(
    TDiskRegistryState::TDisk& disk,
    const TString& deviceUUID)
{
    auto deviceMatcher = [deviceUUID](const auto& deviceConfig)
    {
        return deviceConfig.GetDeviceUUID() == deviceUUID;
    };

    NProto::TDeviceConfig* device = FindIfPtr(disk.Devices, deviceMatcher);
    for (auto& replica: disk.Replicas) {
        device = device ? device : FindIfPtr(replica, deviceMatcher);
    }
    return device;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const NProto::TDeviceConfig* TDiskRegistryState::AllocateNextDevice(
    i32 prevNodeId)
{
    for (int i = 0; i < Devices.ysize(); i++) {
        if (DeviceIsAllocated[i]) {
            continue;
        }

        if (AllocateDiskReplicasOnDifferentNodes &&
            static_cast<i32>(Devices[i].GetNodeId()) <= prevNodeId)
        {
            continue;
        }

        DeviceIsAllocated[i] = true;
        return &Devices[i];
    }

    return nullptr;
}

bool TDiskRegistryState::ReplaceDevice(
    const TString& diskId,
    const TString& deviceUUID)
{
    auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return false;
    }

    auto* device = FindDeviceConfig(*disk, deviceUUID);
    if (!device) {
        return false;
    }

    const auto* newDevice = AllocateNextDevice(-1);
    if (!newDevice) {
        return false;
    }

    *device = *newDevice;
    DeviceReplacementUUIDs.insert(newDevice->GetDeviceUUID());
    return true;
}

bool TDiskRegistryState::StartDeviceMigration(const TString& deviceUUID)
{
    const auto* newDevice = AllocateNextDevice(-1);
    if (!newDevice) {
        return false;
    }

    MigrationDevices[deviceUUID] = *newDevice;
    MigrationMode = EMigrationMode::InProgress;
    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
