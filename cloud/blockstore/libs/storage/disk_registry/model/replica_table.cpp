#include "replica_table.h"

#include <cloud/blockstore/libs/storage/disk_registry/model/device_list.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TReplicaTable::TReplicaTable(const TDeviceList* const deviceList)
    : DeviceList(deviceList)
{}

void TReplicaTable::AddReplica(
    const TDiskId& diskId,
    const TVector<TDeviceId>& devices)
{
    auto& diskState = Disks[diskId];
    UpdateReplica(
        diskId,
        diskState.Rows.empty() ? 0 : diskState.Rows[0].size(),
        devices);
}

void TReplicaTable::UpdateReplica(
    const TDiskId& diskId,
    const ui32 replicaNo,
    const TVector<TDeviceId>& devices)
{
    Y_DEBUG_ABORT_UNLESS(devices.size());
    if (devices.empty()) {
        return;
    }

    auto& diskState = Disks[diskId];

    if (diskState.Rows.size() < devices.size()) {
        diskState.Rows.resize(devices.size());
    }

    if (diskState.Rows[0].size() < replicaNo + 1) {
        diskState.Rows[0].resize(replicaNo + 1);
    }
    for (ui32 i = 1; i < diskState.Rows.size(); ++i) {
        diskState.Rows[i].resize(diskState.Rows[0].size());
    }

    for (ui32 j = 0; j < devices.size(); ++j) {
        diskState.Rows[j][replicaNo] = {devices[j], false};
    }

    for (auto& row: diskState.Rows) {
        diskState.DeviceId2Row[row[replicaNo].Id] = &row;
    }
}

bool TReplicaTable::RemoveMirroredDisk(const TDiskId& diskId)
{
    return Disks.erase(diskId) > 0;
}

bool TReplicaTable::IsReplacementAllowed(
    const TDiskId& diskId,
    const TDeviceId& deviceId) const
{
    const auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return false;
    }

    const auto* row = disk->DeviceId2Row.FindPtr(deviceId);
    if (!row) {
        return false;
    }

    ui32 readyDeviceCount = 0;
    for (auto& device: **row) {
        const bool canUseDevice = device.Id && device.Id != deviceId;
        const bool deviceOK =
            !device.IsReplacement &&
            DeviceList->GetDeviceState(device.Id) != NProto::DEVICE_STATE_ERROR;
        if (canUseDevice && deviceOK) {
            ++readyDeviceCount;
        }
    }

    return readyDeviceCount > 0;
}

bool TReplicaTable::ReplaceDevice(
    const TDiskId& diskId,
    const TDeviceId& deviceId,
    const TDeviceId& replacementId)
{
    return ChangeDevice(diskId, deviceId, replacementId, true);
}

void TReplicaTable::MarkReplacementDevice(
    const TDiskId& diskId,
    const TDeviceId& deviceId,
    bool isReplacement)
{
    ChangeDevice(diskId, deviceId, deviceId, isReplacement);
}

bool TReplicaTable::ChangeDevice(
    const TDiskId& diskId,
    const TDeviceId& oldDeviceId,
    const TDeviceId& newDeviceId,
    bool isReplacement)
{
    auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return false;
    }

    auto* row = disk->DeviceId2Row.FindPtr(oldDeviceId);
    if (!row) {
        return false;
    }

    TDeviceInfo* unit = nullptr;
    for (auto& device: **row) {
        if (device.Id == oldDeviceId) {
            unit = &device;
            break;
        }
    }

    Y_DEBUG_ABORT_UNLESS(unit);
    if (!unit) {
        return false;
    }

    if (oldDeviceId != newDeviceId) {
        disk->DeviceId2Row[newDeviceId] = *row;
        disk->DeviceId2Row.erase(oldDeviceId);
        unit->Id = newDeviceId;
    }
    unit->IsReplacement = isReplacement;
    return true;
}

TVector<TVector<TReplicaTable::TDeviceInfo>> TReplicaTable::AsMatrix(
    const TString& diskId) const
{
    TVector<TVector<TDeviceInfo>> matrix;

    auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return {};
    }

    for (const auto& row: disk->Rows) {
        matrix.push_back(row);
    }

    return matrix;
}

TMirroredDisksStat TReplicaTable::CalculateReplicaCountStats() const
{
    TMirroredDisksStat result;
    ui32 dummyField = 0;

    auto getCounter = [&](ui32 replicaCount, ui32 incompleteCount) -> ui32&
    {
        if (replicaCount == 2) {
            switch (incompleteCount) {
                case 0:
                    return result.Mirror2DiskMinus0;
                case 1:
                    return result.Mirror2DiskMinus1;
                case 2:
                    return result.Mirror2DiskMinus2;
                default:
                    return dummyField;
            }
        } else if (replicaCount == 3) {
            switch (incompleteCount) {
                case 0:
                    return result.Mirror3DiskMinus0;
                case 1:
                    return result.Mirror3DiskMinus1;
                case 2:
                    return result.Mirror3DiskMinus2;
                case 3:
                    return result.Mirror3DiskMinus3;
                default:
                    return dummyField;
            }
        }
        return dummyField;
    };

    for (const auto& [diskId, diskState]: Disks) {
        if (diskState.Rows.empty()) {
            continue;
        }

        ui32 replicaCount = diskState.Rows.front().size();
        ui32 incompleteCount = 0;
        for (const auto& row: diskState.Rows) {
            ui32 incompleteCountInCell = 0;
            for (const auto& device: row) {
                incompleteCountInCell +=
                    device.IsReplacement ||
                    DeviceList->GetDeviceState(device.Id) ==
                        NProto::EDeviceState::DEVICE_STATE_ERROR;
            }

            incompleteCount = Max(incompleteCount, incompleteCountInCell);
        }

        ui32& counter = getCounter(replicaCount, incompleteCount);
        ++counter;
    }

    Y_DEBUG_ABORT_UNLESS(dummyField == 0);
    return result;
}

TMirroredDiskDevicesStat TReplicaTable::CalculateDiskStats(
    const TString& diskId) const
{
    TMirroredDiskDevicesStat result;
    const auto* diskState = Disks.FindPtr(diskId);
    if (!diskState) {
        return result;
    }

    for (const auto& row: diskState->Rows) {
        ui32 incompleteCountInCell = 0;
        ui32 cellReplacementCount = 0;
        ui32 cellErrorCount = 0;
        for (const auto& deviceInfo: row) {
            if (DeviceList->GetDeviceState(deviceInfo.Id) ==
                NProto::EDeviceState::DEVICE_STATE_ERROR)
            {
                ++cellErrorCount;
            } else if (deviceInfo.IsReplacement) {
                ++cellReplacementCount;
            }
        }
        ui32 replacementAndErrorCount = cellReplacementCount + cellErrorCount;
        Y_DEBUG_ABORT_UNLESS(replacementAndErrorCount < result.CellsByState.size());
        if (replacementAndErrorCount >= result.CellsByState.size()) {
            replacementAndErrorCount = result.CellsByState.size() - 1;
        }
        ++result.CellsByState[replacementAndErrorCount];

        result.DeviceReadyCount += row.size() - replacementAndErrorCount;
        result.DeviceReplacementCount += cellReplacementCount;
        result.DeviceErrorCount += cellErrorCount;
        result.MaxIncompleteness =
            Max(result.MaxIncompleteness, incompleteCountInCell);
    }
    return result;
}

TVector<TString> TReplicaTable::GetDevicesReplacements(
    const TDiskId& diskId) const
{
    const auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return {};
    }

    TVector<TDiskId> devicesReplacements;
    for (const auto& row: disk->Rows) {
        for (const auto& deviceInfo: row) {
            if (deviceInfo.IsReplacement) {
                devicesReplacements.push_back(deviceInfo.Id);
            }
        }
    }

    return devicesReplacements;
}

ui32 TReplicaTable::GetDevicesReplacementsCount(const TDiskId& diskId) const
{
    const auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return 0;
    }

    ui32 devicesReplacementsCount = 0;

    for (const auto& row: disk->Rows) {
        for (const auto& deviceInfo: row) {
            if (deviceInfo.IsReplacement) {
                devicesReplacementsCount++;
            }
        }
    }

    return devicesReplacementsCount;
}

bool TReplicaTable::IsReplacementDevice(
    const TDiskId& diskId,
    const TDeviceId& deviceId) const
{
    const auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return false;
    }

    const auto* row = disk->DeviceId2Row.FindPtr(deviceId);
    if (!row) {
        return false;
    }

    for (const auto& device: **row) {
        if (device.Id == deviceId) {
            return device.IsReplacement;
        }
    }
    Y_DEBUG_ABORT_UNLESS(false);

    return false;
}

}   // namespace NCloud::NBlockStore::NStorage
