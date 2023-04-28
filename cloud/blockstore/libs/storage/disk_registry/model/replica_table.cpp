#include "replica_table.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TReplicaTable::AddReplica(
    const TDiskId& diskId,
    const TVector<TDeviceId>& devices)
{
    auto& diskState = Disks[diskId];
    UpdateReplica(
        diskId,
        diskState.Cells.empty() ? 0 : diskState.Cells[0].size(),
        devices);
}

void TReplicaTable::UpdateReplica(
    const TDiskId& diskId,
    const ui32 replicaNo,
    const TVector<TDeviceId>& devices)
{
    Y_VERIFY_DEBUG(devices.size());
    if (devices.empty()) {
        return;
    }

    auto& diskState = Disks[diskId];

    if (diskState.Cells.size() < devices.size()) {
        diskState.Cells.resize(devices.size());
    }

    if (diskState.Cells[0].size() < replicaNo + 1) {
        diskState.Cells[0].resize(replicaNo + 1);
    }
    for (ui32 i = 1; i < diskState.Cells.size(); ++i) {
        diskState.Cells[i].resize(diskState.Cells[0].size());
    }

    for (ui32 j = 0; j < devices.size(); ++j) {
        diskState.Cells[j][replicaNo] = {devices[j], false};
    }

    for (auto& cell: diskState.Cells) {
        diskState.DeviceId2Cell[cell[replicaNo].Id] = &cell;
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

    const auto* cell = disk->DeviceId2Cell.FindPtr(deviceId);
    if (!cell) {
        return false;
    }

    ui32 readyDeviceCount = 0;
    for (auto& device: **cell) {
        if (device.Id && device.Id != deviceId && !device.IsReplacement) {
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

    auto* cell = disk->DeviceId2Cell.FindPtr(oldDeviceId);
    if (!cell) {
        return false;
    }

    TDeviceInfo* unit = nullptr;
    for (auto& device: **cell) {
        if (device.Id == oldDeviceId) {
            unit = &device;
            break;
        }
    }

    Y_VERIFY_DEBUG(unit);
    if (!unit) {
        return false;
    }

    if (oldDeviceId != newDeviceId) {
        disk->DeviceId2Cell[newDeviceId] = *cell;
        disk->DeviceId2Cell.erase(oldDeviceId);
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

    for (const auto& cell: disk->Cells) {
        matrix.push_back(cell);
    }

    return matrix;
}

THashMap<ui32, THashMap<ui32, ui32>> TReplicaTable::CalculateReplicaCountStats() const
{
    THashMap<ui32, THashMap<ui32, ui32>> result;

    for (const auto& x: Disks) {
        const auto& disk = x.second;
        if (disk.Cells.empty()) {
            continue;
        }

        ui32 replicaCount = disk.Cells.front().size();
        ui32 incompleteCount = 0;
        for (auto& cell: disk.Cells) {
            ui32 incompleteCountInCell = 0;
            for (const auto& device: cell) {
                incompleteCountInCell += device.IsReplacement;
            }

            incompleteCount = Max(incompleteCount, incompleteCountInCell);
        }

        ++result[replicaCount][incompleteCount];
    }

    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
