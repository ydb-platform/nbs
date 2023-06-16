#include "disk_registry_state_notification.h"

namespace NCloud::NBlockStore::NStorage::NDiskRegistry {

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DISK_STATE_MIGRATION_MESSAGE =
    "data migration in progress, slight performance decrease may be experienced";

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TNotificationSystem::TNotificationSystem(
        TVector<TDiskId> errorNotifications,
        TVector<TDiskId> disksToNotify,
        TVector<TDiskStateUpdate> diskStateUpdates,
        ui64 diskStateSeqNo,
        TVector<TDiskId> outdatedVolumes)
    : DiskStateUpdates {std::move(diskStateUpdates)}
    , DiskStateSeqNo {diskStateSeqNo}
{
    for (auto& diskId: errorNotifications) {
        ErrorNotifications.insert(std::move(diskId));
    }

    for (auto& diskId: disksToNotify) {
        DisksToNotify.emplace(std::move(diskId), DisksToNotifySeqNo++);
    }

    for (auto& diskId: outdatedVolumes) {
        OutdatedVolumeConfigs.emplace(std::move(diskId), VolumeConfigSeqNo++);
    }
}

void TNotificationSystem::AllowNotifications(const TDiskId& diskId)
{
    SupportsNotifications.insert(diskId);
}

void TNotificationSystem::DeleteDisk(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    SupportsNotifications.erase(diskId);
    ErrorNotifications.erase(diskId);

    DisksToNotify.erase(diskId);
    db.DeleteDiskToNotify(diskId);

    OutdatedVolumeConfigs.erase(diskId);

    db.DeleteOutdatedVolumeConfig(diskId);

    std::erase_if(DiskStateUpdates, [&] (auto& update) {
        return update.State.GetDiskId() == diskId;
    });
}

void TNotificationSystem::AddErrorNotification(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    if (!SupportsNotifications.contains(diskId)) {
        return;
    }

    db.AddErrorNotification(diskId);
    ErrorNotifications.emplace(diskId);
}

void TNotificationSystem::DeleteErrorNotification(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    ErrorNotifications.erase(diskId);
    db.DeleteErrorNotification(diskId);
}

auto TNotificationSystem::GetErrorNotifications() const
    -> const THashSet<TDiskId>&
{
    return ErrorNotifications;
}

ui64 TNotificationSystem::AddDiskToNotify(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    db.AddDiskToNotify(diskId);
    return AddDiskToNotify(diskId);
}

ui64 TNotificationSystem::AddDiskToNotify(const TDiskId& diskId)
{
    const auto seqNo = DisksToNotifySeqNo++;

    DisksToNotify[diskId] = seqNo;

    return seqNo;
}

ui64 TNotificationSystem::GetDiskSeqNo(const TDiskId& diskId) const
{
    const ui64* seqNo = DisksToNotify.FindPtr(diskId);

    return seqNo ? *seqNo : 0;
}

auto TNotificationSystem::GetDisksToNotify() const
    -> const THashMap<TDiskId, ui64>&
{
    return DisksToNotify;
}

void TNotificationSystem::DeleteDiskToNotify(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    ui64 seqNo)
{
    auto it = DisksToNotify.find(diskId);
    if (it != DisksToNotify.end() && it->second == seqNo) {
        DisksToNotify.erase(it);
        db.DeleteDiskToNotify(diskId);
    }
}

auto TNotificationSystem::GetDiskStateUpdates() const
    -> const TVector<TDiskStateUpdate>&
{
    return DiskStateUpdates;
}

void TNotificationSystem::OnDiskStateChanged(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    NProto::EDiskState newState)
{
    NProto::TDiskState diskState;
    diskState.SetDiskId(diskId);
    diskState.SetState(newState);

    if (newState == NProto::DISK_STATE_MIGRATION) {
        diskState.SetStateMessage(DISK_STATE_MIGRATION_MESSAGE);
    }

    const auto seqNo = DiskStateSeqNo++;

    db.UpdateDiskState(diskState, seqNo);
    db.WriteLastDiskStateSeqNo(DiskStateSeqNo);

    if (newState >= NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE) {
        AddErrorNotification(db, diskId);
    }

    if (SupportsNotifications.contains(diskId)) {
        DiskStateUpdates.emplace_back(std::move(diskState), seqNo);
    }
}

void TNotificationSystem::DeleteDiskStateUpdate(
    TDiskRegistryDatabase& db,
    ui64 maxSeqNo)
{
    auto begin = DiskStateUpdates.cbegin();
    auto it = begin;

    for (; it != DiskStateUpdates.cend(); ++it) {
        if (it->SeqNo > maxSeqNo) {
            break;
        }

        db.DeleteDiskStateChanges(it->State.GetDiskId(), it->SeqNo);
    }

    DiskStateUpdates.erase(begin, it);
}

ui64 TNotificationSystem::GetDiskStateSeqNo() const
{
    return DiskStateSeqNo;
}

void TNotificationSystem::DeleteOutdatedVolumeConfig(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    db.DeleteOutdatedVolumeConfig(diskId);
    OutdatedVolumeConfigs.erase(diskId);
}

auto TNotificationSystem::GetOutdatedVolumeConfigs() const -> TVector<TDiskId>
{
    TVector<TDiskId> diskIds;

    for (auto& kv: OutdatedVolumeConfigs) {
        diskIds.emplace_back(kv.first);
    }

    return diskIds;
}

std::optional<ui64> TNotificationSystem::GetOutdatedVolumeSeqNo(
    const TDiskId& diskId) const
{
    auto it = OutdatedVolumeConfigs.find(diskId);
    if (it == OutdatedVolumeConfigs.end()) {
        return {};
    }

    return it->second;
}

void TNotificationSystem::AddOutdatedVolumeConfig(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    OutdatedVolumeConfigs[diskId] = VolumeConfigSeqNo++;
    db.AddOutdatedVolumeConfig(diskId);
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistry
