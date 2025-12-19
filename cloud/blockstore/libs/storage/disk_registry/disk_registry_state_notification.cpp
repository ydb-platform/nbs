#include "disk_registry_state_notification.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/model/user_notification.h>

namespace NCloud::NBlockStore::NStorage::NDiskRegistry {

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DISK_STATE_MIGRATION_MESSAGE =
    "data migration in progress, slight performance decrease may be experienced";

////////////////////////////////////////////////////////////////////////////////

NProto::TUserNotification MakeBlankNotification(
    ui64 seqNo,
    TInstant timestamp)
{
    NProto::TUserNotification notif;
    notif.SetSeqNo(seqNo);
    notif.SetTimestamp(timestamp.MicroSeconds());
    return notif;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TNotificationSystem::TNotificationSystem(
        TStorageConfigPtr storageConfig,
        TVector<TString> errorNotifications,
        TVector<NProto::TUserNotification> userNotifications,
        TVector<TDiskId> disksToReallocate,
        TVector<TDiskStateUpdate> diskStateUpdates,
        ui64 diskStateSeqNo,
        TVector<TDiskId> outdatedVolumes)
    : StorageConfig(std::move(storageConfig))
    , UserNotifications(userNotifications.size() + errorNotifications.size())
    , DiskStateUpdates {std::move(diskStateUpdates)}
    , DiskStateSeqNo {diskStateSeqNo}
{
    PullInUserNotifications(
        std::move(errorNotifications),
        std::move(userNotifications));

    for (auto& diskId: disksToReallocate) {
        DisksToReallocate.emplace(std::move(diskId), DisksToReallocateSeqNo++);
    }

    for (auto& diskId: outdatedVolumes) {
        OutdatedVolumeConfigs.emplace(std::move(diskId), VolumeConfigSeqNo++);
    }
}

// TODO: Remove legacy compatibility in next release
void TNotificationSystem::PullInUserNotifications(
    TVector<TString> errorNotifications,
    TVector<NProto::TUserNotification> userNotifications)
{
    // Filter
    THashSet<TString> ids(errorNotifications.size());
    for (auto& id: errorNotifications) {
        ids.emplace(std::move(id));
    }

    // Can miss fresh notifications here, but unlikely
    for (const auto& notif: userNotifications) {
        if (notif.GetHasLegacyCopy()) {
            ids.erase(notif.GetDiskError().GetDiskId());
        }
    }

    // Merge
    for (auto& id: ids) {
        NProto::TUserNotification notif;
        notif.MutableDiskError()->SetDiskId(id);
        notif.SetHasLegacyCopy(true);
        // Leave SeqNo == 0
        userNotifications.push_back(std::move(notif));
    }

    // Transform
    for (auto& notif: userNotifications) {
        const auto& id = GetEntityId(notif);
        Y_DEBUG_ABORT_UNLESS(!id.empty());
        UserNotifications.Storage[id].Notifications.push_back(std::move(notif));
        ++UserNotifications.Count;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNotificationSystem::AllowNotifications(const TDiskId& diskId)
{
    SupportsNotifications.insert(diskId);
}

void TNotificationSystem::DeleteDisk(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    SupportsNotifications.erase(diskId);
    DeleteUserNotifications(db, diskId);

    DisksToReallocate.erase(diskId);
    db.DeleteDiskToReallocate(diskId);

    OutdatedVolumeConfigs.erase(diskId);

    db.DeleteOutdatedVolumeConfig(diskId);

    std::erase_if(DiskStateUpdates, [&] (auto& update) {
        return update.State.GetDiskId() == diskId;
    });
}

void TNotificationSystem::AddUserNotification(
    TDiskRegistryDatabase& db,
    NProto::TUserNotification notification)
{
    const auto& id = GetEntityId(notification);

    // Note: Only disk events supported at the moment
    if (!SupportsNotifications.contains(id)) {
        return;
    }

    auto& data = UserNotifications.Storage[id];
    if (data.LatestEvent == notification.GetEventCase()) {
           return; // collapse repeats
    }

    // TODO: Remove legacy compatibility in next release
    if (notification.GetEventCase()
        == NProto::TUserNotification::EventCase::kDiskError)
    {
        notification.SetHasLegacyCopy(true);
        db.AddErrorNotification(id);
    }
    db.AddUserNotification(notification);

    data.LatestEvent = notification.GetEventCase();
    data.Notifications.push_back(std::move(notification));
    ++UserNotifications.Count;
}

void TNotificationSystem::DeleteUserNotification(
    TDiskRegistryDatabase& db,
    const TString& entityId,
    ui64 seqNo)
{
    auto found = UserNotifications.Storage.find(entityId);
    if (found != UserNotifications.Storage.end()) {
        auto& notifications = found->second.Notifications;

        auto it = std::find_if(
            notifications.begin(),
            notifications.end(),
            [&seqNo] (const auto& notif) {
                return notif.GetSeqNo() == seqNo;
            });

        if (it != notifications.end()) {
            // TODO: Remove legacy compatibility in next release
            if (it->GetHasLegacyCopy()) {
                db.DeleteErrorNotification(entityId);
            }

            notifications.erase(it);
            --UserNotifications.Count;

            if (notifications.empty()) {
                UserNotifications.Storage.erase(found);
            }
        }
    }

    db.DeleteUserNotification(seqNo);
}

void TNotificationSystem::DeleteUserNotifications(
    TDiskRegistryDatabase& db,
    const TString& entityId)
{
    bool hasLegacyCopy = false;
    auto found = UserNotifications.Storage.find(entityId);
    if (found != UserNotifications.Storage.end()) {
        for (const auto& notif: found->second.Notifications) {
            if (notif.GetHasLegacyCopy()) {
                hasLegacyCopy = true;
            }
            db.DeleteUserNotification(notif.GetSeqNo());
        }

        UserNotifications.Count -= found->second.Notifications.size();
        UserNotifications.Storage.erase(found);
    }

    // TODO: Remove legacy compatibility in next release
    if (hasLegacyCopy) {
        db.DeleteErrorNotification(entityId);
    }
}

void TNotificationSystem::GetUserNotifications(
    TVector<NProto::TUserNotification>& notifications) const
{
    notifications.reserve(notifications.size() +  UserNotifications.Count);
    for (auto&& [id, data]: UserNotifications.Storage) {
        notifications.insert(
            notifications.end(),
            data.Notifications.cbegin(),
            data.Notifications.cend());
    }
}

auto TNotificationSystem::GetUserNotifications() const
    -> const TUserNotifications&
{
    return UserNotifications;
}

ui64 TNotificationSystem::AddReallocateRequest(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    db.AddDiskToReallocate(diskId);
    return AddReallocateRequest(diskId);
}

ui64 TNotificationSystem::AddReallocateRequest(const TDiskId& diskId)
{
    const auto seqNo = DisksToReallocateSeqNo++;

    DisksToReallocate[diskId] = seqNo;

    return seqNo;
}

ui64 TNotificationSystem::GetDiskSeqNo(const TDiskId& diskId) const
{
    const ui64* seqNo = DisksToReallocate.FindPtr(diskId);

    return seqNo ? *seqNo : 0;
}

auto TNotificationSystem::GetDisksToReallocate() const
    -> const THashMap<TDiskId, ui64>&
{
    return DisksToReallocate;
}

void TNotificationSystem::DeleteDiskToReallocate(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    ui64 seqNo)
{
    auto it = DisksToReallocate.find(diskId);
    if (it != DisksToReallocate.end() && it->second == seqNo) {
        DisksToReallocate.erase(it);
        db.DeleteDiskToReallocate(diskId);
    }
}

auto TNotificationSystem::GetDiskStateUpdates() const
    -> const TVector<TDiskStateUpdate>&
{
    return DiskStateUpdates;
}

NProto::TDiskState TNotificationSystem::CreateDiskState(
    const TDiskId& diskId,
    NProto::EDiskState state)
{
    NProto::TDiskState diskState;
    diskState.SetDiskId(diskId);
    diskState.SetState(state);

    if (state == NProto::DISK_STATE_WARNING) {
        diskState.SetStateMessage(DISK_STATE_MIGRATION_MESSAGE);
    }

    return diskState;
}

void TNotificationSystem::OnDiskStateChanged(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    NProto::EDiskState oldState,
    NProto::EDiskState newState,
    TInstant timestamp)
{
    NProto::TDiskState diskState = CreateDiskState(diskId, newState);

    const auto seqNo = DiskStateSeqNo++;

    db.UpdateDiskState(diskState, seqNo);
    db.WriteLastDiskStateSeqNo(DiskStateSeqNo);

    if (newState >= NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE) {
        if (oldState < NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE) {
            auto notif = MakeBlankNotification(seqNo, timestamp);
            notif.MutableDiskError()->SetDiskId(diskId);
            AddUserNotification(db, std::move(notif));
         }
    } else {
        if (oldState >= NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE) {
            auto notif = MakeBlankNotification(seqNo, timestamp);
            notif.MutableDiskBackOnline()->SetDiskId(diskId);
            AddUserNotification(db, std::move(notif));
        }
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
