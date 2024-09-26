#pragma once

#include "disk_registry_database.h"

namespace NCloud::NBlockStore::NStorage::NDiskRegistry {

////////////////////////////////////////////////////////////////////////////////

class TNotificationSystem
{
    using TDiskId = TString;

    struct TUserNotifications
    {
        struct TPerEntityData
        {
            TVector<NProto::TUserNotification> Notifications;
            // Protect from duplicate AddUserNotification() calls, just in case
            NProto::TUserNotification::EventCase LatestEvent =
                NProto::TUserNotification::EventCase::EVENT_NOT_SET;
        };

        THashMap<TString, TPerEntityData> Storage;
        size_t Count = 0;

        TUserNotifications() = default;

        TUserNotifications(size_t sizeHint)
            : Storage(sizeHint)
        {}
    };

private:
    const TStorageConfigPtr StorageConfig;

    THashSet<TDiskId> SupportsNotifications;

    // notify users
    TUserNotifications UserNotifications;

    // // notify volumes (reallocate)
    // THashMap<TDiskId, ui64> DisksToReallocate;
    // ui64 DisksToReallocateSeqNo = 1;

    // // notify volumes (change disk agent node id)
    // THashMap<TDiskId, ui64> DisksToChangeNodeId;
    // ui64 DisksToChangeNodeIdSeqNo = 1;

    // struct TDiskNotificationData
    // {
    //     THashMap<TDiskId, ui64> DisksToNotify;

    // };

    using TDisksToNotify = THashMap<TDiskId, ui64>;
    static constexpr size_t NotificationTypeCount =
        NProto::EDiskNotificationType_ARRAYSIZE;
    std::array<TDisksToNotify, NotificationTypeCount> DisksToNotify;
    ui64 DisksToNotifySeqNo = 1;

    // notify Compute (write events to Logbroker)
    TVector<TDiskStateUpdate> DiskStateUpdates;
    ui64 DiskStateSeqNo = 0;

    // update configs at SS
    THashMap<TDiskId, ui64> OutdatedVolumeConfigs;
    ui64 VolumeConfigSeqNo = 0;

public:
    TNotificationSystem(
        TStorageConfigPtr storageConfig,
        TVector<TString> errorNotifications,
        TVector<NProto::TUserNotification> userNotifications,
        TVector<NProto::TDiskNotification> disksToNotify,
        TVector<TDiskStateUpdate> diskStateUpdates,
        ui64 diskStateSeqNo,
        TVector<TDiskId> outdatedVolumes);

    void AllowNotifications(const TDiskId& diskId);
    void DeleteDisk(TDiskRegistryDatabase& db, const TDiskId& diskId);

    void AddUserNotification(
        TDiskRegistryDatabase& db,
        NProto::TUserNotification notification);

    void DeleteUserNotification(
        TDiskRegistryDatabase& db,
        const TString& entityId,
        ui64 seqNo);

    void DeleteUserNotifications(
        TDiskRegistryDatabase& db,
        const TString& entityId);

    void GetUserNotifications(
        TVector<NProto::TUserNotification>& notifications) const;

    auto GetUserNotifications() const -> const TUserNotifications&;

    ui64 AddReallocateRequest(TDiskRegistryDatabase& db, const TDiskId& diskId);
    ui64 AddReallocateRequest(const TDiskId& diskId);
    ui64 GetDiskToReallocateSeqNo(const TDiskId& diskId) const;
    auto GetDisksToReallocate() const -> const THashMap<TDiskId, ui64>&;
    void DeleteDiskToReallocate(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        ui64 seqNo);

    ui64 AddChangeNodeIdRequest(TDiskRegistryDatabase& db, const TDiskId& diskId);
    ui64 AddChangeNodeIdRequest(const TDiskId& diskId);
    ui64 GetChangeNodeIdSeqNo(const TDiskId& diskId) const;
    auto GetDisksToChangeNodeId() const -> const THashMap<TDiskId, ui64>&;
    void DeleteDiskToChangeNodeId(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        ui64 seqNo);

    auto GetDiskStateUpdates() const -> const TVector<TDiskStateUpdate>&;

    ui64 GetDiskStateSeqNo() const;

    void OnDiskStateChanged(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        NProto::EDiskState oldState,
        NProto::EDiskState newState,
        TInstant timestamp);

    void DeleteDiskStateUpdate(TDiskRegistryDatabase& db, ui64 maxSeqNo);

    void AddOutdatedVolumeConfig(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);

    auto GetOutdatedVolumeConfigs() const -> TVector<TDiskId>;

    std::optional<ui64> GetOutdatedVolumeSeqNo(const TDiskId& diskId) const;

    void DeleteOutdatedVolumeConfig(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);

private:
    void PullInUserNotifications(
        TVector<TString> errorNotifications,
        TVector<NProto::TUserNotification> userNotifications);
};

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistry
