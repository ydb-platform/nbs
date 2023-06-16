#pragma once

#include "disk_registry_database.h"

namespace NCloud::NBlockStore::NStorage::NDiskRegistry {

////////////////////////////////////////////////////////////////////////////////

class TNotificationSystem
{
    using TDiskId = TString;

private:
    THashSet<TDiskId> SupportsNotifications;

    // notify users
    THashSet<TDiskId> ErrorNotifications;

    // notify volumes (reallocate)
    THashMap<TDiskId, ui64> DisksToNotify;
    ui64 DisksToNotifySeqNo = 1;

    // notify Compute (write events to Logbroker)
    TVector<TDiskStateUpdate> DiskStateUpdates;
    ui64 DiskStateSeqNo = 0;

    // update configs at SS
    THashMap<TDiskId, ui64> OutdatedVolumeConfigs;
    ui64 VolumeConfigSeqNo = 0;

public:
    TNotificationSystem(
        TVector<TDiskId> errorNotifications,
        TVector<TDiskId> disksToNotify,
        TVector<TDiskStateUpdate> diskStateUpdates,
        ui64 diskStateSeqNo,
        TVector<TDiskId> outdatedVolumes);

    void AllowNotifications(const TDiskId& diskId);
    void DeleteDisk(TDiskRegistryDatabase& db, const TDiskId& diskId);

    void AddErrorNotification(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);

    void DeleteErrorNotification(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);

    auto GetErrorNotifications() const -> const THashSet<TDiskId>&;

    ui64 AddDiskToNotify(TDiskRegistryDatabase& db, const TDiskId& diskId);
    ui64 AddDiskToNotify(const TDiskId& diskId);

    ui64 GetDiskSeqNo(const TDiskId& diskId) const;

    auto GetDisksToNotify() const -> const THashMap<TDiskId, ui64>&;

    void DeleteDiskToNotify(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        ui64 seqNo);

    auto GetDiskStateUpdates() const -> const TVector<TDiskStateUpdate>&;

    ui64 GetDiskStateSeqNo() const;

    void OnDiskStateChanged(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        NProto::EDiskState newState);

    void DeleteDiskStateUpdate(TDiskRegistryDatabase& db, ui64 maxSeqNo);

    void AddOutdatedVolumeConfig(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);

    auto GetOutdatedVolumeConfigs() const -> TVector<TDiskId>;

    std::optional<ui64> GetOutdatedVolumeSeqNo(const TDiskId& diskId) const;

    void DeleteOutdatedVolumeConfig(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);
};

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistry
