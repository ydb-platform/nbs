#pragma once

#include "disk_registry_private.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceMigration
{
    TString DiskId;
    TString SourceDeviceId;

    TDeviceMigration() = default;
    TDeviceMigration(
            TString diskId,
            TString sourceDeviceId)
        : DiskId(std::move(diskId))
        , SourceDeviceId(std::move(sourceDeviceId))
    {}
};

struct TFinishedMigration
{
    TString DeviceId;
    ui64 SeqNo = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryDatabase
    : public NKikimr::NIceDb::TNiceDb
{
public:
    TDiskRegistryDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema();

    bool ReadDirtyDevices(TVector<TDirtyDevice>& dirtyDevices);

    bool ReadDiskRegistryConfig(NProto::TDiskRegistryConfig& config);
    void WriteDiskRegistryConfig(const NProto::TDiskRegistryConfig& config);

    bool ReadLastDiskStateSeqNo(ui64& lastSeqNo);
    void WriteLastDiskStateSeqNo(ui64 lastSeqNo);

    bool ReadOldAgents(TVector<NProto::TAgentConfig>& agents);
    bool ReadAgents(TVector<NProto::TAgentConfig>& agents);
    bool ReadDisks(TVector<NProto::TDiskConfig>& disks);
    bool ReadPlacementGroups(TVector<NProto::TPlacementGroupConfig>& groups);

    void UpdateDisk(const NProto::TDiskConfig& config);
    void DeleteDisk(const TString& diskId);

    void UpdateOldAgent(const NProto::TAgentConfig& config);
    void UpdateAgent(const NProto::TAgentConfig& config);

    void DeleteOldAgent(ui32 nodeId);
    void DeleteAgent(const TString& id);

    void UpdateDirtyDevice(const TString& uuid, const TString& diskId);
    void DeleteDirtyDevice(const TString& uuid);

    void UpdatePlacementGroup(const NProto::TPlacementGroupConfig& group);
    void DeletePlacementGroup(const TString& groupId);

    void UpdateDiskState(const NProto::TDiskState& state, ui64 seqNo);
    bool ReadDiskStateChanges(TVector<TDiskStateUpdate>& changes);
    void DeleteDiskStateChanges(const TString& diskId, ui64 seqNo);

    void AddBrokenDisk(const TBrokenDiskInfo& diskInfo);
    bool ReadBrokenDisks(TVector<TBrokenDiskInfo>& diskInfos);
    void DeleteBrokenDisk(const TString& diskId);

    void AddDiskToNotify(const TString& diskId);
    bool ReadDisksToNotify(TVector<TString>& diskIds);
    void DeleteDiskToNotify(const TString& diskId);

    void AddDiskToCleanup(const TString& diskId);
    bool ReadDisksToCleanup(TVector<TString>& diskIds);
    void DeleteDiskToCleanup(const TString& diskId);

    bool ReadWritableState(bool& state);
    void WriteWritableState(bool state);

    void AddErrorNotification(const TString& diskId);
    void DeleteErrorNotification(const TString& diskId);
    bool ReadErrorNotifications(TVector<TString>& diskIds);

    void AddOutdatedVolumeConfig(const TString& diskId);
    void DeleteOutdatedVolumeConfig(const TString& diskId);
    bool ReadOutdatedVolumeConfigs(TVector<TString>& diskIds);

    bool ReadSuspendedDevices(TVector<TString>& suspendedDevices);
    void UpdateSuspendedDevice(const TString& uuid);
    void DeleteSuspendedDevice(const TString& uuid);

    bool ReadRestoreState(bool& state);
    void WriteRestoreState(bool state);

    bool ReadLastBackupTs(TInstant& time);
    void WriteLastBackupTs(TInstant time);

private:
    template <typename TTable>
    bool LoadConfigs(TVector<typename TTable::Config::Type>& configs);
};

}   // namespace NCloud::NBlockStore::NStorage
