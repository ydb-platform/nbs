#include "disk_registry_database.h"

#include "disk_registry_schema.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum EDiskRegistryConfigKey
{
    DISK_REGISTRY_CONFIG = 1,
    LAST_DISK_STATE_SEQ_NO = 2,
    WRITABLE_STATE = 3,
    RESTORE_STATE = 4,
    LAST_BACKUP_TS = 5,
};

}   // namespace

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryDatabase::InitSchema()
{
    Materialize<TDiskRegistrySchema>();

    TSchemaInitializer<TDiskRegistrySchema::TTables>::InitStorage(
        Database.Alter());
}

template <typename TTable>
bool TDiskRegistryDatabase::LoadConfigs(
    TVector<typename TTable::Config::Type>& configs)
{
    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        configs.emplace_back(it.template GetValue<typename TTable::Config>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TDiskRegistryDatabase::ReadDirtyDevices(TVector<TDirtyDevice>& dirtyDevices)
{
    using TTable = TDiskRegistrySchema::DirtyDevices;

    auto it = Table<TTable>()
        .Range()
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        dirtyDevices.emplace_back(TDirtyDevice {
            it.GetValue<TTable::Id>(),
            it.GetValue<TTable::DiskId>()
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TDiskRegistryDatabase::ReadAgents(TVector<NProto::TAgentConfig>& agents)
{
    return LoadConfigs<TDiskRegistrySchema::AgentById>(agents);
}

bool TDiskRegistryDatabase::ReadDisks(TVector<NProto::TDiskConfig>& disks)
{
    return LoadConfigs<TDiskRegistrySchema::Disks>(disks);
}

bool TDiskRegistryDatabase::ReadPlacementGroups(TVector<NProto::TPlacementGroupConfig>& groups)
{
    return LoadConfigs<TDiskRegistrySchema::PlacementGroups>(groups);
}

bool TDiskRegistryDatabase::ReadDiskRegistryConfig(NProto::TDiskRegistryConfig& config)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    auto it = Table<TTable>()
        .Key(DISK_REGISTRY_CONFIG)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        config = it.GetValue<TTable::Config>();
    }

    return true;
}

bool TDiskRegistryDatabase::ReadLastDiskStateSeqNo(ui64& lastSeqNo)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    auto it = Table<TTable>()
        .Key(LAST_DISK_STATE_SEQ_NO)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    lastSeqNo = it.IsValid()
        ? it.GetValue<TTable::Config>().GetLastDiskStateSeqNo()
        : 0;

    return true;
}

void TDiskRegistryDatabase::WriteDiskRegistryConfig(
    const NProto::TDiskRegistryConfig& config)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    Table<TTable>()
        .Key(DISK_REGISTRY_CONFIG)
        .Update<TTable::Config>(config);
}

void TDiskRegistryDatabase::WriteLastDiskStateSeqNo(ui64 lastSeqNo)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    NProto::TDiskRegistryConfig config;
    config.SetLastDiskStateSeqNo(lastSeqNo);

    Table<TTable>()
        .Key(LAST_DISK_STATE_SEQ_NO)
        .Update<TTable::Config>(config);
}

void TDiskRegistryDatabase::UpdateDisk(const NProto::TDiskConfig& config)
{
    using TTable = TDiskRegistrySchema::Disks;
    Table<TTable>()
        .Key(config.GetDiskId())
        .Update<TTable::Config>(config);
}

void TDiskRegistryDatabase::DeleteDisk(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::Disks;
    Table<TTable>()
        .Key(diskId)
        .Delete();
}

void TDiskRegistryDatabase::UpdateAgent(const NProto::TAgentConfig& config)
{
    using TTable = TDiskRegistrySchema::AgentById;
    Table<TTable>()
        .Key(config.GetAgentId())
        .Update<TTable::Config>(config);
}

void TDiskRegistryDatabase::DeleteAgent(const TString& id)
{
    using TTable = TDiskRegistrySchema::AgentById;
    Table<TTable>()
        .Key(id)
        .Delete();
}

void TDiskRegistryDatabase::UpdateDirtyDevice(
    const TString& uuid,
    const TString& diskId)
{
    using TTable = TDiskRegistrySchema::DirtyDevices;

    Table<TTable>()
        .Key(uuid)
        .Update<TTable::DiskId>(diskId);
}

void TDiskRegistryDatabase::DeleteDirtyDevice(const TString& uuid)
{
    using TTable = TDiskRegistrySchema::DirtyDevices;

    Table<TTable>()
        .Key(uuid)
        .Delete();
}

void TDiskRegistryDatabase::UpdatePlacementGroup(const NProto::TPlacementGroupConfig& config)
{
    using TTable = TDiskRegistrySchema::PlacementGroups;
    Table<TTable>()
        .Key(config.GetGroupId())
        .Update<TTable::Config>(config);
}

void TDiskRegistryDatabase::DeletePlacementGroup(const TString& groupId)
{
    using TTable = TDiskRegistrySchema::PlacementGroups;
    Table<TTable>()
        .Key(groupId)
        .Delete();
}

void TDiskRegistryDatabase::UpdateDiskState(
    const NProto::TDiskState& state,
    ui64 seqNo)
{
    using TTable = TDiskRegistrySchema::DiskStateChanges;
    Table<TTable>()
        .Key(seqNo, state.GetDiskId())
        .Update<TTable::State>(state);
}

bool TDiskRegistryDatabase::ReadDiskStateChanges(
    TVector<TDiskStateUpdate>& changes)
{
    using TTable = TDiskRegistrySchema::DiskStateChanges;

    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        changes.emplace_back(
            it.GetValue<TTable::State>(),
            it.GetValue<TTable::SeqNo>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TDiskRegistryDatabase::DeleteDiskStateChanges(const TString& diskId, ui64 seqNo)
{
    using TTable = TDiskRegistrySchema::DiskStateChanges;
    Table<TTable>()
        .Key(seqNo, diskId)
        .Delete();
}

void TDiskRegistryDatabase::AddBrokenDisk(const TBrokenDiskInfo& diskInfo)
{
    using TTable = TDiskRegistrySchema::BrokenDisks;
    Table<TTable>()
        .Key(diskInfo.DiskId)
        .Update<TTable::TsToDestroy>(diskInfo.TsToDestroy.MicroSeconds());
}

bool TDiskRegistryDatabase::ReadBrokenDisks(TVector<TBrokenDiskInfo>& diskInfos)
{
    using TTable = TDiskRegistrySchema::BrokenDisks;

    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        diskInfos.push_back({
            it.GetValue<TTable::Id>(),
            TInstant::MicroSeconds(it.GetValue<TTable::TsToDestroy>())
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    Sort(
        diskInfos.begin(),
        diskInfos.end(),
        [] (const TBrokenDiskInfo& l, const TBrokenDiskInfo& r) {
            return l.TsToDestroy < r.TsToDestroy;
        }
    );

    return true;
}

void TDiskRegistryDatabase::DeleteBrokenDisk(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::BrokenDisks;
    Table<TTable>()
        .Key(diskId)
        .Delete();
}

void TDiskRegistryDatabase::AddDiskToReallocate(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::DisksToNotify;
    Table<TTable>()
        .Key(diskId)
        .Update();
}

bool TDiskRegistryDatabase::ReadDisksToReallocate(TVector<TString>& diskIds)
{
    using TTable = TDiskRegistrySchema::DisksToNotify;

    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        diskIds.push_back(it.GetValue<TTable::Id>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TDiskRegistryDatabase::DeleteDiskToReallocate(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::DisksToNotify;
    Table<TTable>()
        .Key(diskId)
        .Delete();
}

void TDiskRegistryDatabase::AddDiskToCleanup(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::DisksToCleanup;
    Table<TTable>()
        .Key(diskId)
        .Update();
}

void TDiskRegistryDatabase::DeleteDiskToCleanup(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::DisksToCleanup;
    Table<TTable>()
        .Key(diskId)
        .Delete();
}

bool TDiskRegistryDatabase::ReadDisksToCleanup(TVector<TString>& diskIds)
{
    using TTable = TDiskRegistrySchema::DisksToCleanup;
    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        diskIds.push_back(it.GetValue<TTable::Id>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TDiskRegistryDatabase::ReadWritableState(bool& state)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    auto it = Table<TTable>()
        .Key(WRITABLE_STATE)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    state = it.IsValid()
        ? it.GetValue<TTable::Config>().GetWritableState()
        : false;

    return true;
}

void TDiskRegistryDatabase::WriteWritableState(bool state)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    NProto::TDiskRegistryConfig config;
    config.SetWritableState(state);

    Table<TTable>()
        .Key(WRITABLE_STATE)
        .Update<TTable::Config>(config);
}

void TDiskRegistryDatabase::AddErrorNotification(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::ErrorNotifications;

    Table<TTable>()
        .Key(diskId)
        .Update();
}

void TDiskRegistryDatabase::DeleteErrorNotification(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::ErrorNotifications;

    Table<TTable>()
        .Key(diskId)
        .Delete();
}

bool TDiskRegistryDatabase::ReadErrorNotifications(TVector<TString>& diskIds)
{
    using TTable = TDiskRegistrySchema::ErrorNotifications;

    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        diskIds.push_back(it.GetValue<TTable::Id>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TDiskRegistryDatabase::AddUserNotification(
    const NProto::TUserNotification& notification)
{
    using TTable = TDiskRegistrySchema::UserNotifications;

    Table<TTable>()
        .Key(notification.GetSeqNo())
        .Update<TTable::Notification>(notification);
}

void TDiskRegistryDatabase::DeleteUserNotification(ui64 seqNo)
{
    using TTable = TDiskRegistrySchema::UserNotifications;

    Table<TTable>()
        .Key(seqNo)
        .Delete();
}

bool TDiskRegistryDatabase::ReadUserNotifications(
    TVector<NProto::TUserNotification>& notifications)
{
    using TTable = TDiskRegistrySchema::UserNotifications;

    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        notifications.push_back(it.GetValue<TTable::Notification>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TDiskRegistryDatabase::AddOutdatedVolumeConfig(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::OutdatedVolumeConfigs;

    Table<TTable>()
        .Key(diskId)
        .Update();
}

void TDiskRegistryDatabase::DeleteOutdatedVolumeConfig(const TString& diskId)
{
    using TTable = TDiskRegistrySchema::OutdatedVolumeConfigs;

    Table<TTable>()
        .Key(diskId)
        .Delete();
}

bool TDiskRegistryDatabase::ReadOutdatedVolumeConfigs(TVector<TString>& diskIds)
{
    using TTable = TDiskRegistrySchema::OutdatedVolumeConfigs;

    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        diskIds.push_back(it.GetValue<TTable::Id>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

bool TDiskRegistryDatabase::ReadSuspendedDevices(
    TVector<NProto::TSuspendedDevice>& suspendedDevices)
{
    using TTable = TDiskRegistrySchema::SuspendedDevices;

    auto it = Table<TTable>()
        .Range()
        .Select<TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        suspendedDevices.emplace_back(it.GetValue<TTable::Config>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TDiskRegistryDatabase::UpdateSuspendedDevice(
    const NProto::TSuspendedDevice& device)
{
    using TTable = TDiskRegistrySchema::SuspendedDevices;

    Table<TTable>()
        .Key(device.GetId())
        .Update<TTable::Config>(device);
}

void TDiskRegistryDatabase::DeleteSuspendedDevice(const TString& uuid)
{
    using TTable = TDiskRegistrySchema::SuspendedDevices;

    Table<TTable>()
        .Key(uuid)
        .Delete();
}

bool TDiskRegistryDatabase::ReadRestoreState(bool& state)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    auto it = Table<TTable>()
        .Key(RESTORE_STATE)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    state = it.IsValid()
        ? it.GetValue<TTable::Config>().GetRestoreState()
        : false;

    return true;
}

void TDiskRegistryDatabase::WriteRestoreState(bool state)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    NProto::TDiskRegistryConfig config;
    config.SetRestoreState(state);

    Table<TTable>()
        .Key(RESTORE_STATE)
        .Update<TTable::Config>(config);
}

bool TDiskRegistryDatabase::ReadLastBackupTs(TInstant& time)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    auto it = Table<TTable>()
        .Key(LAST_BACKUP_TS)
        .Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    time = it.IsValid()
        ? TInstant::MilliSeconds(
            it.GetValue<TTable::Config>().GetLastBackupTs())
        : TInstant::Zero();

    return true;
}

void TDiskRegistryDatabase::WriteLastBackupTs(TInstant time)
{
    using TTable = TDiskRegistrySchema::DiskRegistryConfig;

    NProto::TDiskRegistryConfig config;
    config.SetLastBackupTs(time.MilliSeconds());

    Table<TTable>()
        .Key(LAST_BACKUP_TS)
        .Update<TTable::Config>(config);
}

void TDiskRegistryDatabase::AddAutomaticallyReplacedDevice(
    const TAutomaticallyReplacedDeviceInfo& deviceInfo)
{
    using TTable = TDiskRegistrySchema::AutomaticallyReplacedDevices;
    Table<TTable>()
        .Key(deviceInfo.DeviceId)
        .Update<TTable::ReplacementTs>(deviceInfo.ReplacementTs.MicroSeconds());
}

bool TDiskRegistryDatabase::ReadAutomaticallyReplacedDevices(
    TDeque<TAutomaticallyReplacedDeviceInfo>& deviceInfos)
{
    using TTable = TDiskRegistrySchema::AutomaticallyReplacedDevices;

    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        deviceInfos.push_back({
            it.GetValue<TTable::Id>(),
            TInstant::MicroSeconds(it.GetValue<TTable::ReplacementTs>())
        });

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    Sort(
        deviceInfos.begin(),
        deviceInfos.end(),
        [] (const auto& l, const auto& r) {
            return l.ReplacementTs < r.ReplacementTs;
        }
    );

    return true;
}

void TDiskRegistryDatabase::DeleteAutomaticallyReplacedDevice(
    const TString& deviceId)
{
    using TTable = TDiskRegistrySchema::AutomaticallyReplacedDevices;
    Table<TTable>()
        .Key(deviceId)
        .Delete();
}

void TDiskRegistryDatabase::AddDiskRegistryAgentListParams(const TString& agentId, const NProto::TDiskRegistryAgentParams& params) {
    using TTable = TDiskRegistrySchema::DiskRegistryAgentListParams;

    Table<TTable>()
        .Key(agentId)
        .Update<TTable::Params>(params);
}

bool TDiskRegistryDatabase::ReadDiskRegistryAgentListParams(THashMap<TString, NProto::TDiskRegistryAgentParams>& params) {
    using TTable = TDiskRegistrySchema::DiskRegistryAgentListParams;

    auto it = Table<TTable>()
        .Range()
        .template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        params.emplace(
            it.GetValue<TTable::AgentId>(),
            it.GetValue<TTable::Params>());
        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

void TDiskRegistryDatabase::DeleteDiskRegistryAgentListParams(const TString& agentId) {
    using TTable = TDiskRegistrySchema::DiskRegistryAgentListParams;

    Table<TTable>()
        .Key(agentId)
        .Delete();
}

void TDiskRegistryDatabase::AddDiskWithRecentlyReplacedDevices(
    const TString& masterDiskId,
    const TString& replicaId)
{
    using TTable = TDiskRegistrySchema::DisksWithRecentlyReplacedDevices;
    Table<TTable>().Key(masterDiskId).Update<TTable::ReplicaId>(replicaId);
}

void TDiskRegistryDatabase::DeleteDiskWithRecentlyReplacedDevices(
    const TString& masterDiskId)
{
    using TTable = TDiskRegistrySchema::DisksWithRecentlyReplacedDevices;
    Table<TTable>()
        .Key(masterDiskId)
        .Delete();
}

bool TDiskRegistryDatabase::ReadDiskWithRecentlyReplacedDevices(
    TVector<TString>& masterDiskIds,
    TVector<TString>& replicaIds)
{
    using TTable = TDiskRegistrySchema::DisksWithRecentlyReplacedDevices;
    auto it =
        Table<TTable>().Range().template Select<typename TTable::TColumns>();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    while (it.IsValid()) {
        masterDiskIds.push_back(it.GetValue<TTable::MasterDiskId>());
        replicaIds.push_back(it.GetValue<TTable::ReplicaId>());

        if (!it.Next()) {
            return false;   // not ready
        }
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
