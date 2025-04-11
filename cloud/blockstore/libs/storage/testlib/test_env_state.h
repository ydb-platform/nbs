#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EMigrationMode
{
    Disabled = 0,
    InProgress = 1,
    Finish = 2
};

struct TDiskRegistryState: TAtomicRefCount<TDiskRegistryState>
{
    struct TDisk
    {
        TVector<NProto::TDeviceConfig> Devices;
        TString WriterClientId;
        TVector<TString> ReaderClientIds;
        NProto::EVolumeIOMode IOMode = NProto::VOLUME_IO_OK;
        TInstant IOModeTs;
        TMap<TString, NProto::TDeviceConfig> Migrations;
        TVector<TVector<NProto::TDeviceConfig>> Replicas;
        TVector<NProto::TLaggingDevice> LaggingDevices;
        bool MuteIOErrors = false;
        TString PoolName;
        NCloud::NProto::EStorageMediaKind MediaKind = {};
        ui32 BlockSize = 4096;
    };

    struct TPlacementGroup
    {
        TSet<TString> DiskIds;
        ui32 ConfigVersion = 1;
        NProto::TPlacementGroupSettings Settings;
        NProto::EPlacementStrategy PlacementStrategy;
        ui32 PlacementPartitionCount = 0;
    };

    TMap<TString, TDisk> Disks;
    TSet<TString> DisksMarkedForCleanup;
    TMap<TString, TPlacementGroup> PlacementGroups;
    TVector<NProto::TDeviceConfig> Devices;
    TVector<bool> DeviceIsAllocated;
    TMap<TString, NProto::TDeviceConfig> MigrationDevices;
    ui32 CurrentErrorCode = S_OK;
    EMigrationMode MigrationMode = EMigrationMode::Disabled;
    ui32 ReplicaCount = 0;
    ui32 FinishMigrationRequests = 0;
    THashSet<TString> DeviceReplacementUUIDs;

    TVector<std::pair<TString, NProto::EAgentState>> AgentStates;

    bool AllocateDiskReplicasOnDifferentNodes = false;
    bool WritableState = false;

    const NProto::TDeviceConfig* AllocateNextDevice(i32 prevNodeId);
    bool ReplaceDevice(const TString& diskId, const TString& deviceUUID);
    bool StartDeviceMigration(const TString& deviceUUID);
};

using TDiskRegistryStatePtr = TIntrusivePtr<TDiskRegistryState>;

struct TTestEnvState
{
    TDiskRegistryStatePtr DiskRegistryState;

    TTestEnvState(TDiskRegistryStatePtr diskRegistryState)
        : DiskRegistryState(std::move(diskRegistryState))
    {
    }

    TTestEnvState()
        : DiskRegistryState(new TDiskRegistryState)
    {
    }
};

}   // namespace NCloud::NBlockStore::NStorage
