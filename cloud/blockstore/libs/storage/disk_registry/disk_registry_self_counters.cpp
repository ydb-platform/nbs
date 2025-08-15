#include "disk_registry_self_counters.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistrySelfCounters::Init(
    const TVector<TString>& poolNames,
    NMonitoring::TDynamicCountersPtr counters)
{
    FreeBytes = counters->GetCounter("FreeBytes");
    TotalBytes = counters->GetCounter("TotalBytes");
    BrokenBytes = counters->GetCounter("BrokenBytes");
    DecommissionedBytes = counters->GetCounter("DecommissionedBytes");
    SuspendedBytes = counters->GetCounter("SuspendedBytes");
    DirtyBytes = counters->GetCounter("DirtyBytes");
    AllocatedBytes = counters->GetCounter("AllocatedBytes");
    AllocatedDisks = counters->GetCounter("AllocatedDisks");
    AllocatedDevices = counters->GetCounter("AllocatedDevices");
    DirtyDevices = counters->GetCounter("DirtyDevices");
    UnknownDevices = counters->GetCounter("UnknownDevices");
    DevicesInOnlineState = counters->GetCounter("DevicesInOnlineState");
    DevicesInWarningState = counters->GetCounter("DevicesInWarningState");
    DevicesInErrorState = counters->GetCounter("DevicesInErrorState");
    AgentsInOnlineState = counters->GetCounter("AgentsInOnlineState");
    AgentsInWarningState = counters->GetCounter("AgentsInWarningState");
    AgentsInUnavailableState = counters->GetCounter("AgentsInUnavailableState");
    DisksInOnlineState = counters->GetCounter("DisksInOnlineState");

    DisksInWarningState = counters->GetCounter("DisksInWarningState");
    MaxWarningTime = counters->GetCounter("MaxWarningTime");
    DisksInMigrationState = counters->GetCounter("DisksInMigrationState");
    MaxMigrationTime = counters->GetCounter("MaxMigrationTime");

    DevicesInMigrationState = counters->GetCounter("DevicesInMigrationState");
    DisksInTemporarilyUnavailableState = counters->GetCounter("DisksInTemporarilyUnavailableState");
    DisksInErrorState = counters->GetCounter("DisksInErrorState");
    PlacementGroups = counters->GetCounter("PlacementGroups");
    FullPlacementGroups = counters->GetCounter("FullPlacementGroups");
    AllocatedDisksInGroups = counters->GetCounter("AllocatedDisksInGroups");
    Mirror2Disks = counters->GetCounter("Mirror2Disks");
    Mirror2DisksMinus1 = counters->GetCounter("Mirror2DisksMinus1");
    Mirror2DisksMinus2 = counters->GetCounter("Mirror2DisksMinus2");
    Mirror3Disks = counters->GetCounter("Mirror3Disks");
    Mirror3DisksMinus1 = counters->GetCounter("Mirror3DisksMinus1");
    Mirror3DisksMinus2 = counters->GetCounter("Mirror3DisksMinus2");
    Mirror3DisksMinus3 = counters->GetCounter("Mirror3DisksMinus3");
    PlacementGroupsWithRecentlyBrokenSinglePartition =
        counters->GetCounter("PlacementGroupsWithRecentlyBrokenSinglePartition");
    PlacementGroupsWithRecentlyBrokenTwoOrMorePartitions =
        counters->GetCounter("PlacementGroupsWithRecentlyBrokenTwoOrMorePartitions");
    PlacementGroupsWithBrokenSinglePartition =
        counters->GetCounter("PlacementGroupsWithBrokenSinglePartition");
    PlacementGroupsWithBrokenTwoOrMorePartitions =
        counters->GetCounter("PlacementGroupsWithBrokenTwoOrMorePartitions");
    CachedAcquireDevicesRequestAmount =
        counters->GetCounter("CachedAcquireDevicesRequestAmount");
    MeanTimeBetweenFailures = counters->GetCounter("MeanTimeBetweenFailures");
    AutomaticallyReplacedDevices =
        counters->GetCounter("AutomaticallyReplacedDevices");

    QueryAvailableStorageErrors.Register(counters, "QueryAvailableStorageErrors");

    for (const auto& poolName: poolNames) {
        RegisterPool(poolName, counters);
    }
}

void TDiskRegistrySelfCounters::RegisterPool(
    const TString& poolName,
    NMonitoring::TDynamicCountersPtr counters)
{
    PoolName2Counters[poolName].Init(counters->GetSubgroup("pool", poolName));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistrySelfCounters::TDevicePoolCounters::Init(
    NMonitoring::TDynamicCountersPtr counters)
{
    FreeBytes = counters->GetCounter("FreeBytes");
    TotalBytes = counters->GetCounter("TotalBytes");
    BrokenBytes = counters->GetCounter("BrokenBytes");
    DecommissionedBytes = counters->GetCounter("DecommissionedBytes");
    SuspendedBytes = counters->GetCounter("SuspendedBytes");
    DirtyBytes = counters->GetCounter("DirtyBytes");
    AllocatedBytes = counters->GetCounter("AllocatedBytes");
    AllocatedDevices = counters->GetCounter("AllocatedDevices");
    DirtyDevices = counters->GetCounter("DirtyDevices");
    DevicesInOnlineState = counters->GetCounter("DevicesInOnlineState");
    DevicesInWarningState = counters->GetCounter("DevicesInWarningState");
    DevicesInErrorState = counters->GetCounter("DevicesInErrorState");
}

}   // namespace NCloud::NBlockStore::NStorage
