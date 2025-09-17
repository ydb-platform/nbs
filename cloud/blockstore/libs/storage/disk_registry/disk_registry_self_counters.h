#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/diagnostics/solomon_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDiskRegistrySelfCounters
{
    using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

    struct TDevicePoolCounters
    {
        TCounterPtr FreeBytes;
        TCounterPtr TotalBytes;
        TCounterPtr BrokenBytes;
        TCounterPtr DecommissionedBytes;
        TCounterPtr SuspendedBytes;
        TCounterPtr DirtyBytes;
        TCounterPtr AllocatedBytes;
        TCounterPtr AllocatedDevices;
        TCounterPtr DirtyDevices;
        TCounterPtr DevicesInOnlineState;
        TCounterPtr DevicesInWarningState;
        TCounterPtr DevicesInErrorState;

        void Init(NMonitoring::TDynamicCountersPtr counters);
    };

    struct TNonreplMetricsCounter
    {
        TCounterPtr CountCounter;
        TCounterPtr CountByteCounter;
    };

    TCounterPtr FreeBytes;
    TCounterPtr TotalBytes;
    TCounterPtr BrokenBytes;
    TCounterPtr DecommissionedBytes;
    TCounterPtr SuspendedBytes;
    TCounterPtr DirtyBytes;
    TCounterPtr AllocatedBytes;
    TCounterPtr AllocatedDisks;
    TCounterPtr AllocatedDevices;
    TCounterPtr DirtyDevices;
    TCounterPtr FreshDevices;
    TCounterPtr UnknownDevices;
    TCounterPtr DevicesInOnlineState;
    TCounterPtr DevicesInWarningState;
    TCounterPtr DevicesInErrorState;
    TCounterPtr AgentsInOnlineState;
    TCounterPtr AgentsInWarningState;
    TCounterPtr AgentsInUnavailableState;
    TCounterPtr DisksInOnlineState;

    TCounterPtr DisksInWarningState;
    TCounterPtr MaxWarningTime;
    // XXX for backward compat with alerts
    TCounterPtr DisksInMigrationState;
    TCounterPtr MaxMigrationTime;

    TCounterPtr DevicesInMigrationState;
    TCounterPtr DisksInTemporarilyUnavailableState;
    TCounterPtr DisksInErrorState;
    TCounterPtr PlacementGroups;
    TCounterPtr FullPlacementGroups;
    TCounterPtr AllocatedDisksInGroups;
    TCounterPtr Mirror2Disks;
    TCounterPtr Mirror2DisksMinus1;
    TCounterPtr Mirror2DisksMinus2;
    TCounterPtr Mirror3Disks;
    TCounterPtr Mirror3DisksMinus1;
    TCounterPtr Mirror3DisksMinus2;
    TCounterPtr Mirror3DisksMinus3;
    TCounterPtr PlacementGroupsWithRecentlyBrokenSinglePartition;
    TCounterPtr PlacementGroupsWithRecentlyBrokenTwoOrMorePartitions;
    TCounterPtr PlacementGroupsWithBrokenSinglePartition;
    TCounterPtr PlacementGroupsWithBrokenTwoOrMorePartitions;
    TCounterPtr MeanTimeBetweenFailures;
    TCounterPtr AutomaticallyReplacedDevices;
    TCounterPtr CachedAcquireDevicesRequestAmount;

    TCumulativeCounter QueryAvailableStorageErrors;

    THashMap<TString, TDevicePoolCounters> PoolName2Counters;

    TVector<TNonreplMetricsCounter> NonreplMetricsCounter;

    void Init(
        const TVector<std::pair<TString, NProto::EDevicePoolKind>>& pools,
        NMonitoring::TDynamicCountersPtr counters);

    void RegisterPool(
        const TString& poolName,
        const TString& poolKind,
        NMonitoring::TDynamicCountersPtr counters);
};

}   // namespace NCloud::NBlockStore::NStorage
