#pragma once

#include "public.h"

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <array>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceList;

////////////////////////////////////////////////////////////////////////////////

struct TMirroredDisksStat
{
    ui32 Mirror2DiskMinus0 = 0;
    ui32 Mirror2DiskMinus1 = 0;
    ui32 Mirror2DiskMinus2 = 0;

    ui32 Mirror3DiskMinus0 = 0;
    ui32 Mirror3DiskMinus1 = 0;
    ui32 Mirror3DiskMinus2 = 0;
    ui32 Mirror3DiskMinus3 = 0;
};

struct TMirroredDiskDevicesStat
{
    std::array<ui32, 4> CellsByState = {};
    ui32 DeviceReadyCount = 0;
    ui32 DeviceReplacementCount = 0;
    ui32 DeviceErrorCount = 0;
    ui32 MaxIncompleteness = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicaTable
{
    using TDeviceId = TString;
    using TDiskId = TString;

public:
    struct TDeviceInfo
    {
        TDeviceId Id;

        // Set to true when device has been replaced and data is being copied to
        // it now.
        bool IsReplacement = false;
    };

public:
    TReplicaTable(const TDeviceList* const deviceList);

    void AddReplica(const TDiskId& diskId, const TVector<TDeviceId>& devices);
    void UpdateReplica(
        const TDiskId& diskId,
        const ui32 replicaNo,
        const TVector<TDeviceId>& devices);
    bool RemoveMirroredDisk(const TDiskId& diskId);
    bool IsReplacementAllowed(
        const TDiskId& diskId,
        const TDeviceId& deviceId) const;
    bool ReplaceDevice(
        const TDiskId& diskId,
        const TDeviceId& deviceId,
        const TDeviceId& replacementId);
    void MarkReplacementDevice(
        const TDiskId& diskId,
        const TDeviceId& deviceId,
        bool isReplacement);
    [[nodiscard]] TVector<TString> GetDevicesReplacements(
        const TDiskId& diskId) const;
    [[nodiscard]] bool IsReplacementDevice(
        const TDiskId& diskId,
        const TDeviceId& deviceId) const;

    // for tests and monpages
    TVector<TVector<TDeviceInfo>> AsMatrix(const TString& diskId) const;

    TMirroredDisksStat CalculateReplicaCountStats() const;

    TMirroredDiskDevicesStat CalculateDiskStats(
        const TString& diskId) const;

private:
    bool ChangeDevice(
        const TDiskId& diskId,
        const TDeviceId& oldDeviceId,
        const TDeviceId& newDeviceId,
        bool isReplacement);

private:
    // a transposed view of disk config

    using TRow = TVector<TDeviceInfo>;

    struct TDiskState
    {
        TDeque<TRow> Rows;
        THashMap<TString, TRow*> DeviceId2Row;
    };

    THashMap<TDiskId, TDiskState> Disks;
    const TDeviceList* const DeviceList = nullptr;
};

}   // namespace NCloud::NBlockStore::NStorage
