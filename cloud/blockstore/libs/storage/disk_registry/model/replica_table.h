#pragma once

#include "public.h"

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TReplicaTable
{
    using TDeviceId = TString;
    using TDiskId = TString;

    struct TDeviceInfo
    {
        TDeviceId Id;
        bool IsReplacement = false;
    };

public:
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

    // for tests
    TVector<TVector<TDeviceInfo>> AsMatrix(const TString& diskId) const;

    // for counters
    // returns replicaCount -> {incompleteReplicaCount -> diskCount}
    THashMap<ui32, THashMap<ui32, ui32>> CalculateReplicaCountStats() const;

private:
    bool ChangeDevice(
        const TDiskId& diskId,
        const TDeviceId& oldDeviceId,
        const TDeviceId& newDeviceId,
        bool isReplacement);

private:
    // a transposed view of disk config

    using TCell = TVector<TDeviceInfo>;

    struct TDiskState
    {
        TDeque<TCell> Cells;
        THashMap<TString, TCell*> DeviceId2Cell;
    };

    THashMap<TDiskId, TDiskState> Disks;
};

}   // namespace NCloud::NBlockStore::NStorage
