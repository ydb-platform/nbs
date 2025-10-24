#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TDevicePoolConfigs = THashMap<TString, NProto::TDevicePoolConfig>;

////////////////////////////////////////////////////////////////////////////////

class TDeviceList
{
    using TDeviceId = TString;
    using TDiskId = TString;
    using TNodeId = ui32;

    struct TNodeInfo
    {
        TNodeId NodeId = 0;
        ui64 FreeSpace = 0;
        ui64 OccupiedSpace = 0;
    };

    struct TRack
    {
        TString Id;
        TVector<TNodeInfo> Nodes;
        ui64 FreeSpace = 0;
        ui64 OccupiedSpace = 0;
        bool Preferred = false;
    };

    struct TNodeDevices
    {
        TString Rack;

        // sorted by {PoolKind, BlockSize}
        TVector<NProto::TDeviceConfig> FreeDevices;

        ui64 TotalSize = 0;
    };

    using TDeviceRange = std::tuple<
        TNodeId,
        TVector<NProto::TDeviceConfig>::const_iterator,
        TVector<NProto::TDeviceConfig>::const_iterator>;

private:
    THashMap<TDeviceId, NProto::TDeviceConfig> AllDevices;
    THashMap<TNodeId, TNodeDevices> NodeDevices;
    THashMap<TDeviceId, TDiskId> AllocatedDevices;
    THashSet<TDeviceId> DirtyDevices;
    THashMap<TDeviceId, NProto::TSuspendedDevice> SuspendedDevices;
    THashMap<NProto::EDevicePoolKind, TVector<TString>> PoolKind2PoolNames;
    THashMap<TString, ui32> PoolName2DeviceCount;
    const bool AlwaysAllocateLocalDisks;

public:
    struct TAllocationQuery
    {
        THashSet<TString> ForbiddenRacks;
        THashSet<TString> PreferredRacks;
        THashSet<ui32> DownrankedNodeIds;

        ui32 LogicalBlockSize = 0;
        ui64 BlockCount = 0;

        TString PoolName;
        NProto::EDevicePoolKind PoolKind = NProto::DEVICE_POOL_KIND_DEFAULT;
        THashSet<ui32> NodeIds;

        ui64 GetTotalByteCount() const
        {
            return LogicalBlockSize * BlockCount;
        }
    };

    TDeviceList();

    explicit TDeviceList(
        TVector<TDeviceId> dirtyDevices,
        TVector<NProto::TSuspendedDevice> suspendedDevices,
        TVector<std::pair<TDeviceId, TDiskId>> allocatedDevices,
        bool alwaysAllocateLocalDisks);

    [[nodiscard]] size_t Size() const
    {
        return AllDevices.size();
    }

    [[nodiscard]] const auto& GetPoolName2DeviceCount() const
    {
        return PoolName2DeviceCount;
    }

    void UpdateDevices(
        const NProto::TAgentConfig& agent,
        const TDevicePoolConfigs& poolConfigs,
        TNodeId prevNodeId);

    void UpdateDevices(
        const NProto::TAgentConfig& agent,
        const TDevicePoolConfigs& poolConfigs);

    void RemoveDevices(const NProto::TAgentConfig& agent);

    [[nodiscard]] TNodeId FindNodeId(const TDeviceId& id) const;
    [[nodiscard]] TString FindAgentId(const TDeviceId& id) const;

    [[nodiscard]] TString FindRack(const TDeviceId& id) const;
    [[nodiscard]] TDiskId FindDiskId(const TDeviceId& id) const;
    [[nodiscard]] const NProto::TDeviceConfig* FindDevice(const TDeviceId& id) const;

    [[nodiscard]] TVector<NProto::TDeviceConfig> GetBrokenDevices() const;
    [[nodiscard]] TVector<NProto::TDeviceConfig> GetDirtyDevices() const;
    [[nodiscard]] TVector<TString> GetDirtyDevicesId() const;

    NProto::TDeviceConfig AllocateDevice(
        const TDiskId& diskId,
        const TAllocationQuery& query);
    TResultOrError<NProto::TDeviceConfig> AllocateSpecificDevice(
        const TDiskId& diskId,
        const TDeviceId& deviceId,
        const TAllocationQuery& query);
    [[nodiscard]] bool IsAllocatedDevice(const TDeviceId& id) const;
    bool ValidateAllocationQuery(
        const TAllocationQuery& query,
        const TDeviceId& targetDeviceId);

    void MarkDeviceAllocated(const TDiskId& diskId, const TDeviceId& id);
    bool ReleaseDevice(const TDeviceId& id);

    [[nodiscard]] bool DevicesAllocationAllowed(
        NProto::EDevicePoolKind poolKind,
        NProto::EAgentState agentState) const;

    TVector<NProto::TDeviceConfig> AllocateDevices(
        const TDiskId& diskId,
        const TAllocationQuery& query);

    bool CanAllocateDevices(const TAllocationQuery& query);

    bool MarkDeviceAsClean(const TDeviceId& uuid);
    void MarkDeviceAsDirty(const TDeviceId& uuid);

    [[nodiscard]] bool IsDirtyDevice(const TDeviceId& uuid) const;
    [[nodiscard]] NProto::EDeviceState GetDeviceState(const TDeviceId& uuid) const;

    void SuspendDevice(const TDeviceId& ids);
    void ResumeDevice(const TDeviceId& id);
    void ResumeAfterErase(const TDeviceId& id);
    [[nodiscard]] bool IsSuspendedDevice(const TDeviceId& id) const;
    // Similar to the above, but also checks that the device is not resuming
    // from suspension.
    [[nodiscard]] bool IsSuspendedAndNotResumingDevice(
        const TDeviceId& id) const;
    [[nodiscard]] TVector<NProto::TSuspendedDevice> GetSuspendedDevices() const;

    [[nodiscard]] ui64 GetDeviceByteCount(const TDeviceId& id) const;

    void ForgetDevice(const TString& id);

private:
    TVector<TDeviceRange> CollectDevices(
        const TAllocationQuery& query,
        const TString& poolName);
    TVector<TDeviceRange> CollectDevices(const TAllocationQuery& query);

    [[nodiscard]] TVector<TRack> SelectRacks(
        const TAllocationQuery& query,
        const TString& poolName) const;

    [[nodiscard]] TVector<TNodeInfo> RankNodes(
        const TAllocationQuery& query,
        TVector<TRack> racks) const;

    void RemoveDeviceFromFreeList(const TDeviceId& id);
    void UpdateInAllDevices(
        const TDeviceId& id,
        const NProto::TDeviceConfig& device);
    void RemoveFromAllDevices(const TDeviceId& id);
};

////////////////////////////////////////////////////////////////////////////////

TVector<NProto::TDeviceConfig> FilterDevices(
    TVector<NProto::TDeviceConfig>& dirtyDevices,
    ui32 maxPerDeviceNameForDefaultPoolKind,
    ui32 maxPerDeviceNameForLocalPoolKind,
    ui32 maxPerDeviceNameForGlobalPoolKind);

}   // namespace NCloud::NBlockStore::NStorage
