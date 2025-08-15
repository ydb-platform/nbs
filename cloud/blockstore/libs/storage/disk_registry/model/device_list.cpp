#include "device_list.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <util/generic/algorithm.h>
#include <util/generic/iterator_range.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TSortQueryKey = std::tuple<
    NProto::EDevicePoolKind,
    TString,
    ui32,
    TString>;

struct TBySortQueryKey
{
    auto operator () (const NProto::TDeviceConfig& config) const
    {
        return TSortQueryKey {
            config.GetPoolKind(),
            config.GetPoolName(),
            config.GetBlockSize(),
            config.GetDeviceName(),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

using TAllocationQueryKey = std::tuple<
    NProto::EDevicePoolKind,
    TString,
    ui32>;

struct TByAllocationQueryKey
{
    auto operator () (const NProto::TDeviceConfig& config) const
    {
        return TAllocationQueryKey {
            config.GetPoolKind(),
            config.GetPoolName(),
            config.GetBlockSize(),
        };
    }
};

auto FindDeviceRange(
    const TDeviceList::TAllocationQuery& query,
    const TString& poolName,
    const TVector<NProto::TDeviceConfig>& devices)
{
    auto begin = LowerBoundBy(
        devices.begin(),
        devices.end(),
        std::make_pair(query.PoolKind, poolName),
        [] (const auto& d) {
            return std::make_pair(d.GetPoolKind(), d.GetPoolName());
        });

    auto end = UpperBoundBy(
        begin,
        devices.end(),
        TAllocationQueryKey {
            query.PoolKind,
            poolName,
            query.LogicalBlockSize
        },
        TByAllocationQueryKey());

    return std::pair { begin, end };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDeviceList::TDeviceList()
    : AlwaysAllocateLocalDisks(false)
{}

TDeviceList::TDeviceList(
        TVector<TDeviceId> dirtyDevices,
        TVector<NProto::TSuspendedDevice> suspendedDevices,
        TVector<std::pair<TDeviceId, TDiskId>> allocatedDevices,
        bool alwaysAllocateLocalDisks)
    : AllocatedDevices(
          std::make_move_iterator(allocatedDevices.begin()),
          std::make_move_iterator(allocatedDevices.end()))
    , DirtyDevices(
          std::make_move_iterator(dirtyDevices.begin()),
          std::make_move_iterator(dirtyDevices.end()))
    , AlwaysAllocateLocalDisks(alwaysAllocateLocalDisks)
{
    for (auto& device: suspendedDevices) {
        auto id = device.GetId();
        SuspendedDevices.emplace(std::move(id), std::move(device));
    }
}

void TDeviceList::UpdateDevices(
    const NProto::TAgentConfig& agent,
    const TDevicePoolConfigs& poolConfigs,
    TNodeId prevNodeId)
{
    NodeDevices.erase(prevNodeId);
    UpdateDevices(agent, poolConfigs);
}

void TDeviceList::UpdateDevices(
    const NProto::TAgentConfig& agent,
    const TDevicePoolConfigs& poolConfigs)
{
    if (agent.GetNodeId() == 0) {
        for (const auto& device: agent.GetDevices()) {
            Y_DEBUG_ABORT_UNLESS(device.GetNodeId() == 0);

            const auto& uuid = device.GetDeviceUUID();
            UpdateInAllDevices(uuid, device);
        }

        return;
    }

    auto& nodeDevices = NodeDevices[agent.GetNodeId()];
    nodeDevices.FreeDevices.clear();
    nodeDevices.Rack.clear();
    nodeDevices.TotalSize = 0;

    for (const auto& device: agent.GetDevices()) {
        if (device.GetState() == NProto::DEVICE_STATE_ONLINE
                && !device.GetRack().empty())
        {
            nodeDevices.Rack = device.GetRack();
            break;
        }
    }

    for (const auto& device: agent.GetDevices()) {
        if (device.GetNodeId() != agent.GetNodeId()) {
            ReportDiskRegistryAgentDeviceNodeIdMismatch(
                {{"Agent", agent.GetAgentId()},
                 {"Device", device.GetDeviceUUID()},
                 {"AgentNodeId", agent.GetNodeId()},
                 {"DeviceNodeId", device.GetNodeId()}});

            continue;
        }

        const auto& uuid = device.GetDeviceUUID();
        UpdateInAllDevices(uuid, device);

        if (device.GetRack() != nodeDevices.Rack) {
            continue;
        }

        const ui64 deviceSize = device.GetBlockSize() * device.GetBlocksCount();

        const bool isFree =
            DevicesAllocationAllowed(device.GetPoolKind(), agent.GetState()) &&
            device.GetState() == NProto::DEVICE_STATE_ONLINE &&
            !AllocatedDevices.contains(uuid) && !DirtyDevices.contains(uuid) &&
            !SuspendedDevices.contains(uuid);

        const auto* poolConfig = poolConfigs.FindPtr(device.GetPoolName());
        if (!poolConfig || poolConfig->GetKind() != device.GetPoolKind() ||
            poolConfig->GetAllocationUnit() != deviceSize)
        {
            if (isFree) {
                ReportDiskRegistryAgentDevicePoolConfigMismatch(
                    TStringBuilder()
                    << "Device: " << device << " Pool config: "
                    << (poolConfig ? *poolConfig
                                   : NProto::TDevicePoolConfig{}));
            }

            continue;
        }

        if (isFree) {
            nodeDevices.FreeDevices.push_back(device);
        }

        auto& poolNames = PoolKind2PoolNames[device.GetPoolKind()];
        auto it = Find(poolNames, device.GetPoolName());
        if (it == poolNames.end()) {
            poolNames.push_back(device.GetPoolName());
        }

        nodeDevices.TotalSize += deviceSize;
    }

    SortBy(nodeDevices.FreeDevices, TBySortQueryKey());
}

void TDeviceList::RemoveDevices(const NProto::TAgentConfig& agent)
{
    NodeDevices.erase(agent.GetNodeId());

    for (const auto& device: agent.GetDevices()) {
        const auto& uuid = device.GetDeviceUUID();
        RemoveFromAllDevices(uuid);
        DirtyDevices.erase(uuid);
    }
}

TDeviceList::TNodeId TDeviceList::FindNodeId(const TDeviceId& id) const
{
    auto* device = FindDevice(id);

    if (device) {
        return device->GetNodeId();
    }

    return {};
}

TString TDeviceList::FindAgentId(const TDeviceId& id) const
{
    auto* device = FindDevice(id);

    if (device) {
        return device->GetAgentId();
    }

    return {};
}

TString TDeviceList::FindRack(const TDeviceId& id) const
{
    auto it = AllDevices.find(id);
    if (it != AllDevices.end()) {
        return it->second.GetRack();
    }

    return {};
}

TDeviceList::TDiskId TDeviceList::FindDiskId(const TDeviceId& id) const
{
    auto it = AllocatedDevices.find(id);
    if (it != AllocatedDevices.end()) {
        return it->second;
    }
    return {};
}

bool TDeviceList::DevicesAllocationAllowed(
    NProto::EDevicePoolKind poolKind,
    NProto::EAgentState agentState) const
{
    if (agentState == NProto::AGENT_STATE_UNAVAILABLE) {
        return false;
    }

    if (agentState == NProto::AGENT_STATE_ONLINE) {
        return true;
    }

    Y_DEBUG_ABORT_UNLESS(agentState == NProto::AGENT_STATE_WARNING);
    if (poolKind == NProto::DEVICE_POOL_KIND_LOCAL) {
        return AlwaysAllocateLocalDisks;
    }

    return false;
}

NProto::TDeviceConfig TDeviceList::AllocateDevice(
    const TDiskId& diskId,
    const TAllocationQuery& query)
{
    for (auto& kv: NodeDevices) {
        if (!query.NodeIds.empty() && !query.NodeIds.contains(kv.first)) {
            continue;
        }

        const ui32 nodeId = kv.first;
        auto& nodeDevices = kv.second;

        const auto& currentRack = nodeDevices.Rack;
        auto& devices = nodeDevices.FreeDevices;

        if (devices.empty() || query.ForbiddenRacks.contains(currentRack)) {
            continue;
        }

        auto it = FindIf(devices, [&] (const auto& device) {
            if (device.GetRack() != currentRack) {
                ReportDiskRegistryPoolDeviceRackMismatch(
                    {{"disk", diskId},
                     {"NodeId", nodeId},
                     {"PoolRack", currentRack},
                     {"Device", device.GetDeviceUUID()},
                     {"DeviceRack", device.GetRack()}});
                return false;
            }

            const ui64 size = device.GetBlockSize() * device.GetUnadjustedBlockCount();
            const ui64 blockCount = size / query.LogicalBlockSize;

            return query.BlockCount <= blockCount
                && device.GetPoolName() == query.PoolName;
        });

        if (it != devices.end()) {
            auto it2 = it;  // for Coverity: NBS-2899
            NProto::TDeviceConfig config = std::move(*it2);
            devices.erase(it);

            AllocatedDevices.emplace(config.GetDeviceUUID(), diskId);

            return config;
        }
    }

    return {};
}

TResultOrError<NProto::TDeviceConfig> TDeviceList::AllocateSpecificDevice(
    const TDiskId& diskId,
    const TDeviceId& deviceId,
    const TAllocationQuery& query)
{
    const auto* config = FindDevice(deviceId);
    if (!config) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "device not found, " << deviceId.Quote());
    }

    if (IsSuspendedDevice(deviceId)) {
        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "device is suspended, " << deviceId.Quote());
    }

    if (IsAllocatedDevice(deviceId)) {
        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "device is allocated, " << deviceId.Quote());
    }

    if (!query.NodeIds.empty() && !query.NodeIds.contains(config->GetNodeId())) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "device node id is not allowed, "
            << deviceId.Quote()
            << "NodeId: " << config->GetNodeId());
    }

    if (query.ForbiddenRacks.contains(config->GetRack())) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "device rack is forbidden, "
            << deviceId.Quote()
            << "Rack: " << config->GetRack());
    }

    if (query.PoolName != config->GetPoolName()) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "device pool name is not allowed, "
            << deviceId.Quote()
            << "PoolName: " << config->GetPoolName());
    }

    const ui64 size = config->GetBlockSize() * config->GetUnadjustedBlockCount();
    const ui64 blockCount = size / query.LogicalBlockSize;

    if (query.BlockCount > blockCount) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "device block count is too small, "
            << deviceId.Quote()
            << "BlockCount: " << blockCount);
    }

    if (IsDirtyDevice(deviceId)) {
        DirtyDevices.erase(deviceId);
    }

    MarkDeviceAllocated(diskId, deviceId);
    return *config;
}

bool TDeviceList::ValidateAllocationQuery(
    const TAllocationQuery& query,
    const TDeviceId& targetDeviceId)
{
    const TNodeId node = FindNodeId(targetDeviceId);
    if (!query.NodeIds.empty() && !query.NodeIds.contains(node)) {
        return false;
    }

    const auto nodeItr = NodeDevices.find(node);
    if (nodeItr == NodeDevices.end()) {
        return false;
    }

    const TNodeDevices& nodeDevices = nodeItr->second;

    if (query.ForbiddenRacks.contains(nodeDevices.Rack)) {
        return false;
    }

    const auto freeDeviceItr = FindIf(
        nodeDevices.FreeDevices,
        [&targetDeviceId] (const NProto::TDeviceConfig& device) {
            return device.GetDeviceUUID() == targetDeviceId;
        });

    if (freeDeviceItr == nodeDevices.FreeDevices.end()) {
        return false;
    }

    const ui64 freeBlockCount =
        freeDeviceItr->GetBlockSize() *
        freeDeviceItr->GetUnadjustedBlockCount() /
        query.LogicalBlockSize;

    return query.BlockCount <= freeBlockCount
        && freeDeviceItr->GetPoolName() == query.PoolName;

}

void TDeviceList::MarkDeviceAllocated(const TDiskId& diskId, const TDeviceId& id)
{
    RemoveDeviceFromFreeList(id);
    AllocatedDevices.emplace(id, diskId);
}

// returns a list of racks sorted by preference and then by occupied space ASC
// then by free space DESC
// the nodes in each rack are sorted by occupied space ASC then by free space
// DESC
auto TDeviceList::SelectRacks(
    const TAllocationQuery& query,
    const TString& poolName) const -> TVector<TRack>
{
    THashMap<TString, TRack> racks;

    auto appendNode = [&] (auto& currentRack, ui32 nodeId) {
        if (query.ForbiddenRacks.contains(currentRack)) {
            return;
        }

        auto& rack = racks[currentRack];
        rack.Id = currentRack;
        rack.Nodes.push_back({nodeId, 0, 0});
        rack.Preferred = query.PreferredRacks.contains(currentRack);
    };

    if (!query.NodeIds.empty()) {
        for (ui32 id: query.NodeIds) {
            if (auto* nodeDevices = NodeDevices.FindPtr(id)) {
                appendNode(nodeDevices->Rack, id);
            }
        }
    } else {
        for (auto& [nodeId, nodeDevices]: NodeDevices) {
            appendNode(nodeDevices.Rack, nodeId);
        }
    }

    for (auto& [id, rack]: racks) {
        ui64 rackTotalSpace = 0;

        for (auto& node: rack.Nodes) {
            const auto* nodeDevices = NodeDevices.FindPtr(node.NodeId);
            Y_ABORT_UNLESS(nodeDevices);

            auto r = FindDeviceRange(query, poolName, nodeDevices->FreeDevices);
            node.OccupiedSpace = nodeDevices->TotalSize;
            rackTotalSpace += nodeDevices->TotalSize;

            for (const auto& device: MakeIteratorRange(r)) {
                const auto s = device.GetBlockSize() * device.GetBlocksCount();
                rack.FreeSpace += s;
                Y_DEBUG_ABORT_UNLESS(node.OccupiedSpace >= s);
                node.OccupiedSpace -= s;
                node.FreeSpace += s;
            }
        }

        Sort(
            rack.Nodes,
            [] (const TNodeInfo& lhs, const TNodeInfo& rhs) {
                if (lhs.OccupiedSpace != rhs.OccupiedSpace) {
                    return lhs.OccupiedSpace < rhs.OccupiedSpace;
                }

                return lhs.FreeSpace > rhs.FreeSpace;
            });

        rack.OccupiedSpace = rackTotalSpace - rack.FreeSpace;
    }

    TVector<TRack*> bySpace;
    for (auto& x: racks) {
        if (x.second.FreeSpace) {
            bySpace.push_back(&x.second);
        }
    }

    Sort(
        bySpace,
        [] (const TRack* lhs, const TRack* rhs) {
            if (lhs->Preferred != rhs->Preferred) {
                return lhs->Preferred > rhs->Preferred;
            }
            if (lhs->OccupiedSpace != rhs->OccupiedSpace) {
                return lhs->OccupiedSpace < rhs->OccupiedSpace;
            }
            if (lhs->FreeSpace != rhs->FreeSpace) {
                return lhs->FreeSpace > rhs->FreeSpace;
            }
            return lhs->Id < rhs->Id;
        });

    TVector<TRack> result;
    result.reserve(bySpace.size());

    for (auto* x: bySpace) {
        result.push_back(*x);
    }

    return result;
}

TVector<TDeviceList::TDeviceRange> TDeviceList::CollectDevices(
    const TAllocationQuery& query,
    const TString& poolName)
{
    if (!query.BlockCount || !query.LogicalBlockSize) {
        return {};
    }

    TVector<TDeviceRange> ranges;
    ui64 totalSize = query.GetTotalByteCount();

    for (const auto& rack: SelectRacks(query, poolName)) {
        for (const auto& node: rack.Nodes) {
            const auto* nodeDevices = NodeDevices.FindPtr(node.NodeId);
            Y_ABORT_UNLESS(nodeDevices);

            // finding free devices belonging to this node that match our
            // query
            auto [begin, end] =
                FindDeviceRange(query, poolName, nodeDevices->FreeDevices);

            using TDeviceIter = decltype(begin);
            struct TDeviceInfo
            {
                TString DeviceName;
                ui64 Size = 0;
                std::pair<TDeviceIter, TDeviceIter> Range;
            };

            // grouping these matching devices by DeviceName and sorting
            // these groups by size in descending order
            TVector<TDeviceInfo> bySize;
            for (auto it = begin; it != end; ++it) {
                if (bySize.empty()
                        || bySize.back().DeviceName != it->GetDeviceName())
                {
                    bySize.emplace_back(TDeviceInfo{
                        it->GetDeviceName(),
                        0,
                        std::make_pair(it, it),
                    });
                }

                auto& current = bySize.back();
                current.Size += it->GetBlockSize() * it->GetBlocksCount();
                ++current.Range.second;
            }

            SortBy(bySize, [] (const TDeviceInfo& d) {
                return Max<ui64>() - d.Size;
            });

            // traversing device groups from the biggest to the smallest
            // the goal is to greedily select as few groups as possible
            // in most of the cases it will lead to an allocation which is placed
            // on a single physical device, which is what we want
            for (const auto& deviceInfo: bySize) {
                auto it = deviceInfo.Range.first;
                for (; it != deviceInfo.Range.second; ++it) {
                    const auto& device = *it;

                    Y_DEBUG_ABORT_UNLESS(device.GetRack() == nodeDevices->Rack);

                    const ui64 size = device.GetBlockSize() * device.GetBlocksCount();

                    if (totalSize <= size) {
                        totalSize = 0;
                        ++it;
                        break;
                    }

                    totalSize -= size;
                }

                if (deviceInfo.Range.first != it) {
                    ranges.emplace_back(node.NodeId, deviceInfo.Range.first, it);
                }

                if (totalSize == 0) {
                    return ranges;
                }
            }

            if (query.PoolKind == NProto::DEVICE_POOL_KIND_LOCAL) {
                // here we go again

                ranges.clear();
                totalSize = query.GetTotalByteCount();
            }
        }
    }

    return {};
}

TVector<TDeviceList::TDeviceRange> TDeviceList::CollectDevices(
    const TAllocationQuery& query)
{
    if (query.PoolName) {
        return CollectDevices(query, query.PoolName);
    }

    if (auto* poolNames = PoolKind2PoolNames.FindPtr(query.PoolKind)) {
        for (const auto& poolName: *poolNames) {
            if (auto collected = CollectDevices(query, poolName)) {
                return collected;
            }
        }
    }

    return {};
}

TVector<NProto::TDeviceConfig> TDeviceList::AllocateDevices(
    const TString& diskId,
    const TAllocationQuery& query)
{
    TVector<NProto::TDeviceConfig> allocated;
    using TDeviceIter = TVector<NProto::TDeviceConfig>::const_iterator;
    using TAllocatedRange = std::pair<TDeviceIter, TDeviceIter>;
    THashMap<TNodeId, TVector<TAllocatedRange>> allocatedRanges;

    for (auto [nodeId, it, end]: CollectDevices(query)) {
        for (const auto& device: MakeIteratorRange(it, end)) {
            const auto& uuid = device.GetDeviceUUID();

            Y_DEBUG_ABORT_UNLESS(device.GetState() == NProto::DEVICE_STATE_ONLINE);

            AllocatedDevices.emplace(uuid, diskId);
            allocated.emplace_back(device);
        }

        allocatedRanges[nodeId].push_back(std::make_pair(it, end));
    }

    // erasing allocated ranges from last to first
    // otherwise some of the ranges can become invalid because there may be
    // multiple ranges from the same node
    for (auto& [nodeId, aranges]: allocatedRanges) {
        Sort(aranges, [] (const auto& l, const auto& r) {
            return l.first > r.first;
        });

        auto& nodeDevices = NodeDevices[nodeId];

        for (const auto& arange: aranges) {
            nodeDevices.FreeDevices.erase(arange.first, arange.second);
        }
    }

    return allocated;
}

bool TDeviceList::CanAllocateDevices(const TAllocationQuery& query)
{
    return !CollectDevices(query).empty();
}

bool TDeviceList::ReleaseDevice(const TDeviceId& id)
{
    AllocatedDevices.erase(id);

    if (!AllDevices.contains(id)) {
        return false;
    }

    DirtyDevices.insert(id);

    return true;
}

bool TDeviceList::MarkDeviceAsClean(const TDeviceId& id)
{
    auto it = SuspendedDevices.find(id);
    if (it != SuspendedDevices.end() && it->second.GetResumeAfterErase()) {
        SuspendedDevices.erase(it);
    }

    return DirtyDevices.erase(id) != 0;
}

void TDeviceList::MarkDeviceAsDirty(const TDeviceId& id)
{
    DirtyDevices.insert(id);
    RemoveDeviceFromFreeList(id);
}

void TDeviceList::RemoveDeviceFromFreeList(const TDeviceId& id)
{
    auto nodeId = FindNodeId(id);

    if (nodeId) {
        auto& devices = NodeDevices[nodeId].FreeDevices;

        auto it = FindIf(devices, [&] (const auto& x) {
            return x.GetDeviceUUID() == id;
        });

        if (it != devices.end()) {
            devices.erase(it);
        }
    }
}

const NProto::TDeviceConfig* TDeviceList::FindDevice(const TDeviceId& id) const
{
    auto it = AllDevices.find(id);

    if (it == AllDevices.end()) {
        return nullptr;
    }

    return &it->second;
}

TVector<NProto::TDeviceConfig> TDeviceList::GetBrokenDevices() const
{
    TVector<NProto::TDeviceConfig> devices;

    for (const auto& x: AllDevices){
        if (x.second.GetState() == NProto::DEVICE_STATE_ERROR) {
            devices.push_back(x.second);
        }
    }

    return devices;
}

TVector<NProto::TDeviceConfig> TDeviceList::GetDirtyDevices() const
{
    TVector<NProto::TDeviceConfig> devices;
    devices.reserve(DirtyDevices.size());

    for (const auto& id: DirtyDevices) {
        auto it = SuspendedDevices.find(id);
        if (it != SuspendedDevices.end() && !it->second.GetResumeAfterErase()) {
            continue;
        }

        auto* device = FindDevice(id);
        if (device) {
            devices.push_back(*device);
        }
    }

    return devices;
}

TVector<TString> TDeviceList::GetDirtyDevicesId() const
{
    return {DirtyDevices.begin(), DirtyDevices.end()};
}

bool TDeviceList::IsDirtyDevice(const TDeviceId& uuid) const
{
    return DirtyDevices.contains(uuid);
}

NProto::EDeviceState TDeviceList::GetDeviceState(const TDeviceId& uuid) const
{
    if (auto* device = AllDevices.FindPtr(uuid)) {
        return device->GetState();
    }
    return NProto::EDeviceState::DEVICE_STATE_ERROR;
}

void TDeviceList::SuspendDevice(const TDeviceId& id)
{
    NProto::TSuspendedDevice device;
    device.SetId(id);
    SuspendedDevices.emplace(id, device);
    RemoveDeviceFromFreeList(id);
}

void TDeviceList::ResumeDevice(const TDeviceId& id)
{
    SuspendedDevices.erase(id);
}

void TDeviceList::ResumeAfterErase(const TDeviceId& id)
{
    auto it = SuspendedDevices.find(id);
    if (it == SuspendedDevices.end()) {
        return;
    }

    it->second.SetResumeAfterErase(true);
}

bool TDeviceList::IsSuspendedDevice(const TDeviceId& id) const
{
    return SuspendedDevices.contains(id);
}

bool TDeviceList::IsSuspendedAndNotResumingDevice(const TDeviceId& id) const
{
    auto it = SuspendedDevices.find(id);
    if (it == SuspendedDevices.end()) {
        return false;
    }

    return !it->second.GetResumeAfterErase();
}

bool TDeviceList::IsAllocatedDevice(const TDeviceId& id) const
{
    return AllocatedDevices.contains(id);
}

TVector<NProto::TSuspendedDevice> TDeviceList::GetSuspendedDevices() const
{
    TVector<NProto::TSuspendedDevice> devices;
    devices.reserve(SuspendedDevices.size());
    for (auto& [_, device]: SuspendedDevices) {
        devices.push_back(device);
    }

    return devices;
}

ui64 TDeviceList::GetDeviceByteCount(const TDeviceId& id) const
{
    const auto* device = FindDevice(id);
    return device
        ? device->GetBlocksCount() * device->GetBlockSize()
        : 0;
}

void TDeviceList::ForgetDevice(const TString& id)
{
    RemoveDeviceFromFreeList(id);

    RemoveFromAllDevices(id);
    AllocatedDevices.erase(id);
    DirtyDevices.erase(id);
    SuspendedDevices.erase(id);
}

////////////////////////////////////////////////////////////////////////////////

void TDeviceList::UpdateInAllDevices(
    const TDeviceId& id,
    const NProto::TDeviceConfig& device)
{
    auto& config = AllDevices[id];
    if (config.GetDeviceUUID()) {
        // device with this id is already registered
        --PoolName2DeviceCount[config.GetPoolName()];
    }
    config = device;
    ++PoolName2DeviceCount[device.GetPoolName()];
}

void TDeviceList::RemoveFromAllDevices(const TDeviceId& id)
{
    auto it = AllDevices.find(id);
    if (it == AllDevices.end()) {
        return;
    }

    --PoolName2DeviceCount[it->second.GetPoolName()];
    AllDevices.erase(it);
}

////////////////////////////////////////////////////////////////////////////////

TVector<NProto::TDeviceConfig> FilterDevices(
    TVector<NProto::TDeviceConfig>& dirtyDevices,
    ui32 maxPerDeviceNameForDefaultPoolKind,
    ui32 maxPerDeviceNameForLocalPoolKind,
    ui32 maxPerDeviceNameForGlobalPoolKind)
{
    THashMap<TString, ui32> fullPath2Count;
    TVector<NProto::TDeviceConfig> filtered;
    for (auto& d: dirtyDevices) {
        auto key = Sprintf(
            "%s:/%s",
            d.GetAgentId().c_str(),
            d.GetDeviceName().c_str());
        auto& c = fullPath2Count[key];
        ui32 limit = 1;
        switch (d.GetPoolKind()) {
            case NProto::DEVICE_POOL_KIND_DEFAULT: {
                limit = maxPerDeviceNameForDefaultPoolKind;
                break;
            }
            case NProto::DEVICE_POOL_KIND_LOCAL: {
                limit = maxPerDeviceNameForLocalPoolKind;
                break;
            }
            case NProto::DEVICE_POOL_KIND_GLOBAL: {
                limit = maxPerDeviceNameForGlobalPoolKind;
                break;
            }
            default: {
                Y_DEBUG_ABORT_UNLESS(0);
            }
        }

        if (c >= limit) {
            continue;
        }

        ++c;
        filtered.push_back(std::move(d));
    }

    return filtered;
}

}   // namespace NCloud::NBlockStore::NStorage
