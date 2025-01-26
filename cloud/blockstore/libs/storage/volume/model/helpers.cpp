#include "helpers.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/common/media.h>

#include <util/generic/algorithm.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {
namespace {

////////////////////////////////////////////////////////////////////////////////

using google::protobuf::RepeatedPtrField;
using TDeviceMatcher = std::function<bool(const NProto::TDeviceConfig& device)>;

struct TDeviceLocation
{
    ui32 RowIndex = 0;
    ui32 ReplicaIndex = 0;
    std::optional<ui32> MigrationIndex;
};

const RepeatedPtrField<NProto::TDeviceConfig>& GetReplicaDevices(
    const NProto::TVolumeMeta& meta,
    ui32 index)
{
    if (index == 0) {
        return meta.GetDevices();
    }
    index--;
    Y_DEBUG_ABORT_UNLESS(meta.ReplicasSize() > index);
    return meta.GetReplicas(index).GetDevices();
}

const NProto::TDeviceConfig& GetDeviceConfig(
    const NProto::TVolumeMeta& meta,
    TDeviceLocation deviceLocation)
{
    if (!deviceLocation.MigrationIndex) {
        return GetReplicaDevices(
            meta,
            deviceLocation.ReplicaIndex)[deviceLocation.RowIndex];
    }
    Y_DEBUG_ABORT_UNLESS(
        meta.MigrationsSize() > *deviceLocation.MigrationIndex);
    return meta.GetMigrations(*deviceLocation.MigrationIndex).GetTargetDevice();
}

std::optional<TDeviceLocation> FindDeviceLocation(
    const NProto::TVolumeMeta& meta,
    TStringBuf deviceUUID)
{
    auto deviceMatcher = [&deviceUUID](const auto& device)
    {
        return device.GetDeviceUUID() == deviceUUID;
    };

    for (size_t i = 0; i <= meta.ReplicasSize(); i++) {
        const auto& devices = GetReplicaDevices(meta, i);
        const auto index = FindIndexIf(devices, deviceMatcher);
        if (index != NPOS) {
            return TDeviceLocation{
                .RowIndex = static_cast<ui32>(index),
                .ReplicaIndex = static_cast<ui32>(i)};
        }
    }

    for (size_t i = 0; i < meta.MigrationsSize(); i++) {
        const auto& migration = meta.GetMigrations(i);
        if (!deviceMatcher(migration.GetTargetDevice())) {
            continue;
        }
        auto sourceLocation =
            FindDeviceLocation(meta, migration.GetSourceDeviceId());
        if (!sourceLocation) {
            ReportDiskAllocationFailure(
                TStringBuilder()
                << "Migration source device " << migration.GetSourceDeviceId()
                << " doesn't belong to the disk "
                << meta.GetConfig().GetDiskId() << ". Target device: "
                << migration.GetTargetDevice().GetDeviceUUID());
            continue;
        }
        sourceLocation->MigrationIndex = i;
        return sourceLocation;
    }

    return std::nullopt;
}

[[nodiscard]] std::optional<ui32> FindReplicaIndex(
    const NProto::TVolumeMeta& meta,
    const TDeviceMatcher& deviceMatcher)
{
    std::optional<ui32> replicaIndex;
    for (size_t i = 0; i <= meta.ReplicasSize(); i++) {
        const auto& devices = GetReplicaDevices(meta, i);
        if (AnyOf(devices, deviceMatcher)) {
            replicaIndex = i;
            break;
        }
    }

    if (!replicaIndex) {
        for (const auto& migration: meta.GetMigrations()) {
            if (deviceMatcher(migration.GetTargetDevice())) {
                auto deviceLocation =
                    FindDeviceLocation(meta, migration.GetSourceDeviceId());
                if (deviceLocation) {
                    replicaIndex = deviceLocation->ReplicaIndex;
                    break;
                }
            }
        }
    }

    return replicaIndex;
}

[[nodiscard]] TVector<NProto::TLaggingDevice> CollectLaggingDevices(
    const NProto::TVolumeMeta& meta,
    ui32 replicaIndex,
    const TDeviceMatcher& deviceMatcher)
{
    TVector<NProto::TLaggingDevice> result;
    const auto replicaDevices = GetReplicaDevices(meta, replicaIndex);
    for (int i = 0; i < replicaDevices.size(); i++) {
        const auto& device = replicaDevices[i];
        if (deviceMatcher(device)) {
            auto& laggingDevice = result.emplace_back();
            laggingDevice.SetRowIndex(i);
            laggingDevice.SetDeviceUUID(device.GetDeviceUUID());
        }
    }

    for (const auto& migration: meta.GetMigrations()) {
        const auto& targetDevice = migration.GetTargetDevice();
        if (deviceMatcher(targetDevice)) {
            auto deviceLocation =
                FindDeviceLocation(meta, migration.GetSourceDeviceId());
            if (!deviceLocation) {
                ReportDiskAllocationFailure(
                    TStringBuilder()
                    << "Migration source device "
                    << migration.GetSourceDeviceId()
                    << " doesn't belong to the disk "
                    << meta.GetConfig().GetDiskId()
                    << ". Target device: " << targetDevice.GetDeviceUUID());
                continue;
            }
            auto& laggingDevice = result.emplace_back();
            laggingDevice.SetRowIndex(deviceLocation->RowIndex);
            laggingDevice.SetDeviceUUID(targetDevice.GetDeviceUUID());
        }
    }

    Sort(result, TLaggingDeviceIndexCmp());
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TLaggingDeviceIndexCmp::operator()(
    const NProto::TLaggingDevice& lhs,
    const NProto::TLaggingDevice& rhs) const
{
    return lhs.GetRowIndex() < rhs.GetRowIndex();
}

const NProto::TDeviceConfig* FindDeviceConfig(
    const NProto::TVolumeMeta& meta,
    TStringBuf deviceUUID)
{
    auto deviceLocation = FindDeviceLocation(meta, deviceUUID);
    if (!deviceLocation) {
        return nullptr;
    }
    return &GetDeviceConfig(meta, *deviceLocation);
}

std::optional<ui32> FindReplicaIndexByAgentNodeId(
    const NProto::TVolumeMeta& meta,
    ui32 agentNodeId)
{
    const auto deviceMatcher =
        [agentNodeId](const NProto::TDeviceConfig& device)
    {
        return device.GetNodeId() == agentNodeId;
    };

    return FindReplicaIndex(meta, deviceMatcher);
}

std::optional<ui32> FindReplicaIndexByAgentId(
    const NProto::TVolumeMeta& meta,
    TStringBuf agentId)
{
    const auto deviceMatcher = [agentId](const NProto::TDeviceConfig& device)
    {
        return device.GetAgentId() == agentId;
    };

    return FindReplicaIndex(meta, deviceMatcher);
}

TVector<NProto::TLaggingDevice> CollectLaggingDevices(
    const NProto::TVolumeMeta& meta,
    ui32 replicaIndex,
    ui32 agentNodeId)
{
    const auto deviceMatcher =
        [agentNodeId](const NProto::TDeviceConfig& device)
    {
        return device.GetNodeId() == agentNodeId;
    };

    return CollectLaggingDevices(meta, replicaIndex, deviceMatcher);
}

TVector<NProto::TLaggingDevice> CollectLaggingDevices(
    const NProto::TVolumeMeta& meta,
    ui32 replicaIndex,
    TStringBuf agentId)
{
    const auto deviceMatcher = [agentId](const NProto::TDeviceConfig& device)
    {
        return device.GetAgentId() == agentId;
    };

    return CollectLaggingDevices(meta, replicaIndex, deviceMatcher);
}

bool HaveCommonRows(
    const TVector<NProto::TLaggingDevice>& laggingCandidates,
    const RepeatedPtrField<NProto::TLaggingDevice>& alreadyLagging)
{
    Y_ABORT_UNLESS(IsSorted(
        laggingCandidates.begin(),
        laggingCandidates.end(),
        TLaggingDeviceIndexCmp()));
    Y_ABORT_UNLESS(IsSorted(
        alreadyLagging.begin(),
        alreadyLagging.end(),
        TLaggingDeviceIndexCmp()));

    for (int i = 0, j = 0;
         i < laggingCandidates.ysize() && j < alreadyLagging.size();)
    {
        if (TLaggingDeviceIndexCmp()(laggingCandidates[i], alreadyLagging[j])) {
            i++;
        } else if (
            TLaggingDeviceIndexCmp()(alreadyLagging[j], laggingCandidates[i]))
        {
            j++;
        } else {
            return true;
        }
    }

    return false;
}

bool RowHasFreshDevices(
    const NProto::TVolumeMeta& meta,
    ui32 rowIndex,
    ui32 timeoutedDeviceReplicaIndex)
{
    for (size_t i = 0; i <= meta.ReplicasSize(); i++) {
        if (i == timeoutedDeviceReplicaIndex) {
            continue;
        }
        const auto& devices = GetReplicaDevices(meta, i);
        if (FindPtr(
                meta.GetFreshDeviceIds(),
                devices[rowIndex].GetDeviceUUID()))
        {
            return true;
        }
    }
    return false;
}

void RemoveLaggingDevicesFromMeta(
    NProto::TVolumeMeta& meta,
    const TVector<TString>& laggingDeviceIds)
{
    auto& agents = *meta.MutableLaggingAgentsInfo()->MutableAgents();
    for (auto& agent: agents) {
        EraseIf(
            *agent.MutableDevices(),
            [&laggingDeviceIds](const NProto::TLaggingDevice& laggingDevice)
            {
                return !!FindPtr(
                    laggingDeviceIds,
                    laggingDevice.GetDeviceUUID());
            });
    }
    EraseIf(
        agents,
        [](const NProto::TLaggingAgent& laggingAgent)
        { return laggingAgent.GetDevices().empty(); });
    if (agents.empty()) {
        meta.MutableLaggingAgentsInfo()->Clear();
    }
}

void UpdateLaggingDevicesAfterMetaUpdate(NProto::TVolumeMeta& meta)
{
    auto& laggingAgents = *meta.MutableLaggingAgentsInfo()->MutableAgents();
    for (auto& agent: laggingAgents) {
        agent.ClearDevices();

        auto replicaIndex = FindReplicaIndexByAgentId(meta, agent.GetAgentId());
        if (!replicaIndex) {
            continue;
        }
        TVector<NProto::TLaggingDevice> updatedLaggingDevices =
            CollectLaggingDevices(meta, *replicaIndex, agent.GetAgentId());

        Y_DEBUG_ABORT_UNLESS(*replicaIndex == agent.GetReplicaIndex());
        Y_DEBUG_ABORT_UNLESS(!updatedLaggingDevices.empty());
        for (auto& laggingDevice: updatedLaggingDevices) {
            *agent.AddDevices() = std::move(laggingDevice);
        }
    }

    EraseIf(
        laggingAgents,
        [](const NProto::TLaggingAgent& laggingAgent)
        { return laggingAgent.GetDevices().empty(); });
    if (meta.GetLaggingAgentsInfo().GetAgents().empty()) {
        meta.MutableLaggingAgentsInfo()->Clear();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
