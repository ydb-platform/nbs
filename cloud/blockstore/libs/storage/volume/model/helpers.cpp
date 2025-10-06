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

struct TLaggingDeviceIndexCmp
{
    bool operator()(
        const NProto::TLaggingDevice& lhs,
        const NProto::TLaggingDevice& rhs) const
    {
        return lhs.GetRowIndex() < rhs.GetRowIndex();
    }
};

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
    return meta.GetMigrations(*deviceLocation.MigrationIndex).GetTargetDevice();
}

std::optional<TDeviceLocation> FindReplicaDeviceLocation(
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

    return std::nullopt;
}

std::optional<TDeviceLocation> FindTargetMigrationDeviceLocation(
    const NProto::TVolumeMeta& meta,
    TStringBuf deviceUUID)
{
    auto deviceMatcher = [&deviceUUID](const auto& device)
    {
        return device.GetDeviceUUID() == deviceUUID;
    };

    for (size_t i = 0; i < meta.MigrationsSize(); i++) {
        const auto& migration = meta.GetMigrations(i);
        if (!deviceMatcher(migration.GetTargetDevice())) {
            continue;
        }
        auto sourceLocation =
            FindReplicaDeviceLocation(meta, migration.GetSourceDeviceId());
        if (!sourceLocation) {
            ReportDiskAllocationFailure(
                "Migration source device not found",
                {{"disk", meta.GetConfig().GetDiskId()},
                 {"target_device", migration.GetTargetDevice().GetDeviceUUID()},
                 {"source_device", migration.GetSourceDeviceId()}});
            continue;
        }
        sourceLocation->MigrationIndex = i;
        return sourceLocation;
    }

    return std::nullopt;
}

std::optional<TDeviceLocation> FindDeviceLocation(
    const NProto::TVolumeMeta& meta,
    TStringBuf deviceUUID)
{
    auto deviceLocation = FindReplicaDeviceLocation(meta, deviceUUID);
    if (!deviceLocation) {
        deviceLocation = FindTargetMigrationDeviceLocation(meta, deviceUUID);
    }

    return deviceLocation;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

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

std::optional<ui32> FindReplicaIndexByAgentId(
    const NProto::TVolumeMeta& meta,
    TStringBuf agentId)
{
    const auto deviceMatcher = [agentId](const NProto::TDeviceConfig& device)
    {
        return device.GetAgentId() == agentId;
    };

    for (size_t i = 0; i <= meta.ReplicasSize(); i++) {
        const auto& devices = GetReplicaDevices(meta, i);
        if (AnyOf(devices, deviceMatcher)) {
            return i;
        }
    }

    for (const auto& migration: meta.GetMigrations()) {
        if (deviceMatcher(migration.GetTargetDevice())) {
            auto deviceLocation =
                FindReplicaDeviceLocation(meta, migration.GetSourceDeviceId());
            if (deviceLocation) {
                return deviceLocation->ReplicaIndex;
            }
        }
    }

    return std::nullopt;
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
                FindReplicaDeviceLocation(meta, migration.GetSourceDeviceId());
            if (!deviceLocation) {
                ReportDiskAllocationFailure(
                    "Migration inconsistency detected",
                    {{"disk", meta.GetConfig().GetDiskId()},
                     {"target_device", targetDevice.GetDeviceUUID()},
                     {"source_device", migration.GetSourceDeviceId()}});
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

bool HaveCommonRows(
    const RepeatedPtrField<NProto::TLaggingDevice>& laggingCandidates,
    const RepeatedPtrField<NProto::TLaggingDevice>& alreadyLagging)
{
    TLaggingDeviceIndexCmp cmp;

    Y_ABORT_UNLESS(
        IsSorted(laggingCandidates.begin(), laggingCandidates.end(), cmp));
    Y_ABORT_UNLESS(IsSorted(alreadyLagging.begin(), alreadyLagging.end(), cmp));

    for (int i = 0, j = 0;
         i < laggingCandidates.size() && j < alreadyLagging.size();)
    {
        if (cmp(laggingCandidates[i], alreadyLagging[j])) {
            i++;
        } else if (cmp(alreadyLagging[j], laggingCandidates[i])) {
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
    ui32 timedOutDeviceReplicaIndex)
{
    for (size_t i = 0; i <= meta.ReplicasSize(); i++) {
        if (i == timedOutDeviceReplicaIndex) {
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

void UpdateLaggingDevicesAfterMetaUpdate(
    NProto::TVolumeMeta& meta,
    const TVector<TString>& removedLaggingDeviceIds)
{
    auto& laggingAgents = *meta.MutableLaggingAgentsInfo()->MutableAgents();
    for (auto& agent: laggingAgents) {
        const bool laggingDevicesWereRemoved = AllOf(
            agent.GetDevices(),
            [&removedLaggingDeviceIds](
                const NProto::TLaggingDevice& laggingDevice)
            {
                return !!FindPtr(
                    removedLaggingDeviceIds,
                    laggingDevice.GetDeviceUUID());
            });

        agent.ClearDevices();
        if (laggingDevicesWereRemoved) {
            continue;
        }

        auto replicaIndex = FindReplicaIndexByAgentId(meta, agent.GetAgentId());
        if (!replicaIndex) {
            continue;
        }
        TVector<NProto::TLaggingDevice> updatedLaggingDevices =
            CollectLaggingDevices(meta, *replicaIndex, agent.GetAgentId());

        Y_ABORT_UNLESS(*replicaIndex == agent.GetReplicaIndex());
        Y_DEBUG_ABORT_UNLESS(!updatedLaggingDevices.empty());
        for (auto& laggingDevice: updatedLaggingDevices) {
            *agent.AddDevices() = std::move(laggingDevice);
        }
    }

    EraseIf(
        laggingAgents,
        [](const NProto::TLaggingAgent& laggingAgent)
        { return laggingAgent.GetDevices().empty(); });
    if (laggingAgents.empty()) {
        meta.MutableLaggingAgentsInfo()->Clear();
    }
}

TVector<NProto::TDeviceConfig> GetReplacedDevices(
    const NProto::TVolumeMeta& oldMeta,
    const NProto::TVolumeMeta& newMeta)
{
    TVector<NProto::TDeviceConfig> result;
    for (size_t i = 0; i <= oldMeta.ReplicasSize(); i++) {
        const auto& oldDevices = GetReplicaDevices(oldMeta, i);
        const auto& newDevices = GetReplicaDevices(newMeta, i);

        for (int deviceIdx = 0; deviceIdx < oldDevices.size(); deviceIdx++) {
            if (oldDevices[deviceIdx].GetDeviceUUID() !=
                newDevices[deviceIdx].GetDeviceUUID())
            {
                result.push_back(oldDevices[deviceIdx]);
            }
        }
    }

    for (const auto& oldMigration: oldMeta.GetMigrations()) {
        auto targetLocation = FindDeviceLocation(
            newMeta,
            oldMigration.GetTargetDevice().GetDeviceUUID());
        if (!targetLocation) {
            result.push_back(oldMigration.GetTargetDevice());
        }
    }

    return result;
}

TMap<TString, TString> ParseTags(const TString& tags)
{
    TMap<TString, TString> result;

    TStringBuf tok;
    TStringBuf sit(tags);
    while (sit.NextTok(',', tok)) {
        TStringBuf key;
        TStringBuf value;
        if (tok.TrySplit('=', key, value)) {
            result.insert({TString(key), TString(value)});
        } else {
            result.insert({TString(tok), ""});
        }
    }

    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
