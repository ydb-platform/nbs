#include "helpers.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/common/media.h>

#include <util/generic/algorithm.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {
namespace {

using google::protobuf::RepeatedPtrField;

// Finds index of the replica by device UUID, without searching through
// migrations.
std::pair<ui32, const RepeatedPtrField<NProto::TDeviceConfig>*>
FindReplicaDevices(const NProto::TVolumeMeta& meta, const TString& deviceUUID)
{
    auto deviceMatcher = [&deviceUUID](const auto& device)
    {
        return device.GetDeviceUUID() == deviceUUID;
    };

    if (AnyOf(meta.GetDevices(), deviceMatcher)) {
        return std::make_pair(0, &meta.GetDevices());
    }

    for (int i = 0; i < meta.GetReplicas().size(); i++) {
        const auto& replica = meta.GetReplicas()[i];
        if (AnyOf(replica.GetDevices(), deviceMatcher)) {
            return std::make_pair(i + 1, &replica.GetDevices());
        }
    }

    return std::make_pair(0, nullptr);
}

// Finds index of the device by device UUID, without searching through
// migrations.
std::optional<ui32> FindDeviceRowIndex(
    const NProto::TVolumeMeta& meta,
    const TString& deviceUUID)
{
    auto deviceMatcher = [&deviceUUID](const auto& device)
    {
        return device.GetDeviceUUID() == deviceUUID;
    };

    auto it = FindIf(meta.GetDevices(), deviceMatcher);
    if (it != meta.GetDevices().end()) {
        return std::distance(meta.GetDevices().begin(), it);
    }

    for (const auto& replica: meta.GetReplicas()) {
        auto it = FindIf(replica.GetDevices(), deviceMatcher);
        if (it != replica.GetDevices().end()) {
            return std::distance(replica.GetDevices().begin(), it);
        }
    }

    return std::nullopt;
}

}   // namespace

bool TLaggingDeviceIndexCmp::operator()(
    const NProto::TLaggingDevice& lhs,
    const NProto::TLaggingDevice& rhs) const
{
    return lhs.GetRowIndex() < rhs.GetRowIndex();
}

const NProto::TDeviceConfig* FindDeviceConfig(
    const NProto::TVolumeMeta& meta,
    const TString& deviceUUID)
{
    auto deviceMatcher = [&deviceUUID](const auto& device)
    {
        return device.GetDeviceUUID() == deviceUUID;
    };

    const NProto::TDeviceConfig* deviceConfig = nullptr;
    deviceConfig = FindIfPtr(meta.GetDevices(), deviceMatcher);
    if (deviceConfig) {
        return deviceConfig;
    }

    for (const auto& replica: meta.GetReplicas()) {
        deviceConfig = FindIfPtr(replica.GetDevices(), deviceMatcher);
        if (deviceConfig) {
            return deviceConfig;
        }
    }

    for (const auto& migration: meta.GetMigrations()) {
        if (deviceMatcher(migration.GetTargetDevice())) {
            return &migration.GetTargetDevice();
        }
    }

    return nullptr;
}

std::optional<ui32> GetAgentDevicesIndexes(
    const NProto::TVolumeMeta& meta,
    ui32 agentNodeId,
    TVector<NProto::TLaggingDevice>* laggingDevices)
{
    Y_DEBUG_ABORT_UNLESS(laggingDevices);
    std::optional<ui32> replicaIndex;
    const RepeatedPtrField<NProto::TDeviceConfig>* replicaDevices = nullptr;
    const auto deviceMatcher = [agentNodeId](const NProto::TDeviceConfig& device)
    {
        return device.GetNodeId() == agentNodeId;
    };
    if (AnyOf(
            meta.GetDevices().begin(),
            meta.GetDevices().end(),
            deviceMatcher))
    {
        replicaIndex = 0;
        replicaDevices = &meta.GetDevices();
    }

    for (int i = 0; !replicaIndex && i < meta.GetReplicas().size(); i++) {
        if (AnyOf(
                meta.GetReplicas()[i].GetDevices().begin(),
                meta.GetReplicas()[i].GetDevices().end(),
                deviceMatcher))
        {
            replicaIndex = i + 1;
            replicaDevices = &meta.GetReplicas()[i].GetDevices();
            break;
        }
    }

    if (!replicaIndex) {
        for (const auto& migration: meta.GetMigrations()) {
            if (deviceMatcher(migration.GetTargetDevice())) {
                auto [index, devices] =
                    FindReplicaDevices(meta, migration.GetSourceDeviceId());
                if (devices) {
                    replicaIndex = index;
                    replicaDevices = devices;
                }
            }
        }
    }

    // There is no devices from desired agent.
    if (!replicaIndex || !replicaDevices) {
        return std::nullopt;
    }

    for (int i = 0; i < replicaDevices->size(); i++) {
        const auto& device = (*replicaDevices)[i];
        if (deviceMatcher(device)) {
            NProto::TLaggingDevice laggingDevice;
            laggingDevice.SetRowIndex(i);
            laggingDevice.SetDeviceUUID(device.GetDeviceUUID());
            laggingDevices->push_back(std::move(laggingDevice));
        }
    }

    for (int i = 0; i < meta.GetMigrations().size(); i++) {
        const auto& targetDevice = meta.GetMigrations()[i].GetTargetDevice();
        if (deviceMatcher(targetDevice)) {
            NProto::TLaggingDevice laggingDevice;
            auto sourceDeviceIndex = FindDeviceRowIndex(
                meta,
                meta.GetMigrations()[i].GetSourceDeviceId());
            if (!sourceDeviceIndex) {
                ReportDiskAllocationFailure(
                    TStringBuilder()
                    << "Migration source device "
                    << meta.GetMigrations()[i].GetSourceDeviceId()
                    << " doesn't belong to the disk "
                    << meta.GetConfig().GetDiskId()
                    << ". Target device: " << targetDevice.GetDeviceUUID());
                continue;
            }
            laggingDevice.SetRowIndex(*sourceDeviceIndex);
            laggingDevice.SetDeviceUUID(targetDevice.GetDeviceUUID());
            laggingDevices->push_back(std::move(laggingDevice));
        }
    }

    return replicaIndex;
}

TSet<ui32> ReplicaIndexesWithFreshDevices(
    const NProto::TVolumeMeta& meta,
    ui32 rowIndex)
{
    TSet<ui32> result;
    auto it = Find(
        meta.GetFreshDeviceIds(),
        meta.GetDevices()[rowIndex].GetDeviceUUID());
    if (it != meta.GetFreshDeviceIds().end()) {
        result.insert(0);
    }

    const auto& replicas = meta.GetReplicas();
    for (int i = 0; i < replicas.size(); i++) {
        auto it = Find(
            meta.GetFreshDeviceIds(),
            replicas[i].GetDevices()[rowIndex].GetDeviceUUID());
        if (it != meta.GetFreshDeviceIds().end()) {
            result.insert(i + 1);
        }
    }
    return result;
}

void RemoveLaggingDevicesFromMeta(
    NProto::TVolumeMeta& meta,
    const TVector<TString>& laggingDeviceIds)
{
    for (auto& agent: *meta.MutableLaggingAgentsInfo()->MutableAgents()) {
        EraseIf(
            *agent.MutableDevices(),
            [&laggingDeviceIds](const NProto::TLaggingDevice& laggingDevice)
            {
                auto it =
                    Find(laggingDeviceIds, laggingDevice.GetDeviceUUID());
                return it != laggingDeviceIds.end();
            });
    }
    EraseIf(
        *meta.MutableLaggingAgentsInfo()->MutableAgents(),
        [](const NProto::TLaggingAgent& laggingAgent)
        { return laggingAgent.GetDevices().empty(); });
    if (meta.GetLaggingAgentsInfo().GetAgents().empty()) {
        meta.MutableLaggingAgentsInfo()->Clear();
    }
}

void UpdateLaggingDevicesAfterMetaUpdate(NProto::TVolumeMeta& meta)
{
    for (auto& agent: *meta.MutableLaggingAgentsInfo()->MutableAgents()) {
        agent.ClearDevices();

        TVector<NProto::TLaggingDevice> updatedLaggingDevices;
        auto replicaIndex = GetAgentDevicesIndexes(
            meta,
            agent.GetNodeId(),
            &updatedLaggingDevices);
        if (!replicaIndex.has_value()) {
            continue;
        }

        Y_DEBUG_ABORT_UNLESS(*replicaIndex == agent.GetReplicaIndex());
        Y_DEBUG_ABORT_UNLESS(!updatedLaggingDevices.empty());
        for (auto& laggingDevice: updatedLaggingDevices) {
            *agent.AddDevices() = std::move(laggingDevice);
        }
    }

    EraseIf(
        *meta.MutableLaggingAgentsInfo()->MutableAgents(),
        [](const NProto::TLaggingAgent& laggingAgent)
        { return laggingAgent.GetDevices().empty(); });
    if (meta.GetLaggingAgentsInfo().GetAgents().empty()) {
        meta.MutableLaggingAgentsInfo()->Clear();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
