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

struct TDeviceLocation
{
    ui32 RowIndex = 0;
    ui32 ReplicaIndex = 0;
    std::optional<int> MigrationIndex;
};

const RepeatedPtrField<NProto::TDeviceConfig>& GetReplicaDevices(
    const NProto::TVolumeMeta& meta,
    ui32 index)
{
    if (index == 0) {
        return meta.GetDevices();
    }
    index--;
    Y_DEBUG_ABORT_UNLESS(meta.GetReplicas().size() > static_cast<i32>(index));
    return meta.GetReplicas(index).GetDevices();
}

const NProto::TDeviceConfig& GetDeviceConfig(
    const NProto::TVolumeMeta& meta,
    TDeviceLocation deviceLocation)
{
    if (!deviceLocation.MigrationIndex.has_value()) {
        return GetReplicaDevices(
            meta,
            deviceLocation.ReplicaIndex)[deviceLocation.RowIndex];
    }
    Y_DEBUG_ABORT_UNLESS(
        meta.GetMigrations().size() > *deviceLocation.MigrationIndex);
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

    auto it = FindIf(meta.GetDevices(), deviceMatcher);
    if (it != meta.GetDevices().end()) {
        return TDeviceLocation{
            .RowIndex =
                static_cast<ui32>(std::distance(meta.GetDevices().begin(), it)),
            .ReplicaIndex = 0};
    }

    for (int i = 0; i < meta.GetReplicas().size(); i++) {
        const auto& replica = meta.GetReplicas(i);
        auto it = FindIf(replica.GetDevices(), deviceMatcher);
        if (it != replica.GetDevices().end()) {
            return TDeviceLocation{
                .RowIndex = static_cast<ui32>(
                    std::distance(replica.GetDevices().begin(), it)),
                .ReplicaIndex = static_cast<ui32>(i + 1)};
        }
    }

    for (int i = 0; i < meta.GetMigrations().size(); i++) {
        const auto& migration = meta.GetMigrations(i);
        if (deviceMatcher(migration.GetTargetDevice())) {
            auto sourceLocation =
                FindDeviceLocation(meta, migration.GetSourceDeviceId());
            if (!sourceLocation) {
                ReportDiskAllocationFailure(
                    TStringBuilder()
                    << "Migration source device "
                    << migration.GetSourceDeviceId()
                    << " doesn't belong to the disk "
                    << meta.GetConfig().GetDiskId() << ". Target device: "
                    << migration.GetTargetDevice().GetDeviceUUID());
                continue;
            }
            sourceLocation->MigrationIndex = i;
            return sourceLocation;
        }
    }

    return std::nullopt;
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
    const TStringBuf& deviceUUID)
{
    auto deviceLocation = FindDeviceLocation(meta, deviceUUID);
    if (!deviceLocation.has_value()) {
        return nullptr;
    }
    return &GetDeviceConfig(meta, *deviceLocation);
}

std::optional<ui32> GetAgentDevicesIndexes(
    const NProto::TVolumeMeta& meta,
    ui32 agentNodeId,
    TVector<NProto::TLaggingDevice>* laggingDevices)
{
    Y_DEBUG_ABORT_UNLESS(laggingDevices);
    std::optional<ui32> replicaIndex;
    const RepeatedPtrField<NProto::TDeviceConfig>* replicaDevices = nullptr;
    const auto deviceMatcher =
        [agentNodeId](const NProto::TDeviceConfig& device)
    {
        return device.GetNodeId() == agentNodeId;
    };
    if (AnyOf(meta.GetDevices(), deviceMatcher)) {
        replicaIndex = 0;
        replicaDevices = &meta.GetDevices();
    }

    for (int i = 0; !replicaIndex && i < meta.GetReplicas().size(); i++) {
        if (AnyOf(meta.GetReplicas()[i].GetDevices(), deviceMatcher)) {
            replicaIndex = i + 1;
            replicaDevices = &meta.GetReplicas()[i].GetDevices();
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
                    replicaDevices =
                        &GetReplicaDevices(meta, deviceLocation->ReplicaIndex);
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

    for (const auto& migration: meta.GetMigrations()) {
        const auto& targetDevice = migration.GetTargetDevice();
        if (deviceMatcher(targetDevice)) {
            NProto::TLaggingDevice laggingDevice;
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
            laggingDevice.SetRowIndex(deviceLocation->RowIndex);
            laggingDevice.SetDeviceUUID(targetDevice.GetDeviceUUID());
            laggingDevices->push_back(std::move(laggingDevice));
        }
    }

    Sort(*laggingDevices, TLaggingDeviceIndexCmp());
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

    for (int i = 1; i <= meta.GetReplicas().size(); i++) {
        if (FindPtr(
                meta.GetFreshDeviceIds(),
                GetReplicaDevices(meta, i)[rowIndex].GetDeviceUUID()))
        {
            result.insert(i);
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
                return !!FindPtr(
                    laggingDeviceIds,
                    laggingDevice.GetDeviceUUID());
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
